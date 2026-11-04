use std::borrow::Cow;
use std::sync::Arc;

use polars_arrow::datatypes::{ArrowDataType, ArrowSchema};
use polars_core::prelude::{ArrowField, Column, DataType};
use polars_core::schema::Schema;
use polars_error::PolarsResult;
use polars_parquet::read::n_columns;
use polars_plan::dsl::{CastColumnsPolicy, MissingColumnsPolicy};
use polars_utils::pl_str::PlSmallStr;

use crate::nodes::io_sources::multi_scan::components::column_selector::ColumnSelector;
use crate::nodes::io_sources::multi_scan::components::column_selector::builder::ColumnSelectorBuilder;
use crate::nodes::io_sources::multi_scan::components::projection::MappedProjectionRef;
use crate::nodes::io_sources::multi_scan::components::projection::builder::ProjectionBuilder;
use crate::nodes::io_sources::multi_scan::reader_interface::Projection;

pub fn resolve_arrow_field_projections(
    file_arrow_schema: &ArrowSchema,
    file_schema: &Schema,
    projection: Projection,
    cast_columns_policy: CastColumnsPolicy,
) -> PolarsResult<Arc<[ArrowFieldProjection]>> {
    let projection: Projection = match projection {
        Projection::Plain(projected_schema) => ProjectionBuilder::new(projected_schema, None, None)
            .build_projection(
                Some(file_schema),
                None,
                cast_columns_policy.clone(),
                usize::MAX,
            )?,
        Projection::Mapped { .. } => projection,
    };

    projection
        .iter_non_missing_columns()
        .map(
            |MappedProjectionRef {
                 source_name,
                 output_name,
                 output_dtype,
                 resolved_transform,
             }| {
                let arrow_field = file_arrow_schema.get(source_name.as_str()).unwrap().clone();

                let Some(resolved_transform) = resolved_transform else {
                    assert_eq!(source_name, output_name);

                    return Ok(ArrowFieldProjection::Plain(arrow_field));
                };

                assert_eq!(
                    resolved_transform.source_dtype,
                    file_schema.get(source_name.as_str()).unwrap()
                );

                let nested = NestedProjection::try_new(
                    &arrow_field,
                    output_dtype,
                    output_name,
                    &cast_columns_policy,
                )?;

                Ok(ArrowFieldProjection::Mapped {
                    arrow_field,
                    output_name: output_name.clone(),
                    output_dtype: output_dtype.clone(),
                    transform: resolved_transform.attach_transforms(ColumnSelector::Position(0)),
                    nested,
                })
            },
        )
        .collect::<PolarsResult<Arc<[ArrowFieldProjection]>>>()
}

/// Represents a potentially mapped (i.e. cast and/or renamed) arrow field projection.
#[derive(Debug)]
pub enum ArrowFieldProjection {
    Plain(ArrowField),
    Mapped {
        /// The field as it is in the file.
        arrow_field: ArrowField,
        output_name: PlSmallStr,
        output_dtype: DataType,
        /// Transform from the file dtype to `output_dtype`. This is what statistics use, as they
        /// always have the file's field layout.
        transform: ColumnSelector,
        /// Set when `output_dtype` drops struct fields that exist in the file. Decoding then only
        /// touches the parquet leaf columns that are needed.
        nested: Option<NestedProjection>,
    },
}

/// Nested projection pushdown for a single top-level column.
///
/// Parquet stores every scalar leaf of a nested column as its own column chunk. When the
/// requested output dtype is a struct (possibly under lists) with fewer fields than the file has,
/// only the leaves under the kept fields need to be fetched and decoded. This is what Spark calls
/// nested schema pruning and what DuckDB and pyarrow do when given a leaf path.
#[derive(Debug)]
pub struct NestedProjection {
    /// The file field with the dropped struct fields removed, keeping the file's leaf types.
    pub field: ArrowField,
    /// Indices of the leaves to read, relative to the file's leaves under this root column, in
    /// ascending order. These match the leaf order of `field`.
    pub leaf_idxs: Arc<[usize]>,
    /// Transform from the dtype of `field` to the output dtype.
    pub transform: ColumnSelector,
}

impl NestedProjection {
    /// Returns `None` when there is nothing to prune (or the shapes do not line up, in which case
    /// the regular transform reports the mismatch).
    pub fn try_new(
        file_field: &ArrowField,
        output_dtype: &DataType,
        output_name: &PlSmallStr,
        cast_columns_policy: &CastColumnsPolicy,
    ) -> PolarsResult<Option<Self>> {
        if !file_field.dtype.is_nested() {
            return Ok(None);
        }

        let mut leaf_idxs = Vec::new();
        let mut leaf_offset = 0usize;
        let mut pruned_any = false;

        let Some(pruned_dtype) = prune_arrow_dtype(
            &file_field.dtype,
            output_dtype,
            &mut leaf_offset,
            &mut leaf_idxs,
            &mut pruned_any,
        ) else {
            return Ok(None);
        };

        if !pruned_any {
            return Ok(None);
        }

        debug_assert_eq!(leaf_offset, n_columns(&file_field.dtype));
        debug_assert_eq!(leaf_idxs.len(), n_columns(&pruned_dtype));

        let mut field = file_field.clone();
        field.dtype = pruned_dtype;

        let selector_builder = ColumnSelectorBuilder {
            cast_columns_policy: cast_columns_policy.clone(),
            // Not used by `attach_transforms()`.
            missing_columns_policy: MissingColumnsPolicy::Raise,
        };

        let transform = selector_builder.attach_transforms(
            ColumnSelector::Position(0),
            &DataType::from_arrow_field(&field),
            output_dtype,
            output_name.as_str(),
        )?;

        Ok(Some(Self {
            field,
            leaf_idxs: leaf_idxs.into(),
            transform,
        }))
    }
}

/// Walks `file_dtype` and `target_dtype` together. Struct fields that are in the file but not in
/// the target are dropped; their leaves are skipped. Everything else is kept with the file's type,
/// so that the decoder sees a consistent (field, leaves) pair and the transform does the casts.
///
/// Returns `None` if the two shapes differ in a way this function does not handle (then no pruning
/// happens).
fn prune_arrow_dtype(
    file_dtype: &ArrowDataType,
    target_dtype: &DataType,
    leaf_offset: &mut usize,
    leaf_idxs: &mut Vec<usize>,
    pruned_any: &mut bool,
) -> Option<ArrowDataType> {
    match (file_dtype, target_dtype) {
        (ArrowDataType::Struct(file_fields), DataType::Struct(target_fields)) => {
            let mut out = Vec::with_capacity(target_fields.len());
            let start = *leaf_offset;

            for file_field in file_fields {
                let Some(target_field) = target_fields
                    .iter()
                    .find(|f| f.name().as_str() == file_field.name.as_str())
                else {
                    // Dropped by the projection: skip its leaves.
                    *leaf_offset += n_columns(&file_field.dtype);
                    *pruned_any = true;
                    continue;
                };

                let dtype = prune_arrow_dtype(
                    &file_field.dtype,
                    target_field.dtype(),
                    leaf_offset,
                    leaf_idxs,
                    pruned_any,
                )?;

                let mut f = file_field.clone();
                f.dtype = dtype;
                out.push(f);
            }

            if out.is_empty() {
                // Keep one leaf-carrying field so that the struct keeps its length and validity.
                // The transform drops it again. Nothing else of this struct was kept, so these
                // are the first leaves of the struct and stay in ascending order.
                let file_field = file_fields.first()?;
                leaf_idxs.extend(start..start + n_columns(&file_field.dtype));
                out.push(file_field.clone());
            }

            Some(ArrowDataType::Struct(out))
        },
        (ArrowDataType::List(file_inner), DataType::List(target_inner)) => {
            let dtype = prune_arrow_dtype(
                &file_inner.dtype,
                target_inner,
                leaf_offset,
                leaf_idxs,
                pruned_any,
            )?;
            let mut f = file_inner.as_ref().clone();
            f.dtype = dtype;
            Some(ArrowDataType::List(Box::new(f)))
        },
        (ArrowDataType::LargeList(file_inner), DataType::List(target_inner)) => {
            let dtype = prune_arrow_dtype(
                &file_inner.dtype,
                target_inner,
                leaf_offset,
                leaf_idxs,
                pruned_any,
            )?;
            let mut f = file_inner.as_ref().clone();
            f.dtype = dtype;
            Some(ArrowDataType::LargeList(Box::new(f)))
        },
        #[cfg(feature = "dtype-array")]
        (ArrowDataType::FixedSizeList(file_inner, width), DataType::Array(target_inner, _)) => {
            let dtype = prune_arrow_dtype(
                &file_inner.dtype,
                target_inner,
                leaf_offset,
                leaf_idxs,
                pruned_any,
            )?;
            let mut f = file_inner.as_ref().clone();
            f.dtype = dtype;
            Some(ArrowDataType::FixedSizeList(Box::new(f), *width))
        },
        (file_dtype, _) if file_dtype.is_nested() => {
            // Maps, or a shape mismatch (e.g. struct in file, non-struct target). Keep the file
            // type as-is and let the regular transform handle or reject it.
            keep_all_leaves(file_dtype, leaf_offset, leaf_idxs);
            Some(file_dtype.clone())
        },
        (file_dtype, _) => {
            leaf_idxs.push(*leaf_offset);
            *leaf_offset += 1;
            Some(file_dtype.clone())
        },
    }
}

fn keep_all_leaves(dtype: &ArrowDataType, leaf_offset: &mut usize, leaf_idxs: &mut Vec<usize>) {
    let n = n_columns(dtype);
    leaf_idxs.extend(*leaf_offset..*leaf_offset + n);
    *leaf_offset += n;
}

impl ArrowFieldProjection {
    /// The field as it is in the file.
    #[inline]
    pub fn arrow_field(&self) -> &ArrowField {
        match self {
            Self::Plain(field) => field,
            Self::Mapped { arrow_field, .. } => arrow_field,
        }
    }

    /// The field to decode from the row group: the file field, or the pruned field when a nested
    /// projection applies.
    #[inline]
    pub fn decode_field(&self) -> &ArrowField {
        match self {
            Self::Mapped {
                nested: Some(nested),
                ..
            } => &nested.field,
            _ => self.arrow_field(),
        }
    }

    /// Indices of the parquet leaf columns to read, relative to the leaves under this root column.
    /// `None` means all of them.
    #[inline]
    pub fn leaf_idxs(&self) -> Option<&[usize]> {
        match self {
            Self::Mapped {
                nested: Some(nested),
                ..
            } => Some(&nested.leaf_idxs),
            _ => None,
        }
    }

    pub fn output_name(&self) -> &PlSmallStr {
        match self {
            Self::Plain(field) => &field.name,
            Self::Mapped { output_name, .. } => output_name,
        }
    }

    pub fn output_dtype(&self) -> Cow<'_, DataType> {
        match self {
            Self::Plain(field) => Cow::Owned(DataType::from_arrow_field(field)),
            Self::Mapped { output_dtype, .. } => Cow::Borrowed(output_dtype),
        }
    }

    /// Transform a column with the file's field layout (e.g. statistics) to the output.
    pub fn apply_transform(&self, column: Column) -> PolarsResult<Column> {
        match self {
            Self::Plain(_) => Ok(column),
            Self::Mapped {
                transform,
                output_dtype,
                ..
            } => Self::apply(transform, output_dtype, column),
        }
    }

    /// Transform a column decoded with `decode_field()` to the output.
    pub fn apply_decode_transform(&self, column: Column) -> PolarsResult<Column> {
        match self {
            Self::Mapped {
                nested: Some(nested),
                output_dtype,
                ..
            } => Self::apply(&nested.transform, output_dtype, column),
            _ => self.apply_transform(column),
        }
    }

    fn apply(
        transform: &ColumnSelector,
        output_dtype: &DataType,
        column: Column,
    ) -> PolarsResult<Column> {
        let output_height = column.len();
        let out = transform.select_from_columns(&[column], output_height)?;

        debug_assert_eq!(out.dtype(), output_dtype);

        Ok(out)
    }
}
