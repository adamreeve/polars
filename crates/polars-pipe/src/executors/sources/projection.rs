use std::sync::Arc;

use polars_core::prelude::SchemaRef;
use smartstring::alias::String as SmartString;

use crate::operators::{PExecutionContext, PolarsResult, Source, SourceResult};

/// A source that applies a projection directly to an underlying source.
/// This is required so that union and hconcat sources can stream inputs that consist of
/// just a source and fast projection.
pub(crate) struct FastProjectionSource {
    source: Box<dyn Source>,
    columns: Arc<[SmartString]>,
    input_schema: SchemaRef,
}

impl FastProjectionSource {
    pub(crate) fn new(
        source: Box<dyn Source>,
        columns: Arc<[SmartString]>,
        input_schema: SchemaRef,
    ) -> Self {
        FastProjectionSource {
            source,
            columns,
            input_schema,
        }
    }
}

impl Source for FastProjectionSource {
    fn get_batches(&mut self, context: &PExecutionContext) -> PolarsResult<SourceResult> {
        Ok(match self.source.get_batches(context)? {
            SourceResult::Finished => SourceResult::Finished,
            SourceResult::GotMoreData(chunks) => {
                let mut projected = Vec::with_capacity(chunks.len());
                for chunk in chunks {
                    let data = chunk
                        .data
                        .select_with_schema_unchecked(self.columns.as_ref(), &self.input_schema)?;
                    projected.push(chunk.with_data(data));
                }
                SourceResult::GotMoreData(projected)
            },
        })
    }

    fn fmt(&self) -> &str {
        "fast-projection-source"
    }
}
