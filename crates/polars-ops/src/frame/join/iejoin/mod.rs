mod filtered_bit_array;

use filtered_bit_array::FilteredBitArray;
use polars_core::chunked_array::ChunkedArray;
use polars_core::datatypes::{IdxCa, NumericNative, PolarsNumericType};
use polars_core::frame::DataFrame;
use polars_core::prelude::*;
use polars_core::{with_match_physical_numeric_polars_type, POOL};
use polars_error::{polars_err, PolarsResult};
use polars_utils::binary_search::ExponentialSearch;
use polars_utils::slice::GetSaferUnchecked;
use polars_utils::total_ord::{TotalEq, TotalOrd};
use polars_utils::IdxSize;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use crate::frame::_finish_join;

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum InequalityOperator {
    #[default]
    Lt,
    LtEq,
    Gt,
    GtEq,
}

impl InequalityOperator {
    fn is_strict(&self) -> bool {
        matches!(self, InequalityOperator::Gt | InequalityOperator::Lt)
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Default, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct IEJoinOptions {
    pub operator1: InequalityOperator,
    pub operator2: InequalityOperator,
}

#[allow(clippy::too_many_arguments)]
fn ie_join_impl_t<T: PolarsNumericType>(
    slice: Option<(i64, usize)>,
    l1_order: IdxCa,
    l2_order: &[IdxSize],
    op1: InequalityOperator,
    op2: InequalityOperator,
    x: Series,
    y_ordered: Series,
    left_height: usize,
) -> PolarsResult<(Vec<IdxSize>, Vec<IdxSize>)> {
    // Create a bit array with order corresponding to L1,
    // denoting which entries have been visited while traversing L2.
    let mut bit_array = FilteredBitArray::from_len_zeroed(l1_order.len());

    let mut left_row_idx: Vec<IdxSize> = vec![];
    let mut right_row_idx: Vec<IdxSize> = vec![];

    let slice_end = match slice {
        Some((offset, len)) if offset >= 0 => Some(offset.saturating_add_unsigned(len as u64)),
        _ => None,
    };
    let mut match_count = 0;

    let ca: &ChunkedArray<T> = x.as_ref().as_ref();
    let l1_array = build_l1_array(ca, &l1_order, left_height as IdxSize)?;

    if op2.is_strict() {
        // For strict inequalities, we rely on using a stable sort of l2 so that
        // p values only increase as we traverse a run of equal y values.
        // To handle inclusive comparisons in x and duplicate x values we also need the
        // sort of l1 to be stable, so that the left hand side entries come before the right
        // hand side entries (as we mark visited entries from the right hand side).
        for &p in l2_order {
            match_count += unsafe {
                l1_array.process_entry(
                    p as usize,
                    &mut bit_array,
                    op1,
                    &mut left_row_idx,
                    &mut right_row_idx,
                )
            };

            if slice_end.is_some_and(|end| match_count >= end) {
                break;
            }
        }
    } else {
        with_match_physical_numeric_polars_type!(y_ordered.dtype(), |$Ty| {
            let ca: &ChunkedArray<$Ty> = y_ordered.as_ref().as_ref().as_ref();
            traverse_l2_array_non_strict(ca, &l2_order, &l1_array, op1, slice_end, &mut bit_array, &mut left_row_idx, &mut right_row_idx);
        });
    }
    Ok((left_row_idx, right_row_idx))
}

/// Inequality join. Matches rows between two DataFrames using two inequality operators
/// (one of [<, <=, >, >=]).
/// Based on Khayyat et al. 2015, "Lightning Fast and Space Efficient Inequality Joins"
/// and extended to work with duplicate values.
pub fn iejoin(
    left: &DataFrame,
    right: &DataFrame,
    selected_left: Vec<Series>,
    selected_right: Vec<Series>,
    options: &IEJoinOptions,
    suffix: Option<PlSmallStr>,
    slice: Option<(i64, usize)>,
) -> PolarsResult<DataFrame> {
    if selected_left.len() != 2 {
        return Err(
            polars_err!(ComputeError: "IEJoin requires exactly two expressions from the left DataFrame"),
        );
    };
    if selected_right.len() != 2 {
        return Err(
            polars_err!(ComputeError: "IEJoin requires exactly two expressions from the right DataFrame"),
        );
    };

    let op1 = options.operator1;
    let op2 = options.operator2;

    // Determine the sort order based on the comparison operators used.
    // We want to sort L1 so that "x[i] op1 x[j]" is true for j > i,
    // and L2 so that "y[i] op2 y[j]" is true for j < i
    // (except in the case of duplicates and strict inequalities).
    // Note that the algorithms published in Khayyat et al. have incorrect logic for
    // determining whether to sort descending.
    let l1_descending = matches!(op1, InequalityOperator::Gt | InequalityOperator::GtEq);
    let l2_descending = matches!(op2, InequalityOperator::Lt | InequalityOperator::LtEq);

    let mut x = selected_left[0].to_physical_repr().into_owned();
    x.extend(&selected_right[0].to_physical_repr())?;
    // Rechunk because we will gather.
    let x = x.rechunk();

    let mut y = selected_left[1].to_physical_repr().into_owned();
    y.extend(&selected_right[1].to_physical_repr())?;
    // Rechunk because we will gather.
    let y = y.rechunk();

    let l1_sort_options = SortOptions::default()
        .with_maintain_order(true)
        .with_nulls_last(false)
        .with_order_descending(l1_descending);
    // Get ordering of x, skipping any null entries as these cannot be matches
    let l1_order = x
        .arg_sort(l1_sort_options)
        .slice(x.null_count() as i64, x.len() - x.null_count());

    let y_ordered = unsafe { y.take_unchecked(&l1_order) };
    let l2_sort_options = SortOptions::default()
        .with_maintain_order(true)
        .with_nulls_last(false)
        .with_order_descending(l2_descending);
    // Get the indexes into l1, ordered by y values.
    // l2_order is the same as "p" from Khayyat et al.
    let l2_order = y_ordered
        .arg_sort(l2_sort_options)
        .slice(
            y_ordered.null_count() as i64,
            y_ordered.len() - y_ordered.null_count(),
        )
        .rechunk();
    let l2_order = l2_order.downcast_get(0).unwrap().values().as_slice();

    let (left_row_idx, right_row_idx) = with_match_physical_numeric_polars_type!(x.dtype(), |$T| {
         ie_join_impl_t::<$T>(
            slice,
            l1_order,
            l2_order,
            op1,
            op2,
            x,
            y_ordered,
            left.height()
        )
    })?;

    debug_assert_eq!(left_row_idx.len(), right_row_idx.len());
    let left_row_idx = IdxCa::from_vec("".into(), left_row_idx);
    let right_row_idx = IdxCa::from_vec("".into(), right_row_idx);
    let (left_row_idx, right_row_idx) = match slice {
        None => (left_row_idx, right_row_idx),
        Some((offset, len)) => (
            left_row_idx.slice(offset, len),
            right_row_idx.slice(offset, len),
        ),
    };

    let (join_left, join_right) = unsafe {
        POOL.join(
            || left.take_unchecked(&left_row_idx),
            || right.take_unchecked(&right_row_idx),
        )
    };

    _finish_join(join_left, join_right, suffix)
}

/// Item in L1 array used in the IEJoin algorithm
#[derive(Clone, Copy, Debug)]
struct L1Item<T> {
    /// 1 based index for entries from the LHS df, or -1 based index for entries from the RHS
    row_index: i64,
    /// X value
    value: T,
}

trait L1Array {
    unsafe fn process_entry(
        &self,
        l1_index: usize,
        bit_array: &mut FilteredBitArray,
        op1: InequalityOperator,
        left_row_ids: &mut Vec<IdxSize>,
        right_row_ids: &mut Vec<IdxSize>,
    ) -> i64;

    unsafe fn process_lhs_entry(
        &self,
        l1_index: usize,
        bit_array: &FilteredBitArray,
        op1: InequalityOperator,
        left_row_ids: &mut Vec<IdxSize>,
        right_row_ids: &mut Vec<IdxSize>,
    ) -> i64;

    unsafe fn mark_visited(&self, index: usize, bit_array: &mut FilteredBitArray);
}

/// Find the position in the L1 array where we should begin checking for matches,
/// given the index in L1 corresponding to the current position in L2.
unsafe fn find_search_start_index<T>(
    l1_array: &[L1Item<T>],
    index: usize,
    operator: InequalityOperator,
) -> usize
where
    T: NumericNative,
    T: TotalOrd,
{
    let sub_l1 = l1_array.get_unchecked_release(index..);
    let value = l1_array.get_unchecked_release(index).value;

    match operator {
        InequalityOperator::Gt => {
            sub_l1.partition_point_exponential(|a| a.value.tot_ge(&value)) + index
        },
        InequalityOperator::Lt => {
            sub_l1.partition_point_exponential(|a| a.value.tot_le(&value)) + index
        },
        InequalityOperator::GtEq => {
            sub_l1.partition_point_exponential(|a| value.tot_lt(&a.value)) + index
        },
        InequalityOperator::LtEq => {
            sub_l1.partition_point_exponential(|a| value.tot_gt(&a.value)) + index
        },
    }
}

fn find_matches_in_l1<T>(
    l1_array: &[L1Item<T>],
    l1_index: usize,
    row_index: i64,
    bit_array: &FilteredBitArray,
    op1: InequalityOperator,
    left_row_ids: &mut Vec<IdxSize>,
    right_row_ids: &mut Vec<IdxSize>,
) -> i64
where
    T: NumericNative,
    T: TotalOrd,
{
    debug_assert!(row_index > 0);
    let mut match_count = 0;

    // This entry comes from the left hand side DataFrame.
    // Find all following entries in L1 (meaning they satisfy the first operator)
    // that have already been visited (so satisfy the second operator).
    // Because we use a stable sort for l2, we know that we won't find any
    // matches for duplicate y values when traversing forwards in l1.
    let start_index = unsafe { find_search_start_index(l1_array, l1_index, op1) };
    unsafe {
        bit_array.on_set_bits_from(start_index, |set_bit: usize| {
            // SAFETY
            // set bit is within bounds.
            let right_row_index = l1_array.get_unchecked_release(set_bit).row_index;
            debug_assert!(right_row_index < 0);
            left_row_ids.push((row_index - 1) as IdxSize);
            right_row_ids.push((-right_row_index) as IdxSize - 1);
            match_count += 1;
        })
    };

    match_count
}

impl<T> L1Array for Vec<L1Item<T>>
where
    T: NumericNative,
{
    unsafe fn process_entry(
        &self,
        l1_index: usize,
        bit_array: &mut FilteredBitArray,
        op1: InequalityOperator,
        left_row_ids: &mut Vec<IdxSize>,
        right_row_ids: &mut Vec<IdxSize>,
    ) -> i64 {
        let row_index = self.get_unchecked_release(l1_index).row_index;
        let from_lhs = row_index > 0;
        if from_lhs {
            find_matches_in_l1(
                self,
                l1_index,
                row_index,
                bit_array,
                op1,
                left_row_ids,
                right_row_ids,
            )
        } else {
            bit_array.set_bit_unchecked(l1_index);
            0
        }
    }

    unsafe fn process_lhs_entry(
        &self,
        l1_index: usize,
        bit_array: &FilteredBitArray,
        op1: InequalityOperator,
        left_row_ids: &mut Vec<IdxSize>,
        right_row_ids: &mut Vec<IdxSize>,
    ) -> i64 {
        let row_index = self.get_unchecked_release(l1_index).row_index;
        let from_lhs = row_index > 0;
        if from_lhs {
            find_matches_in_l1(
                self,
                l1_index,
                row_index,
                bit_array,
                op1,
                left_row_ids,
                right_row_ids,
            )
        } else {
            0
        }
    }

    unsafe fn mark_visited(&self, index: usize, bit_array: &mut FilteredBitArray) {
        let from_lhs = self.get_unchecked_release(index).row_index > 0;
        // We only mark RHS entries as visited,
        // so that we don't try to match LHS entries with other LHS entries.
        if !from_lhs {
            bit_array.set_bit_unchecked(index);
        }
    }
}

/// Create a vector of L1 items from the array of LHS x values concatenated with RHS x values
/// and their ordering.
fn build_l1_array<T>(
    ca: &ChunkedArray<T>,
    order: &IdxCa,
    right_df_offset: IdxSize,
) -> PolarsResult<Vec<L1Item<T::Native>>>
where
    T: PolarsNumericType,
{
    assert_eq!(order.null_count(), 0);
    assert_eq!(ca.chunks().len(), 1);
    let arr = ca.downcast_get(0).unwrap();
    // Even if there are nulls, they will not be selected by order.
    let values = arr.values().as_slice();

    let mut array: Vec<L1Item<T::Native>> = Vec::with_capacity(ca.len());

    for order_arr in order.downcast_iter() {
        for index in order_arr.values().as_slice().iter().copied() {
            debug_assert!(arr.get(index as usize).is_some());
            let value = unsafe { *values.get_unchecked(index as usize) };
            let row_index = if index < right_df_offset {
                // Row from LHS
                index as i64 + 1
            } else {
                // Row from RHS
                -((index - right_df_offset) as i64) - 1
            };
            array.push(L1Item { row_index, value });
        }
    }

    Ok(array)
}

/// For the case of non-strict inequalities in l2,
/// traverse the y values in sorted order (specified by the order parameter),
/// adding left and right row indices corresponding to join matches.
/// When the l2 inequality is non-strict, we need to track runs of equal y values and only
/// check for matches after we reach the end of the run and have marked all rhs entries
/// in the run as visited.
/// The chunked array of y values before sorting should have rows ordered according to the L1 order.
fn traverse_l2_array_non_strict<TxNative, Ty>(
    ca: &ChunkedArray<Ty>,
    order: &[IdxSize],
    l1_array: &Vec<L1Item<TxNative>>,
    op1: InequalityOperator,
    slice_end: Option<i64>,
    bit_array: &mut FilteredBitArray,
    left_row_idx: &mut Vec<IdxSize>,
    right_row_idx: &mut Vec<IdxSize>,
) where
    Ty: PolarsNumericType,
    Ty::Native: TotalOrd,
    TxNative: NumericNative,
    TxNative: TotalOrd,
{
    assert_eq!(ca.chunks().len(), 1);
    let arr = ca.downcast_get(0).unwrap();
    // Even if there are nulls, they will not be selected by order.
    let values = arr.values().as_slice();

    let mut match_count = 0;
    let mut run_start = 0;
    let mut prev_value = Ty::Native::default();

    for (i, l1_index) in order.iter().copied().enumerate() {
        debug_assert!(arr.get(l1_index as usize).is_some());
        let value = unsafe { *values.get_unchecked_release(l1_index as usize) };

        if i > 0 && value.tot_ne(&prev_value) {
            for j in run_start..i {
                let p = unsafe { *order.get_unchecked_release(j) } as usize;
                match_count += unsafe {
                    l1_array.process_lhs_entry(p, bit_array, op1, left_row_idx, right_row_idx)
                };
            }

            if slice_end.is_some_and(|end| match_count >= end) {
                // Early return if we've reached the end of the required slice
                return;
            }

            run_start = i;
        }

        unsafe { l1_array.mark_visited(l1_index as usize, bit_array) };

        prev_value = value;
    }

    for j in run_start..order.len() {
        let p = unsafe { *order.get_unchecked_release(j) } as usize;
        unsafe { l1_array.process_lhs_entry(p, bit_array, op1, left_row_idx, right_row_idx) };
    }
}
