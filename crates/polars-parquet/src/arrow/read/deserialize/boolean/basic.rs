use std::collections::VecDeque;

use arrow::array::BooleanArray;
use arrow::bitmap::utils::BitmapIter;
use arrow::bitmap::MutableBitmap;
use arrow::datatypes::ArrowDataType;
use polars_error::PolarsResult;

use super::super::utils::{
    extend_from_decoder, next, DecodedState, Decoder, MaybeNext, OptionalPageValidity,
};
use super::super::{utils, PagesIter};
use crate::parquet::deserialize::{HybridDecoderBitmapIter, HybridRleBooleanIter};
use crate::parquet::encoding::{hybrid_rle, Encoding};
use crate::parquet::page::{split_buffer, DataPage, DictPage};

#[derive(Debug)]
struct Values<'a>(BitmapIter<'a>);

impl<'a> Values<'a> {
    pub fn try_new(page: &'a DataPage) -> PolarsResult<Self> {
        let values = split_buffer(page)?.values;

        Ok(Self(BitmapIter::new(values, 0, values.len() * 8)))
    }
}

// The state of a required DataPage with a boolean physical type
#[derive(Debug)]
struct Required<'a> {
    values: &'a [u8],
    // invariant: offset <= length;
    offset: usize,
    length: usize,
}

impl<'a> Required<'a> {
    pub fn new(page: &'a DataPage) -> Self {
        Self {
            values: page.buffer(),
            offset: 0,
            length: page.num_values(),
        }
    }
}

// The state of a `DataPage` of `Boolean` parquet boolean type
#[derive(Debug)]
enum State<'a> {
    Optional(OptionalPageValidity<'a>, Values<'a>),
    Required(Required<'a>),
    RleOptional(
        OptionalPageValidity<'a>,
        HybridRleBooleanIter<'a, HybridDecoderBitmapIter<'a>>,
    ),
}

impl<'a> State<'a> {
    pub fn len(&self) -> usize {
        match self {
            State::Optional(validity, _) => validity.len(),
            State::Required(page) => page.length - page.offset,
            State::RleOptional(optional, _) => optional.len(),
        }
    }
}

impl<'a> utils::PageState<'a> for State<'a> {
    fn len(&self) -> usize {
        self.len()
    }
}

impl DecodedState for (MutableBitmap, MutableBitmap) {
    fn len(&self) -> usize {
        self.0.len()
    }
}

#[derive(Default)]
struct BooleanDecoder {}

impl<'a> Decoder<'a> for BooleanDecoder {
    type State = State<'a>;
    type Dict = ();
    type DecodedState = (MutableBitmap, MutableBitmap);

    fn build_state(
        &self,
        page: &'a DataPage,
        _: Option<&'a Self::Dict>,
    ) -> PolarsResult<Self::State> {
        let is_optional = utils::page_is_optional(page);

        match (page.encoding(), is_optional) {
            (Encoding::Plain, true) => Ok(State::Optional(
                OptionalPageValidity::try_new(page)?,
                Values::try_new(page)?,
            )),
            (Encoding::Plain, false) => Ok(State::Required(Required::new(page))),
            (Encoding::Rle, true) => {
                let optional = OptionalPageValidity::try_new(page)?;
                let values = split_buffer(page)?.values;
                // For boolean values the length is pre-pended.
                let (_len_in_bytes, values) = values.split_at(4);
                let iter = hybrid_rle::Decoder::new(values, 1);
                let values = HybridDecoderBitmapIter::new(iter, page.num_values());
                let values = HybridRleBooleanIter::new(values);
                Ok(State::RleOptional(optional, values))
            },
            _ => Err(utils::not_implemented(page)),
        }
    }

    fn with_capacity(&self, capacity: usize) -> Self::DecodedState {
        (
            MutableBitmap::with_capacity(capacity),
            MutableBitmap::with_capacity(capacity),
        )
    }

    fn extend_from_state(
        &self,
        state: &mut Self::State,
        decoded: &mut Self::DecodedState,
        remaining: usize,
    ) -> PolarsResult<()> {
        let (values, validity) = decoded;
        match state {
            State::Optional(page_validity, page_values) => extend_from_decoder(
                validity,
                page_validity,
                Some(remaining),
                values,
                &mut page_values.0,
            ),
            State::Required(page) => {
                let remaining = remaining.min(page.length - page.offset);
                values.extend_from_slice(page.values, page.offset, remaining);
                page.offset += remaining;
            },
            State::RleOptional(page_validity, page_values) => {
                utils::extend_from_decoder(
                    validity,
                    page_validity,
                    Some(remaining),
                    values,
                    &mut *page_values,
                );
            },
        }
        Ok(())
    }

    fn deserialize_dict(&self, _: &DictPage) -> Self::Dict {}
}

fn finish(
    data_type: &ArrowDataType,
    values: MutableBitmap,
    validity: MutableBitmap,
) -> BooleanArray {
    BooleanArray::new(data_type.clone(), values.into(), validity.into())
}

/// An iterator adapter over [`PagesIter`] assumed to be encoded as boolean arrays
#[derive(Debug)]
pub struct Iter<I: PagesIter> {
    iter: I,
    data_type: ArrowDataType,
    items: VecDeque<(MutableBitmap, MutableBitmap)>,
    chunk_size: Option<usize>,
    remaining: usize,
}

impl<I: PagesIter> Iter<I> {
    pub fn new(
        iter: I,
        data_type: ArrowDataType,
        chunk_size: Option<usize>,
        num_rows: usize,
    ) -> Self {
        Self {
            iter,
            data_type,
            items: VecDeque::new(),
            chunk_size,
            remaining: num_rows,
        }
    }
}

impl<I: PagesIter> Iterator for Iter<I> {
    type Item = PolarsResult<BooleanArray>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let maybe_state = next(
                &mut self.iter,
                &mut self.items,
                &mut None,
                &mut self.remaining,
                self.chunk_size,
                &BooleanDecoder::default(),
            );
            match maybe_state {
                MaybeNext::Some(Ok((values, validity))) => {
                    return Some(Ok(finish(&self.data_type, values, validity)))
                },
                MaybeNext::Some(Err(e)) => return Some(Err(e)),
                MaybeNext::None => return None,
                MaybeNext::More => continue,
            }
        }
    }
}
