use polars_core::error::PolarsResult;
use polars_core::functions::concat_df_horizontal;
use polars_core::prelude::*;
use polars_core::utils::accumulate_dataframes_vertical;

use crate::operators::{DataChunk, PExecutionContext, Source, SourceResult};

/// A streaming source that horizontally concatenates data from multiple input sources
pub struct HConcatSource {
    sources: Vec<Box<dyn Source>>,
    source_schemas: Vec<SchemaRef>,
    source_data: Vec<Vec<DataChunk>>,
    chunk_index: IdxSize,
}

impl HConcatSource {
    pub(crate) fn new(sources: Vec<Box<dyn Source>>, source_schemas: Vec<SchemaRef>) -> Self {
        debug_assert_eq!(sources.len(), source_schemas.len());
        let mut source_data = Vec::with_capacity(sources.len());
        for _ in 0..sources.len() {
            source_data.push(Vec::new());
        }
        Self {
            sources,
            source_schemas,
            source_data,
            chunk_index: 0,
        }
    }
}

impl Source for HConcatSource {
    fn get_batches(&mut self, context: &PExecutionContext) -> PolarsResult<SourceResult> {
        // We need to get chunks of matching sizes from all sources, and ensure that
        // the row ordering is consistent between sources, while allowing for sources
        // to contain different numbers of rows (columns will be padded with nulls as required).
        // The number of chunks and rows per chunk for each result is determined from the first non-finished source.

        let mut chunk_sizes: Option<(Vec<usize>, usize)> = None;
        for (source, source_data) in self.sources.iter_mut().zip(self.source_data.iter_mut()) {
            match chunk_sizes {
                None => {
                    // Fetch new data unless there is previously retrieved data for this source that wasn't fully consumed
                    if source_data.is_empty() {
                        let source_result = source.get_batches(context)?;
                        match source_result {
                            SourceResult::Finished => continue,
                            SourceResult::GotMoreData(mut data) => {
                                // Reverse so we can pop chunks off the end while keeping order unchanged
                                data.reverse();
                                source_data.extend(data);
                            },
                        }
                    }
                    let sizes = source_data
                        .iter()
                        .rev()
                        .map(|chunk| chunk.data.height())
                        .collect::<Vec<usize>>();
                    let total_size = sizes.iter().sum();
                    chunk_sizes = Some((sizes, total_size));
                },
                Some((_, total_size)) => {
                    // Populate data for this source until the total number of rows is at least
                    // the required size, or we have exhausted the source
                    let mut source_total_size = source_data
                        .iter()
                        .map(|chunk| chunk.data.height())
                        .sum::<usize>();
                    while source_total_size < total_size {
                        let source_result = source.get_batches(context)?;
                        match source_result {
                            SourceResult::Finished => break,
                            SourceResult::GotMoreData(mut data) => {
                                source_total_size +=
                                    data.iter().map(|chunk| chunk.data.height()).sum::<usize>();
                                data.reverse();
                                source_data.extend(data);
                            },
                        }
                    }
                },
            }
        }

        match chunk_sizes {
            None => {
                // All sources are finished
                Ok(SourceResult::Finished)
            },
            Some((chunk_sizes, _)) => {
                let chunk_count = chunk_sizes.len();
                let mut chunks = Vec::with_capacity(chunk_count);

                for row_count in chunk_sizes {
                    let mut chunk_dfs = Vec::with_capacity(self.sources.len());

                    // For each chunk, build a DataFrame of the required height from each source
                    for (source_data, source_schema) in
                        self.source_data.iter_mut().zip(self.source_schemas.iter())
                    {
                        let mut source_dfs = Vec::new();
                        let mut source_rows = 0;
                        while source_rows < row_count
                            || source_data
                                .last()
                                .map(|chunk| chunk.data.height() == 0)
                                .unwrap_or(false)
                        {
                            let required_rows = row_count - source_rows;
                            let chunk = source_data.pop();
                            match chunk {
                                Some(DataChunk { chunk_index, data }) => {
                                    let chunk_height = data.height();
                                    let slice = data.slice(0, required_rows);
                                    let slice_height = slice.height();
                                    let remainder_len = chunk_height - slice_height;
                                    if remainder_len > 0 {
                                        let remainder =
                                            data.slice(slice_height as i64, remainder_len);
                                        let chunk_index = chunk_index;
                                        source_data.push(DataChunk::new(chunk_index, remainder));
                                    }
                                    if slice_height > 0 {
                                        source_rows += slice_height;
                                        source_dfs.push(slice);
                                    }
                                },
                                None => {
                                    break;
                                },
                            }
                        }
                        match source_dfs.len() {
                            0 => {
                                chunk_dfs.push(DataFrame::from(source_schema.as_ref()));
                            },
                            1 => {
                                chunk_dfs.push(source_dfs.pop().unwrap());
                            },
                            _ => {
                                chunk_dfs.push(accumulate_dataframes_vertical(source_dfs)?);
                            },
                        }
                    }

                    let mut concatenated = concat_df_horizontal(&chunk_dfs)?;
                    // Chunk data arrays must only have a single chunk, so rechunk in case we have
                    // had to vertically concatenate some arrays.
                    concatenated.as_single_chunk();
                    chunks.push(DataChunk::new(self.chunk_index, concatenated));

                    self.chunk_index += 1;
                }

                Ok(SourceResult::GotMoreData(chunks))
            },
        }
    }

    fn fmt(&self) -> &str {
        "hconcat"
    }
}
