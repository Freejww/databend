// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use bumpalo::Bump;
use databend_common_catalog::plan::AggIndexMeta;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::PayloadFlushState;
use databend_common_expression::SortAggregator;
use databend_common_expression::SortProbeState;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_transforms::processors::AccumulatingTransform;
use databend_common_pipeline_transforms::processors::AccumulatingTransformer;

use crate::pipelines::processors::transforms::aggregator::transform_aggregate_partial::aggregate_arguments;
use crate::pipelines::processors::transforms::aggregator::AggregatorParams;

enum SortAggregate {
    MovedOut,
    SortAggregator(SortAggregator),
}

impl Default for SortAggregate {
    fn default() -> Self {
        Self::MovedOut
    }
}

pub struct TransformSortAggregate {
    sort_aggregator: SortAggregate,
    probe_state: SortProbeState,
    params: Arc<AggregatorParams>,
    flush_state: PayloadFlushState,
    reach_limit: bool,
}

impl TransformSortAggregate {
    pub fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        params: Arc<AggregatorParams>,
    ) -> Result<Box<dyn Processor>> {
        let arena = Arc::new(Bump::new());
        let sort_aggregator = SortAggregator::new(
            params.group_data_types.clone(),
            params.aggregate_functions.clone(),
            arena,
        );

        Ok(AccumulatingTransformer::create(
            input,
            output,
            TransformSortAggregate {
                sort_aggregator: SortAggregate::SortAggregator(sort_aggregator),
                probe_state: SortProbeState::default(),
                params,
                flush_state: PayloadFlushState::default(),
                reach_limit: false,
            },
        ))
    }
}

impl AccumulatingTransform for TransformSortAggregate {
    const NAME: &'static str = "TransformSortAggregate";

    fn transform(&mut self, data: DataBlock) -> Result<Vec<DataBlock>> {
        let is_agg_index_block = data
            .get_meta()
            .and_then(AggIndexMeta::downcast_ref_from)
            .map(|index| index.is_agg)
            .unwrap_or_default();

        let block = data.convert_to_full();
        let order_col = block.get_last_column();

        let group_columns = self
            .params
            .group_columns
            .iter()
            .map(|&index| block.get_by_offset(index))
            .collect::<Vec<_>>();

        let group_columns = group_columns
            .iter()
            .map(|c| c.value.as_column().unwrap().clone())
            .collect::<Vec<_>>();

        let rows_num = block.num_rows();

        let (params_columns, agg_states) = match self.params.aggregate_functions.is_empty() {
            true => (vec![], vec![]),
            false => {
                if is_agg_index_block {
                    (
                        vec![],
                        (0..self.params.aggregate_functions.len())
                            .map(|index| {
                                block
                                    .get_by_offset(
                                        block.num_columns()
                                            - 1
                                            - self.params.aggregate_functions.len()
                                            + index,
                                    )
                                    .value
                                    .as_column()
                                    .cloned()
                                    .unwrap()
                            })
                            .collect(),
                    )
                } else {
                    (aggregate_arguments(&block, &self.params)?, vec![])
                }
            }
        };

        match &mut self.sort_aggregator {
            SortAggregate::MovedOut => unreachable!(),
            SortAggregate::SortAggregator(sort_aggregator) => {
                let (blocks, _) = sort_aggregator.execute_one_block(
                    &mut self.probe_state,
                    &mut self.flush_state,
                    &group_columns,
                    order_col,
                    &params_columns,
                    &agg_states,
                    rows_num,
                )?;
                Ok(blocks)
            }
        }
    }

    fn on_finish(&mut self, _output: bool) -> Result<Vec<DataBlock>> {
        let sort_aggregator = std::mem::take(&mut self.sort_aggregator);
        let mut blocks = vec![];
        let mut rows = 0;
        self.flush_state.clear();
        match sort_aggregator {
            SortAggregate::MovedOut => unreachable!(),
            SortAggregate::SortAggregator(sort_aggregator) => loop {
                if sort_aggregator.merge_result(&mut self.flush_state)? {
                    let mut cols = self.flush_state.take_aggregate_results();
                    cols.extend_from_slice(&self.flush_state.take_group_columns());
                    rows += cols[0].len();
                    blocks.push(DataBlock::new_from_columns(cols));

                    if rows >= self.params.limit.unwrap_or(usize::MAX) {
                        log::info!(
                            "reach limit optimization in flush agg hashtable, current {}, total {}",
                            rows,
                            sort_aggregator.group_count,
                        );
                        self.reach_limit = true;
                        break;
                    }
                } else {
                    break;
                }
            },
        }

        if blocks.is_empty() {
            blocks.push(self.params.empty_result_block());
        }

        Ok(blocks)
    }
}
