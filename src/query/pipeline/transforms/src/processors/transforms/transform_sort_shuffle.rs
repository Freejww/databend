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

use std::any::Any;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::group_hash_columns;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use strength_reduce::StrengthReducedU64;
pub struct TransformSortAggregateShuffle {
    input: Arc<InputPort>,
    outputs: Vec<Arc<OutputPort>>,
    input_data: Option<DataBlock>,
    outputs_data: Vec<Option<DataBlock>>,
}

impl TransformSortAggregateShuffle {
    pub fn create(output_len: usize) -> Result<Self> {
        let input = InputPort::create();
        let outputs = (0..output_len).map(|_| OutputPort::create()).collect();
        let outputs_data = (0..output_len).map(|_| None).collect();

        Ok(Self {
            input,
            outputs,
            input_data: None,
            outputs_data,
        })
    }

    pub fn get_input(&self) -> Arc<InputPort> {
        self.input.clone()
    }
    pub fn get_outputs(&self) -> Vec<Arc<OutputPort>> {
        self.outputs.clone()
    }
}

impl Processor for TransformSortAggregateShuffle {
    fn name(&self) -> String {
        "SortAggregateShuffle".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        let all_finished = self.outputs.iter().all(|x| x.is_finished());

        if all_finished {
            self.input.finish();
            return Ok(Event::Finished);
        }

        if self.input.is_finished() {
            self.outputs.iter_mut().for_each(|x| x.finish());
            return Ok(Event::Finished);
        }

        if self
            .outputs
            .iter()
            .any(|x| !x.is_finished() && !x.can_push())
        {
            self.input.set_not_need_data();
            return Ok(Event::NeedConsume);
        }

        if self.outputs_data.iter().any(|x| x.is_some()) {
            for i in 0..self.outputs.len() {
                if let Some(output_data) = self.outputs_data[i].take() {
                    self.outputs[i].push_data(Ok(output_data));
                }
            }
            return Ok(Event::NeedConsume);
        }

        if self.input.has_data() {
            self.input_data = Some(self.input.pull_data().unwrap()?);
            return Ok(Event::Sync);
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        if let Some(data_block) = self.input_data.take() {
            let num_rows = data_block.num_rows();
            let output_len = self.outputs.len();
            let order_col = data_block.get_last_column();
            let col = std::slice::from_ref(order_col);
            let mut hashes = vec![0_u64; num_rows];
            group_hash_columns(col, &mut hashes);

            // Shuffle by output_len
            let mut indices = Vec::with_capacity(num_rows);
            let mods = StrengthReducedU64::new(output_len as u64);
            for hash in hashes {
                indices.push((hash % mods) as u8);
            }

            let scatter_blocks = DataBlock::scatter(&data_block, &indices, output_len)?;

            for (idx, data_block) in scatter_blocks.into_iter().enumerate() {
                self.outputs_data[idx] = match data_block.is_empty() {
                    true => None,
                    false => Some(data_block),
                }
            }
        }
        Ok(())
    }
}
