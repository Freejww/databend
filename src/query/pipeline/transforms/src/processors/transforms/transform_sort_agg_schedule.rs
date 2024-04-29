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
use std::collections::VecDeque;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::EventCause;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;

use crate::processors::ShuffleBlocks;
struct OutputsBuffer {
    inner: Vec<VecDeque<DataBlock>>,
}

impl OutputsBuffer {
    pub fn create(capacity: usize, outputs: usize) -> OutputsBuffer {
        OutputsBuffer {
            inner: vec![capacity; outputs]
                .into_iter()
                .map(VecDeque::with_capacity)
                .collect::<Vec<_>>(),
        }
    }

    pub fn is_all_empty(&self) -> bool {
        self.inner.iter().all(|x| x.is_empty())
    }

    pub fn is_empty(&self, index: usize) -> bool {
        self.inner[index].is_empty()
    }

    pub fn is_full(&self) -> bool {
        self.inner.iter().any(|x| x.len() == x.capacity())
    }

    pub fn clear(&mut self, index: usize) {
        self.inner[index].clear();
    }

    pub fn pop(&mut self, index: usize) -> Option<DataBlock> {
        self.inner[index].pop_front()
    }

    pub fn push_back(&mut self, index: usize, block: DataBlock) -> usize {
        self.inner[index].push_back(block);
        self.inner[index].len()
    }
}

#[derive(PartialEq)]
enum PortStatus {
    Idle,
    HasData,
    NeedData,
    Finished,
}

struct PortWithStatus<Port> {
    pub status: PortStatus,
    pub port: Arc<Port>,
}

pub struct TransformSortAggregateSchedule {
    initialized: bool,

    finished_inputs: usize,
    finished_outputs: usize,

    waiting_outputs: Vec<usize>,
    waiting_inputs: VecDeque<usize>,

    buffer: OutputsBuffer,
    inputs: Vec<PortWithStatus<InputPort>>,
    outputs: Vec<PortWithStatus<OutputPort>>,
}

impl Processor for TransformSortAggregateSchedule {
    fn name(&self) -> String {
        String::from("TransformSortAggregateSchedule")
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event_with_cause(&mut self, cause: EventCause) -> Result<Event> {
        if let EventCause::Output(output_index) = &cause {
            let output = &mut self.outputs[*output_index];

            if output.port.is_finished() {
                if output.status != PortStatus::Finished {
                    self.finished_outputs += 1;
                    output.status = PortStatus::Finished;
                }

                self.buffer.clear(*output_index);

                self.wakeup_inputs();
                self.wakeup_outputs();
            } else if output.port.can_push() {
                if !self.buffer.is_empty(*output_index) {
                    let data_block = self.buffer.pop(*output_index).unwrap();
                    output.status = PortStatus::Idle;
                    output.port.push_data(Ok(data_block));

                    self.wakeup_inputs();
                    self.wakeup_outputs();
                } else if output.status != PortStatus::NeedData {
                    output.status = PortStatus::NeedData;
                    self.waiting_outputs.push(*output_index);
                }
            }
        }

        if !self.initialized && !self.waiting_outputs.is_empty() {
            self.initialized = true;
            for input in &self.inputs {
                input.port.set_need_data();
            }
        }

        if self.finished_outputs == self.outputs.len() {
            for input in &self.inputs {
                input.port.finish();
            }

            return Ok(Event::Finished);
        }

        if let EventCause::Input(input_index) = &cause {
            let input = &mut self.inputs[*input_index];

            if input.port.is_finished() {
                if input.status != PortStatus::Finished {
                    self.finished_inputs += 1;
                    input.status = PortStatus::Finished;
                }

                self.wakeup_outputs();
                self.wakeup_inputs();
            } else if input.port.has_data() {
                if !self.buffer.is_full() {
                    self.take_input_data_into_buffer(*input_index);

                    self.wakeup_outputs();
                    self.wakeup_inputs();
                } else if input.status != PortStatus::HasData {
                    input.status = PortStatus::HasData;
                    self.waiting_inputs.push_back(*input_index);
                }
            }
        }

        if self.finished_outputs == self.outputs.len() {
            for input in &self.inputs {
                input.port.finish();
            }

            return Ok(Event::Finished);
        }

        if self.finished_inputs == self.inputs.len() {
            for (index, output) in self.outputs.iter_mut().enumerate() {
                if self.buffer.is_empty(index) && output.status != PortStatus::Finished {
                    self.finished_outputs += 1;
                    output.status = PortStatus::Finished;
                    output.port.finish();
                }
            }

            if self.buffer.is_all_empty() {
                return Ok(Event::Finished);
            }
        }

        match self.waiting_outputs.is_empty() {
            true => Ok(Event::NeedConsume),
            false => Ok(Event::NeedData),
        }
    }

    fn details_status(&self) -> Option<String> {
        #[derive(Debug)]
        #[allow(dead_code)]
        struct Display {
            queue_status: Vec<(usize, usize)>,
            inputs: usize,
            finished_inputs: usize,
            outputs: usize,
            finished_outputs: usize,

            waiting_outputs: Vec<usize>,
            waiting_inputs: VecDeque<usize>,
        }

        let mut queue_status = vec![];
        for (idx, queue) in self.buffer.inner.iter().enumerate() {
            queue_status.push((idx, queue.len()));
        }

        Some(format!("{:?}", Display {
            queue_status,
            inputs: self.inputs.len(),
            outputs: self.outputs.len(),
            finished_inputs: self.finished_inputs,
            finished_outputs: self.finished_outputs,
            waiting_inputs: self.waiting_inputs.clone(),
            waiting_outputs: self.waiting_outputs.clone(),
        }))
    }
}

impl TransformSortAggregateSchedule {
    fn wakeup_inputs(&mut self) {
        while !self.waiting_inputs.is_empty() && !self.buffer.is_full() {
            let input_index = self.waiting_inputs.pop_front().unwrap();

            self.take_input_data_into_buffer(input_index);
        }
    }

    fn wakeup_outputs(&mut self) {
        let mut new_waiting_output = Vec::with_capacity(self.waiting_outputs.len());

        for waiting_output in &self.waiting_outputs {
            let output = &mut self.outputs[*waiting_output];

            if output.port.is_finished() {
                if output.status != PortStatus::Finished {
                    self.finished_outputs += 1;
                    output.status = PortStatus::Finished;
                }

                self.buffer.clear(*waiting_output);
                continue;
            }

            if self.buffer.is_empty(*waiting_output) {
                new_waiting_output.push(*waiting_output);
                continue;
            }

            let data_block = self.buffer.pop(*waiting_output).unwrap();
            output.status = PortStatus::Idle;
            output.port.push_data(Ok(data_block));
        }

        self.waiting_outputs = new_waiting_output;
    }

    fn take_input_data_into_buffer(&mut self, input_index: usize) {
        let input = &mut self.inputs[input_index];

        input.status = PortStatus::Idle;
        let mut data_block = input.port.pull_data().unwrap().unwrap();

        if let Some(block_meta) = data_block.take_meta() {
            if let Some(shuffle_meta) = ShuffleBlocks::downcast_from(block_meta) {
                for (index, block) in shuffle_meta.blocks.into_iter().enumerate() {
                    if (!block.is_empty() || block.get_meta().is_some())
                        && self.outputs[index].status != PortStatus::Finished
                    {
                        self.buffer.push_back(index, block);
                    }
                }
            }
        }

        if input.port.is_finished() {
            if input.status != PortStatus::Finished {
                self.finished_inputs += 1;
                input.status = PortStatus::Finished;
            }

            return;
        }

        input.port.set_need_data();
    }
}

impl TransformSortAggregateSchedule {
    pub fn create(inputs: usize, outputs: usize, buffer: usize) -> TransformSortAggregateSchedule {
        let mut inputs_port = Vec::with_capacity(inputs);
        let mut outputs_port = Vec::with_capacity(outputs);

        for _index in 0..inputs {
            inputs_port.push(PortWithStatus {
                status: PortStatus::Idle,
                port: InputPort::create(),
            });
        }

        for _index in 0..outputs {
            outputs_port.push(PortWithStatus {
                status: PortStatus::Idle,
                port: OutputPort::create(),
            });
        }

        TransformSortAggregateSchedule {
            initialized: false,
            finished_inputs: 0,
            finished_outputs: 0,
            inputs: inputs_port,
            outputs: outputs_port,
            buffer: OutputsBuffer::create(buffer, outputs),
            waiting_inputs: VecDeque::with_capacity(inputs),
            waiting_outputs: Vec::with_capacity(outputs),
        }
    }

    pub fn get_inputs(&self) -> Vec<Arc<InputPort>> {
        self.inputs.iter().map(|x| x.port.clone()).collect()
    }

    pub fn get_outputs(&self) -> Vec<Arc<OutputPort>> {
        self.outputs.iter().map(|x| x.port.clone()).collect()
    }
}
