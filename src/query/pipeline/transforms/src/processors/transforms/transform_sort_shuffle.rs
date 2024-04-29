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
use databend_common_expression::BlockMetaInfo;
use databend_common_exception::Result;
use databend_common_expression::group_hash_columns;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use strength_reduce::StrengthReducedU64;

use crate::processors::Transform;
use crate::processors::Transformer;

pub struct TransformSortAggregateShuffle {
    shuffle_nums: usize,
}

impl TransformSortAggregateShuffle {
    pub fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        shuffle_nums: usize,
    ) -> Result<Box<dyn Processor>> {
        Ok(Transformer::create(
            input,
            output,
            TransformSortAggregateShuffle { shuffle_nums },
        ))
    }


}

#[async_trait::async_trait]
impl Transform for TransformSortAggregateShuffle {
    const NAME: &'static str = "SortAggregateShuffle";

    fn transform(&mut self, block: DataBlock) -> Result<DataBlock> {
        shuffle(block, self.shuffle_nums)
    }
}

pub fn shuffle(data_block: DataBlock, shuffle_nums: usize) -> Result<DataBlock> {
    let num_rows = data_block.num_rows();
    let last_col = data_block.get_last_column();
    let col = std::slice::from_ref(last_col);
    let mut hashes = vec![0_u64; num_rows];
    group_hash_columns(col, &mut hashes);

    // Shuffle by output_len
    let mut indices = Vec::with_capacity(num_rows);
    let mods = StrengthReducedU64::new(shuffle_nums as u64);
    for hash in hashes {
        indices.push((hash % mods) as u8);
    }

    let scatter_blocks = DataBlock::scatter(&data_block, &indices, shuffle_nums)?;

    Ok(ShuffleBlocks::create_block(scatter_blocks, shuffle_nums))
}

#[derive(Debug)]
pub struct ShuffleBlocks {
    pub shuffle_nums: usize,
    pub blocks: Vec<DataBlock>,
}

impl ShuffleBlocks {
    pub fn create_block(blocks: Vec<DataBlock>, shuffle_nums: usize) -> DataBlock {
        DataBlock::empty_with_meta(Box::new(ShuffleBlocks {
            blocks,
            shuffle_nums,
        }))
    }
}

impl Clone for ShuffleBlocks {
    fn clone(&self) -> Self {
        unreachable!("ShuffleBlocks should not be cloned")
    }
}

impl serde::Serialize for ShuffleBlocks {
    fn serialize<S>(&self, _: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        unreachable!("ShuffleBlocks should not be serialized")
    }
}

impl<'de> serde::Deserialize<'de> for ShuffleBlocks {
    fn deserialize<D>(_: D) -> Result<Self, D::Error>
    where D: serde::Deserializer<'de> {
        unreachable!("ShuffleBlocks should not be deserialized")
    }
}

impl BlockMetaInfo for ShuffleBlocks {
    fn typetag_deserialize(&self) {
        unimplemented!("ShuffleBlocks does not support exchanging between multiple nodes")
    }

    fn typetag_name(&self) -> &'static str {
        unimplemented!("ShuffleBlocks does not support exchanging between multiple nodes")
    }

    fn equals(&self, _: &Box<dyn BlockMetaInfo>) -> bool {
        unimplemented!("Unimplemented equals for ShuffleBlocks")
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        unimplemented!("Unimplemented clone for ShuffleBlocks")
    }
}
