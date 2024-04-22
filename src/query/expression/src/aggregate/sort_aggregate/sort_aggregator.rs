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

use std::mem::MaybeUninit;
use std::sync::Arc;

use bumpalo::Bump;
use databend_common_exception::Result;
use databend_common_hashtable::fast_memcmp;

use crate::aggregate::payload_row::sort_serialize_last_order_col;
use crate::read;
use crate::store;
use crate::types::binary::BinaryColumn;
use crate::types::ArgType;
use crate::types::BinaryType;
use crate::types::DataType;
use crate::types::DateType;
use crate::types::NumberColumn;
use crate::types::NumberType;
use crate::types::TimestampType;
use crate::types::ValueType;
use crate::with_number_mapped_type;
use crate::AggregateFunctionRef;
use crate::Column;
use crate::ColumnBuilder;
use crate::DataBlock;
use crate::Payload;
use crate::PayloadFlushState;
use crate::StateAddr;

#[derive(Clone, Default, Copy)]
pub struct GroupDesc {
    pub index: usize,
    pub count: usize,
}

pub struct SortProbeState {
    pub group_vector: Vec<GroupDesc>,
    pub addresses: Vec<*const u8>,
    pub state_places: Vec<StateAddr>,
    pub group_count: usize,
    pub last_is_init: bool,
    pub last_group_address: *const u8,
    pub equal_last_group: GroupDesc,
    pub last_order_col: Vec<MaybeUninit<u8>>,
}

impl Default for SortProbeState {
    fn default() -> Self {
        Self {
            group_vector: vec![],
            addresses: vec![],
            state_places: vec![],
            group_count: 0,
            last_is_init: false,
            last_group_address: std::ptr::null::<u8>(),
            equal_last_group: GroupDesc::default(),
            last_order_col: vec![],
        }
    }
}

impl SortProbeState {
    pub fn resize(&mut self, rows_num: usize) {
        self.group_vector.resize(rows_num, GroupDesc::default());
        self.addresses.resize(rows_num, std::ptr::null::<u8>());
        self.state_places.resize(rows_num, StateAddr::new(0));
        self.group_count = 0;
    }
}

unsafe impl Send for SortProbeState {}
unsafe impl Sync for SortProbeState {}

pub struct SortAggregator {
    pub payload: Payload,
    pub group_count: usize,
}

unsafe impl Send for SortAggregator {}
unsafe impl Sync for SortAggregator {}

impl SortAggregator {
    pub fn new(
        group_types: Vec<DataType>,
        aggrs: Vec<AggregateFunctionRef>,
        arena: Arc<Bump>,
    ) -> Self {
        Self {
            payload: Payload::new(arena, group_types, aggrs, true),
            group_count: 0,
        }
    }

    pub fn execute_one_block(
        &mut self,
        probe_state: &mut SortProbeState,
        flush_state: &mut PayloadFlushState,
        group_columns: &[Column],
        order_col: &Column,
        params: &[Vec<Column>],
        agg_states: &[Column],
        rows_num: usize,
    ) -> Result<(Vec<DataBlock>, usize)> {
        probe_state.resize(rows_num);

        let (blocks, group_count) = match order_col {
            // Order_col: Number Date Timestamp String Binary
            Column::Number(v) => with_number_mapped_type!(|NUM_TYPE| match v {
                NumberColumn::NUM_TYPE(_) => {
                    self.scan::<NumberType<NUM_TYPE>>(
                        probe_state,
                        flush_state,
                        group_columns,
                        order_col,
                        params,
                        agg_states,
                        rows_num,
                    )?
                }
            }),
            Column::Date(_) => self.scan::<DateType>(
                probe_state,
                flush_state,
                group_columns,
                order_col,
                params,
                agg_states,
                rows_num,
            )?,
            Column::Timestamp(_) => self.scan::<TimestampType>(
                probe_state,
                flush_state,
                group_columns,
                order_col,
                params,
                agg_states,
                rows_num,
            )?,
            Column::String(v) => {
                let v = &BinaryColumn::from(v.clone());
                self.scan_binary(
                    probe_state,
                    flush_state,
                    group_columns,
                    v,
                    params,
                    agg_states,
                    rows_num,
                )?
            }
            Column::Binary(v) => self.scan_binary(
                probe_state,
                flush_state,
                group_columns,
                v,
                params,
                agg_states,
                rows_num,
            )?,
            _ => unreachable!(),
        };

        probe_state.last_is_init = true;

        self.group_count += group_count;
        Ok((blocks, group_count))
    }

    pub fn scan_binary(
        &mut self,
        probe_state: &mut SortProbeState,
        flush_state: &mut PayloadFlushState,
        group_columns: &[Column],
        order_col: &BinaryColumn,
        params: &[Vec<Column>],
        agg_states: &[Column],
        rows_num: usize,
    ) -> Result<(Vec<DataBlock>, usize)> {
        // If last block is [1,2,3], and now block is [3,3,4]
        let mut count = 0;
        if probe_state.last_is_init {
            unsafe {
                let address = probe_state.last_order_col.as_ptr() as *const u8;
                let len = read::<u32>(address as _) as usize;
                let data_address = read::<u64>(address.add(4) as _) as usize as *const u8;
                let last = std::slice::from_raw_parts(data_address, len);
                for index in 0..rows_num {
                    let val = BinaryType::index_column_unchecked(order_col, index);

                    if val.len() != len || !fast_memcmp(last, val) {
                        break;
                    }
                    count += 1;
                }
            }
        };

        // Current block does not have rows that are the same as the previous block
        let blocks = if probe_state.last_is_init && count == 0 {
            self.flush_result(flush_state)?
        } else {
            vec![]
        };

        probe_state.equal_last_group = GroupDesc { index: 0, count };

        let mut start = count;
        let mut end = count;

        let all_rows_equal_last_group = start == rows_num;
        let mut prev = unsafe { BinaryType::index_column_unchecked(order_col, start) };

        // All rows not same to last group
        if !all_rows_equal_last_group {
            for idx in start + 1..rows_num {
                let cur = unsafe { BinaryType::index_column_unchecked(order_col, idx) };

                let equal = fast_memcmp(cur, prev);
                if !equal {
                    // Find a new group
                    probe_state.group_vector[probe_state.group_count] = GroupDesc {
                        index: start,
                        count: end - start + 1,
                    };
                    probe_state.group_count += 1;

                    start = idx;
                    prev = cur;
                }
                end = idx;
            }
            // Last group
            probe_state.group_vector[probe_state.group_count] = GroupDesc {
                index: start,
                count: end - start + 1,
            };
            probe_state.group_count += 1;
        }

        // Append
        self.payload
            .sort_reserve_append_rows(probe_state, group_columns);

        // Last group address and order_col value
        if !all_rows_equal_last_group {
            probe_state.last_group_address = probe_state.addresses[start];

            unsafe {
                let data = self
                    .payload
                    .arena
                    .alloc_slice_copy(order_col.index_unchecked(start));
                probe_state.last_order_col = Vec::with_capacity(4 + 8);
                let address = probe_state.last_order_col.as_ptr() as *const u8;
                store(&(data.len() as u32), address as *mut u8);
                store(&(data.as_ptr() as u64), address.add(4) as *mut u8);
            }
        }

        // Merge state
        if !self.payload.aggrs.is_empty() {
            self.merge_state(probe_state, params, agg_states, rows_num)?;
        }

        Ok((blocks, probe_state.group_count))
    }

    pub fn scan<T: ArgType>(
        &mut self,
        probe_state: &mut SortProbeState,
        flush_state: &mut PayloadFlushState,
        group_columns: &[Column],
        order_col: &Column,
        params: &[Vec<Column>],
        agg_states: &[Column],
        rows_num: usize,
    ) -> Result<(Vec<DataBlock>, usize)> {
        let col = T::try_downcast_column(order_col).unwrap();

        // If last block is [1,2,3], and now block is [3,3,4]
        let mut count = 0;
        if probe_state.last_is_init {
            unsafe {
                let address = probe_state.last_order_col.as_ptr() as *const u8;
                let last = read::<<T as ValueType>::Scalar>(address as _);
                for index in 0..rows_num {
                    let val = T::to_owned_scalar(T::index_column_unchecked(&col, index));

                    if !last.eq(&val) {
                        break;
                    }
                    count += 1;
                }
            }
        };

        // Current block does not have rows that are the same as the previous block
        let blocks = if probe_state.last_is_init && count == 0 {
            self.flush_result(flush_state)?
        } else {
            vec![]
        };

        probe_state.equal_last_group = GroupDesc { index: 0, count };

        let mut start = count;
        let mut end = count;

        let all_rows_equal_last_group = start == rows_num;
        let mut prev = unsafe { T::to_owned_scalar(T::index_column_unchecked(&col, start)) };

        // All rows not same to last group
        if !all_rows_equal_last_group {
            for idx in start + 1..rows_num {
                let cur = unsafe { T::to_owned_scalar(T::index_column_unchecked(&col, idx)) };
                if !cur.eq(&prev) {
                    // Find a new group
                    probe_state.group_vector[probe_state.group_count] = GroupDesc {
                        index: start,
                        count: end - start + 1,
                    };
                    probe_state.group_count += 1;

                    start = idx;
                    prev = cur;
                }
                end = idx;
            }
            // Last group
            probe_state.group_vector[probe_state.group_count] = GroupDesc {
                index: start,
                count: end - start + 1,
            };
            probe_state.group_count += 1;
        }

        // Append
        self.payload
            .sort_reserve_append_rows(probe_state, group_columns);

        if !all_rows_equal_last_group {
            // Record last group hash and address
            probe_state.last_group_address = probe_state.addresses[start];

            unsafe {
                sort_serialize_last_order_col(order_col, probe_state, start);
            }
        }

        // Merge state
        if !self.payload.aggrs.is_empty() {
            self.merge_state(probe_state, params, agg_states, rows_num)?;
        }

        Ok((blocks, probe_state.group_count))
    }

    pub fn merge_state(
        &mut self,
        probe_state: &mut SortProbeState,
        params: &[Vec<Column>],
        agg_states: &[Column],
        rows_num: usize,
    ) -> Result<()> {
        for i in 0..rows_num {
            probe_state.state_places[i] = unsafe {
                StateAddr::new(read::<u64>(
                    probe_state.addresses[i].add(self.payload.state_offset) as _
                ) as usize)
            }
        }

        let state_places = &probe_state.state_places.as_slice()[0..rows_num];

        if agg_states.is_empty() {
            for ((aggr, params), addr_offset) in self
                .payload
                .aggrs
                .iter()
                .zip(params.iter())
                .zip(self.payload.state_addr_offsets.iter())
            {
                aggr.accumulate_keys(state_places, *addr_offset, params, probe_state.group_count)?;
            }
        } else {
            for ((aggr, agg_state), addr_offset) in self
                .payload
                .aggrs
                .iter()
                .zip(agg_states.iter())
                .zip(self.payload.state_addr_offsets.iter())
            {
                aggr.batch_merge(state_places, *addr_offset, agg_state)?;
            }
        }

        Ok(())
    }

    pub fn merge_result(&self, flush_state: &mut PayloadFlushState) -> Result<bool> {
        if self.payload.flush(flush_state) {
            let row_count = flush_state.row_count;

            flush_state.aggregate_results.clear();
            for (aggr, addr_offset) in self
                .payload
                .aggrs
                .iter()
                .zip(self.payload.state_addr_offsets.iter())
            {
                let return_type = aggr.return_type()?;
                let mut builder = ColumnBuilder::with_capacity(&return_type, row_count * 4);

                aggr.batch_merge_result(
                    &flush_state.state_places.as_slice()[0..row_count],
                    *addr_offset,
                    &mut builder,
                )?;
                flush_state.aggregate_results.push(builder.build());
            }
            return Ok(true);
        }
        Ok(false)
    }

    pub fn flush_result(&mut self, flush_state: &mut PayloadFlushState) -> Result<Vec<DataBlock>> {
        let mut blocks = vec![];
        flush_state.clear();

        loop {
            if self.merge_result(flush_state)? {
                let mut cols = flush_state.take_aggregate_results();
                cols.extend_from_slice(&flush_state.take_group_columns());
                blocks.push(DataBlock::new_from_columns(cols));
            } else {
                break;
            }
        }

        let payload = Payload::new(
            Arc::new(Bump::new()),
            self.payload.group_types.clone(),
            self.payload.aggrs.clone(),
            true,
        );
        let _ = std::mem::replace(&mut self.payload, payload);
        self.group_count = 0;

        Ok(blocks)
    }
}
