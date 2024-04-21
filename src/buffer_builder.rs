/*
 * Copyright 2020 UT OVERSEAS INC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use crate::concurrent::atomic_buffer::AtomicBuffer;
use crate::concurrent::logbuffer::{data_frame_header, log_buffer_descriptor};
use crate::concurrent::logbuffer::header::Header;
use crate::context;
use crate::utils::errors::{AeronError, IllegalArgumentError, IllegalStateError};
use crate::utils::misc::{alloc_buffer_aligned, dealloc_buffer_aligned};
use crate::utils::types::Index;

const BUFFER_BUILDER_MAX_CAPACITY: Index = std::i32::MAX as Index - 8;
const BUFFER_BUILDER_INIT_MIN_CAPACITY: Index = 4096;

/// This type must not impl Copy! Only move semantics is allowed.
/// BufferBuilder owns memory (allocates / deallocates it)
///
#[allow(dead_code)]
pub struct BufferBuilder {
    capacity: Index,
    limit: Index,
    next_term_offset :i32,
    buffer: *mut u8,
    header_buffer:  *mut u8,
    header: Header,
}

impl Drop for BufferBuilder {
    fn drop(&mut self) {
        // Free the memory we own
        dealloc_buffer_aligned(self.buffer, self.capacity)
    }
}

impl BufferBuilder {
    pub fn new(initial_capacity: Index) -> Self {
        //let len = bit_utils::find_next_power_of_two_i64(initial_length as i64) as Index;

        let header_buffer = alloc_buffer_aligned(data_frame_header::LENGTH);
        let header_atm_buffer = AtomicBuffer::new(header_buffer, data_frame_header::LENGTH);
        let mut header = Header::new(0, 0);
        header.set_buffer(header_atm_buffer);

        Self {
            capacity: initial_capacity,
            limit: 0,
            next_term_offset: context::NULL_VALUE,
            buffer: alloc_buffer_aligned(initial_capacity),
            header_buffer,
            header,
        }
    }

    pub fn buffer(&self) -> *mut u8 {
        self.buffer
    }

    pub fn limit(&self) -> Index {
        self.limit
    }

    pub fn set_limit(&mut self, limit: Index) -> Result<(), AeronError> {
        if limit >= self.capacity {
            return Err(IllegalArgumentError::LimitOutsideRange {
                capacity: self.capacity,
                limit,
            }
            .into());
        }

        self.limit = limit;

        Ok(())
    }

    pub fn capacity(&self) -> Index {
        self.capacity
    }

    pub fn next_term_offset(&self) -> i32 {
        self.next_term_offset
    }

    pub fn set_next_term_offset(&mut self, next_term_offset: i32) {
        self.next_term_offset = next_term_offset;
    }

    pub fn reset(&mut self) -> &mut BufferBuilder {
        self.limit = 0;
        self.next_term_offset = context::NULL_VALUE;
        self.header.set_fragmented_frame_length(context::NULL_VALUE);
        self
    }

    pub fn compact(&mut self)-> &mut BufferBuilder{
        let new_capacity = if BUFFER_BUILDER_INIT_MIN_CAPACITY < self.limit {
            self.limit
        } else {
            BUFFER_BUILDER_INIT_MIN_CAPACITY
        };

        if new_capacity < self.capacity {
            self.resize(new_capacity);
        }

        self
    }

    pub fn append(
        &mut self,
        buffer: &AtomicBuffer,
        offset: Index,
        length: Index,
    ) -> Result<&BufferBuilder, AeronError> {
        self.ensure_capacity(length)?;

        unsafe {
            std::ptr::copy(
                buffer.buffer().offset(offset as isize),
                self.buffer.offset(self.limit as isize),
                length as usize,
            );
        }

        self.limit += length;

        Ok(self)
    }

    pub fn capture_header(&mut self, header: &Header) -> &mut Self {
        self.header.copy_from(header);
        self
    }

    pub fn set_complete_header(&mut self, header: &Header) -> &mut Header {
        let first_frame_length = self.header.frame_length();
        let fragmented_frame_length = log_buffer_descriptor::compute_fragmented_frame_length(
            self.limit, first_frame_length - data_frame_header::LENGTH);
        self.header.set_fragmented_frame_length(fragmented_frame_length as i32);

        self.header.buffer()
            .put::<i32>(*data_frame_header::FRAME_LENGTH_FIELD_OFFSET + self.header.offset(), data_frame_header::LENGTH + self.limit);
        self.header.buffer()
            .put::<u8>(*data_frame_header::FLAGS_FIELD_OFFSET + self.header.offset(), self.header.flags() | header.flags());

        &mut self.header
    }

    pub fn complete_header(&self) -> &Header {
        &self.header
    }

    fn find_suitable_capacity(current_capacity: Index, required_capacity: Index) -> Result<Index, AeronError> {
        let mut capacity = current_capacity;

        loop {
            let new_capacity = capacity + (capacity >> 1);

            if new_capacity < capacity || new_capacity > BUFFER_BUILDER_MAX_CAPACITY {
                if capacity == BUFFER_BUILDER_MAX_CAPACITY {
                    return Err(IllegalStateError::MaxCapacityReached(BUFFER_BUILDER_MAX_CAPACITY).into());
                }

                capacity = BUFFER_BUILDER_MAX_CAPACITY;
            } else {
                capacity = new_capacity;
            }

            if capacity >= required_capacity {
                break;
            }
        }

        Ok(capacity)
    }

    /// This fn resizes (if needed) the buffer keeping all the data in it.
    fn ensure_capacity(&mut self, additional_capacity: Index) -> Result<(), AeronError> {
        let required_capacity = self.limit + additional_capacity;

        if required_capacity > self.capacity {
            let new_capacity = BufferBuilder::find_suitable_capacity(self.capacity, required_capacity)?;
            self.resize(new_capacity);
        }
        Ok(())
    }

    fn resize(&mut self, new_capacity: Index){
        let new_buffer = alloc_buffer_aligned(new_capacity);

        unsafe {
            std::ptr::copy(self.buffer, new_buffer, self.limit as usize);
            dealloc_buffer_aligned(self.buffer, self.capacity)
        }

        self.buffer = new_buffer;
        self.capacity = new_capacity;
    }
}
