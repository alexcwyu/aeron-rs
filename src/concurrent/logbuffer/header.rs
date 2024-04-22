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
use crate::concurrent::logbuffer::{frame_descriptor, log_buffer_descriptor};
use crate::concurrent::logbuffer::data_frame_header::{self, DataFrameHeaderDefn};
use crate::context;
use crate::utils::bit_utils::align;
use crate::utils::misc::alloc_buffer_aligned;
use crate::utils::types::Index;

/**
 * Represents the header of the data frame for accessing meta data fields.
 */
// TODO: originally there was pointer *void on context. Need to check all types
// which could be stored in this field and make it rusty.
#[derive(Clone)]
pub struct Header {
    //context: Option<Arc<Image>>,
    buffer: Option<AtomicBuffer>,
    offset: Index,
    initial_term_id: i32,
    position_bits_to_shift: i32,
    fragmented_frame_length: i32,
}

impl Header {
    pub fn new(initial_term_id: i32, position_bits_to_shift: i32) -> Self {
        Self {
            //context: None,
            initial_term_id,
            offset: 0,
            position_bits_to_shift,
            buffer: None,
            fragmented_frame_length: -1,
        }
    }

    #[inline]
    pub fn copy_from(&mut self, src: &Header) {
        self.initial_term_id = src.initial_term_id;
        self.position_bits_to_shift = src.position_bits_to_shift;

        if src.buffer.is_some() {
            let buffer = alloc_buffer_aligned(data_frame_header::LENGTH);
            let atomic_buffer = AtomicBuffer::new(buffer, data_frame_header::LENGTH);
            unsafe {
                std::ptr::copy(
                    src.buffer.unwrap().at(src.offset),
                    atomic_buffer.at(self.offset),
                    data_frame_header::LENGTH as usize,
                );
            }
            self.buffer = Some(atomic_buffer);
        }
        else{
            self.buffer = None;
        }
    }

    #[inline]
    pub fn fragmented_frame_length(&self) -> i32 {
        self.fragmented_frame_length
    }

    #[inline]
    pub fn set_fragmented_frame_length(&mut self, length: i32) {
        self.fragmented_frame_length = length;
    }

    /**
     * Get the initial term id this stream started at.
     *
     * @return the initial term id this stream started at.
     */
    #[inline]
    pub fn initial_term_id(&self) -> i32 {
        self.initial_term_id
    }

    #[inline]
    pub fn set_initial_term_id(&mut self, initial_term_id: i32) {
        self.initial_term_id = initial_term_id;
    }

    /**
     * The offset at which the frame begins.
     *
     * @return offset at which the frame begins.
     */
    #[inline]
    pub fn offset(&self) -> Index {
        self.offset
    }

    #[inline]
    pub fn set_offset(&mut self, offset: Index) {
        self.offset = offset;
    }

    /**
     * The AtomicBuffer containing the header.
     *
     * @return AtomicBuffer containing the header.
     */
    #[inline]
    pub fn buffer(&self) -> AtomicBuffer {
        self.buffer.expect("Buffer not set")
    }

    // Header owns the buffer. But buffer doesn't own memory it points to.
    #[inline]
    pub fn set_buffer(&mut self, buffer: AtomicBuffer) {
        self.buffer = Some(buffer);
    }

    /**
     * The total length of the frame including the header.
     *
     * @return the total length of the frame including the header.
     */
    #[inline]
    pub fn frame_length(&self) -> Index {
        self.buffer.expect("Buffer not set").get::<i32>(self.offset) as Index
    }

    /**
     * The session ID to which the frame belongs.
     *
     * @return the session ID to which the frame belongs.
     */
    #[inline]
    pub fn session_id(&self) -> i32 {
        self.buffer
            .expect("Buffer not set")
            .get::<i32>(self.offset + *data_frame_header::SESSION_ID_FIELD_OFFSET)
    }

    /**
     * The stream ID to which the frame belongs.
     *
     * @return the stream ID to which the frame belongs.
     */
    #[inline]
    pub fn stream_id(&self) -> i32 {
        self.buffer
            .expect("Buffer not set")
            .get::<i32>(self.offset + *data_frame_header::STREAM_ID_FIELD_OFFSET)
    }

    /**
     * The term ID to which the frame belongs.
     *
     * @return the term ID to which the frame belongs.
     */
    #[inline]
    pub fn term_id(&self) -> i32 {
        self.buffer
            .expect("Buffer not set")
            .get::<i32>(self.offset + *data_frame_header::TERM_ID_FIELD_OFFSET)
    }

    /**
     * The offset in the term at which the frame begins. This will be the same as {@link #offset()}
     *
     * @return the offset in the term at which the frame begins.
     */
    #[inline]
    pub fn term_offset(&self) -> Index {
        self.buffer
            .expect("Buffer not set")
            .get::<i32>(self.offset + *data_frame_header::TERM_OFFSET_FIELD_OFFSET)
    }

    /**
     * Calculates the offset of the frame immediately after this one.
     *
     * @return the offset of the next frame.
     */
    #[inline]
    pub fn next_term_offset(&self) -> Index {
        align(self.term_offset() + self.term_occupancy_length(), frame_descriptor::FRAME_ALIGNMENT)
    }

    #[inline]
    fn term_occupancy_length(&self) -> Index{
        if self.fragmented_frame_length == context::NULL_VALUE {
            self.frame_length()
        } else {
            self.fragmented_frame_length
        }
    }


    /**
     * The type of the the frame which should always be {@link DataFrameHeader::HDR_TYPE_DATA}
     *
     * @return type of the the frame which should always be {@link DataFrameHeader::HDR_TYPE_DATA}
     */
    #[inline]
    pub fn frame_type(&self) -> u16 {
        self.buffer
            .expect("Buffer not set")
            .get::<u16>(self.offset + *data_frame_header::TYPE_FIELD_OFFSET)
    }

    /**
     * The flags for this frame. Valid flags are {@link DataFrameHeader::BEGIN_FLAG}
     * and {@link DataFrameHeader::END_FLAG}. A convenience flag {@link DataFrameHeader::BEGIN_AND_END_FLAGS}
     * can be used for both flags.
     *
     * @return the flags for this frame.
     */
    #[inline]
    pub fn flags(&self) -> u8 {
        self.buffer
            .expect("Buffer not set")
            .get::<u8>(self.offset + *data_frame_header::FLAGS_FIELD_OFFSET)
    }

    /**
     * Get the current position to which the Image has advanced on reading this message.
     *
     * @return the current position to which the Image has advanced on reading this message.
     */
    #[inline]
    pub fn position(&self) -> i64 {
        log_buffer_descriptor::compute_position(
            self.term_id(),
            self.next_term_offset(),
            self.position_bits_to_shift,
            self.initial_term_id,
        )
    }

    /**
     * The number of times to left shift the term count to multiply by term length.
     *
     * @return number of times to left shift the term count to multiply by term length.
     */
    #[inline]
    pub fn position_bits_to_shift(&self) -> i32 {
        self.position_bits_to_shift
    }

    /**
     * Get the value stored in the reserve space at the end of a data frame header.
     *
     * @return the value stored in the reserve space at the end of a data frame header.
     */
    pub fn reserved_value(&self) -> i64 {
        self.buffer
            .expect("Buffer not set")
            .get::<i64>(self.offset + *data_frame_header::RESERVED_VALUE_FIELD_OFFSET)
    }
}

pub struct HeaderWriter {
    session_id: i32,
    stream_id: i32,
}

impl HeaderWriter {
    pub fn new(default_hdr: AtomicBuffer) -> Self {
        Self {
            session_id: default_hdr.get::<i32>(*data_frame_header::SESSION_ID_FIELD_OFFSET),
            stream_id: default_hdr.get::<i32>(*data_frame_header::STREAM_ID_FIELD_OFFSET),
        }
    }

    /**
     * Write header in LITTLE_ENDIAN order
     */
    pub fn write(&self, term_buffer: &AtomicBuffer, offset: Index, length: Index, term_id: i32) {
        term_buffer.put_ordered::<i32>(offset, -(length));

        unsafe {
            let hdr = term_buffer.overlay_struct::<DataFrameHeaderDefn>(offset);

            (*hdr).version = data_frame_header::CURRENT_VERSION;
            (*hdr).flags = frame_descriptor::BEGIN_FRAG | frame_descriptor::END_FRAG;
            (*hdr).frame_type = data_frame_header::HDR_TYPE_DATA;
            (*hdr).term_offset = offset;
            (*hdr).session_id = self.session_id;
            (*hdr).stream_id = self.stream_id;
            (*hdr).term_id = term_id;
        }
    }
}
