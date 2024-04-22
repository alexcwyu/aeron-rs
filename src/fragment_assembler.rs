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

use std::collections::HashMap;

use crate::buffer_builder::BufferBuilder;
use crate::concurrent::atomic_buffer::AtomicBuffer;
use crate::concurrent::logbuffer::frame_descriptor;
use crate::concurrent::logbuffer::header::Header;
use crate::utils::types::Index;

const DEFAULT_FRAGMENT_ASSEMBLY_BUFFER_LENGTH: isize = 4096;

pub trait Fragment: FnMut(&AtomicBuffer, Index, Index, &Header) {}

impl<T: FnMut(&AtomicBuffer, Index, Index, &Header)> Fragment for T {}

/**
 * A handler that sits in a chain-of-responsibility pattern that reassembles fragmented messages
 * so that the next handler in the chain only sees whole messages.
 * <p>
 * Unfragmented messages are delegated without copy. Fragmented messages are copied to a temporary
 * buffer for reassembly before delegation.
 * <p>
 * The Header passed to the delegate on assembling a message will be that of the last fragment.
 * <p>
 * Session based buffers will be allocated and grown as necessary based on the length of messages to be assembled.
 * When sessions go inactive see {@link on_unavailable_image_t}, it is possible to free the buffer by calling
 * {@link #deleteSessionBuffer(std::int32_t)}.
 */
pub struct FragmentAssembler<'a> {
    delegate: &'a mut dyn Fragment<Output=()>,
    builder_by_session_id_map: HashMap<i32, BufferBuilder>,
    initial_buffer_length: isize,
}

impl<'a> FragmentAssembler<'a> {
    /**
     * Construct an adapter to reassembly message fragments and delegate on only whole messages.
     *
     * @param delegate            onto which whole messages are forwarded.
     * @param initialBufferLength to be used for each session.
     */
    pub fn new(delegate: &'a mut impl Fragment, initial_buffer_length: Option<isize>) -> Self {
        Self {
            delegate,
            builder_by_session_id_map: HashMap::new(),
            initial_buffer_length: initial_buffer_length.unwrap_or(DEFAULT_FRAGMENT_ASSEMBLY_BUFFER_LENGTH),
        }
    }

    /**
     * Compose a fragment_handler_t that calls the this FragmentAssembler instance for reassembly. Suitable for
     * passing to Subscription::poll(fragment_handler_t, int).
     *
     * @return fragment_handler_t composed with the FragmentAssembler instance
     */
    pub fn handler(&'a mut self) -> impl Fragment + 'a {
        move |buffer: &AtomicBuffer, offset, length, header: &Header| self.on_fragment(buffer, offset, length, header)
    }

    /**
     * Free an existing session buffer to reduce memory pressure when an Image goes inactive or no more
     * large messages are expected.
     *
     * @param sessionId to have its buffer freed
     */
    pub fn delete_session_buffer(&mut self, session_id: i32) {
        self.builder_by_session_id_map.remove(&session_id);
    }

    #[inline]
    fn on_fragment(&mut self, buffer: &AtomicBuffer, offset: Index, length: Index, header: &Header) {
        let flags = header.flags();
        if (flags & frame_descriptor::UNFRAGMENTED) == frame_descriptor::UNFRAGMENTED {
            (self.delegate)(buffer, offset, length, header);
        } else {
            if (flags & frame_descriptor::BEGIN_FRAG) == frame_descriptor::BEGIN_FRAG {
                // Here we need following logic: if BufferBuilder for given session_id do exist in the map - use it.
                // If there is no such BufferBuilder then create on, insert in to map and use it.
                let initial_buffer_length = self.initial_buffer_length;
                let builder = self
                    .builder_by_session_id_map
                    .entry(header.session_id())
                    .or_insert_with(|| BufferBuilder::new(initial_buffer_length as Index));

                builder.reset().capture_header(&header).append(buffer, offset, length).expect("append failed");
                builder.set_next_term_offset(header.next_term_offset());
            } else if let Some(builder) = self.builder_by_session_id_map.get_mut(&header.session_id()) {
                if header.term_offset() == builder.next_term_offset() {
                    builder.append(buffer, offset, length).expect("append failed");

                    if flags & frame_descriptor::END_FRAG == frame_descriptor::END_FRAG {
                        let msg_buffer = AtomicBuffer::new(builder.buffer(), builder.limit());
                        (*self.delegate)(&msg_buffer, 0, builder.limit(), builder.set_complete_header(&header));

                        builder.reset();
                    } else {
                        builder.set_next_term_offset(header.next_term_offset());
                    }
                } else {
                    builder.reset();
                }
            }
        }
    }

}

#[cfg(test)]
mod test {
    use std::sync::atomic::{AtomicBool, Ordering};

    use lazy_static::lazy_static;

    use crate::concurrent::atomic_buffer::{AlignedBuffer, AtomicBuffer};
    use crate::concurrent::logbuffer::{frame_descriptor, log_buffer_descriptor};
    use crate::concurrent::logbuffer::data_frame_header::{self, DataFrameHeaderDefn};
    use crate::concurrent::logbuffer::header::Header;
    use crate::fragment_assembler::FragmentAssembler;
    use crate::utils::bit_utils;
    use crate::utils::types::Index;

    // const CHANNEL: &str = "aeron:udp?endpoint=localhost:40123";
    const STREAM_ID: i32 = 10;
    const SESSION_ID: i32 = 200;
    const TERM_OFFSET: i32 = 1024;
    const TERM_LENGTH: i32 = log_buffer_descriptor::TERM_MIN_LENGTH;
    const INITIAL_TERM_ID: i32 = -1234;
    const ACTIVE_TERM_ID: i32 = INITIAL_TERM_ID + 5;

    const MTU_LENGTH: Index = 128;

    lazy_static! {
        pub static ref POSITION_BITS_TO_SHIFT: i32 = bit_utils::number_of_trailing_zeroes(TERM_LENGTH);
        pub static ref CALLED: AtomicBool = AtomicBool::new(false);
    }

    #[allow(dead_code)]
    struct FragmentAssemblerTest {
        fragment: AlignedBuffer,
        buffer: AtomicBuffer,
        header_fragment: AlignedBuffer,
        header_buffer: AtomicBuffer,
        header: Header,
    }

    impl FragmentAssemblerTest {
        pub fn new() -> Self {
            let fragment = AlignedBuffer::with_capacity(TERM_LENGTH);
            let buffer = AtomicBuffer::from_aligned(&fragment);

            let header_fragment = AlignedBuffer::with_capacity(data_frame_header::LENGTH);
            let header_buffer = AtomicBuffer::from_aligned(&header_fragment);
            let mut header = Header::new(INITIAL_TERM_ID, *POSITION_BITS_TO_SHIFT);
            header.set_buffer(header_buffer);
            Self {
                fragment,
                buffer,
                header_fragment,
                header_buffer,
                header,
            }
        }

        fn fill_frame(&self, term_offset: i32, flags: u8, offset: i32, length: i32, payload_value: u8) {
            let frame = self.header.buffer().overlay_struct::<DataFrameHeaderDefn>(0);
            unsafe {
                (*frame).frame_length = data_frame_header::LENGTH + length;
                (*frame).version = data_frame_header::CURRENT_VERSION;
                (*frame).flags = flags;
                (*frame).frame_type = data_frame_header::HDR_TYPE_DATA;
                (*frame).term_offset = term_offset;
                (*frame).session_id = SESSION_ID;
                (*frame).stream_id = STREAM_ID;
                (*frame).term_id = ACTIVE_TERM_ID;
            }

            let mut value = payload_value;
            for i in 0..length {
                self.buffer.put(i + offset, value);
                value = if value == u8::MAX { 0 } else { value + 1 };
            }
        }

        // Fragment_len must contain length on i-th fragment.
        // Each byte of each fragment was previously filled with the fragments seq number.
        fn verify_payload(buffer: &AtomicBuffer, offset: Index, fragment_len: &[Index]) {
            unsafe {
                let ptr = buffer.buffer().offset(offset as isize);

                let mut fragment_offset = 0;
                for (i, len) in fragment_len.iter().enumerate() {
                    for j in fragment_offset..fragment_offset + *len {
                        //println!("i ={}, len={}, j={}, fragment_offset={}, left={}, right={}", i, len, j, fragment_offset, *(ptr.offset(j as isize)), (j % 256) as u8);
                        assert_eq!(*(ptr.offset(j as isize)), (j % 256) as u8);
                    }
                    fragment_offset += *len;
                }
            }
        }

        fn verify_payload2(buffer: &AtomicBuffer, offset: Index, length: Index) {
            unsafe {
                let ptr = buffer.buffer().offset(offset as isize);

                for i in 0..length {
                    //println!("i ={}, length={}, left={}, right={}", i, length, *(ptr.offset(i as isize)), (i % 256) as u8);
                    assert_eq!(*(ptr.offset(i as isize)), (i % 256) as u8);
                }
            }
        }
    }

    #[test]
    fn should_pass_through_unfragmented_message() {
        let test = FragmentAssemblerTest::new();
        let fragment_length = 158;
        let flags = frame_descriptor::UNFRAGMENTED;
        test.fill_frame(TERM_OFFSET, frame_descriptor::UNFRAGMENTED, 0, fragment_length, 0);
        static CALLED: AtomicBool = AtomicBool::new(false);

        let mut fragment = move |buffer: &AtomicBuffer, offset: Index, length: Index, header: &Header| {
            CALLED.store(true, Ordering::Relaxed);
            assert_eq!(offset, 0);
            assert_eq!(length, fragment_length);
            assert_eq!(header.position_bits_to_shift(), *POSITION_BITS_TO_SHIFT);
            assert_eq!(header.initial_term_id(), INITIAL_TERM_ID);
            assert_eq!(header.session_id(), SESSION_ID);
            assert_eq!(header.stream_id(), STREAM_ID);
            assert_eq!(header.term_id(), ACTIVE_TERM_ID);
            assert_eq!(header.term_offset(), TERM_OFFSET);
            assert_eq!(header.frame_length(), data_frame_header::LENGTH + fragment_length);
            assert_eq!(header.flags(), frame_descriptor::UNFRAGMENTED);
            assert_eq!(
                header.position(),
                log_buffer_descriptor::compute_position(
                    ACTIVE_TERM_ID,
                    bit_utils::align(
                        header.term_offset() + header.frame_length(),
                        frame_descriptor::FRAME_ALIGNMENT
                    ),
                    *POSITION_BITS_TO_SHIFT,
                    INITIAL_TERM_ID
                )
            );
            FragmentAssemblerTest::verify_payload2(buffer, offset, length);
        };

        let mut adapter = FragmentAssembler::new(&mut fragment, None);

        adapter.handler()(&test.buffer, 0, fragment_length, &test.header);
        assert!(CALLED.load(Ordering::Relaxed));
    }

    #[test]
    fn should_reassemble_from_two_fragments() {
        let mut test = FragmentAssemblerTest::new();
        let fragment_length = MTU_LENGTH - data_frame_header::LENGTH;
        static CALLED: AtomicBool = AtomicBool::new(false);

        let mut fragment = move |buffer: &AtomicBuffer, offset: Index, length: Index, header: &Header| {
            CALLED.store(true, Ordering::Relaxed);
            assert_eq!(offset, 0);
            assert_eq!(length, fragment_length * 2);
            assert_eq!(header.session_id(), SESSION_ID);
            assert_eq!(header.stream_id(), STREAM_ID);
            assert_eq!(header.term_id(), ACTIVE_TERM_ID);
            assert_eq!(header.initial_term_id(), INITIAL_TERM_ID);
            assert_eq!(header.term_offset(), TERM_OFFSET);
            assert_eq!(header.frame_length(), data_frame_header::LENGTH + (2*fragment_length));
            assert_eq!(header.flags(), frame_descriptor::BEGIN_FRAG | frame_descriptor::END_FRAG);
            assert_eq!(
                header.position(),
                log_buffer_descriptor::compute_position(
                    ACTIVE_TERM_ID,
                    header.term_offset() + log_buffer_descriptor::compute_fragmented_frame_length(
                        2 * fragment_length, fragment_length),
                    *POSITION_BITS_TO_SHIFT,
                    INITIAL_TERM_ID
                )
            );

            FragmentAssemblerTest::verify_payload2(buffer, offset, length);
        };

        let mut adapter = FragmentAssembler::new(&mut fragment, None);
        let mut handler = adapter.handler();
        test.fill_frame(TERM_OFFSET, frame_descriptor::BEGIN_FRAG, 0, fragment_length, 0);
        handler(&test.buffer, 0, fragment_length, &test.header);
        assert!(!CALLED.load(Ordering::Relaxed));

        test.fill_frame(TERM_OFFSET + MTU_LENGTH, frame_descriptor::END_FRAG, MTU_LENGTH, fragment_length, (fragment_length % 256) as u8);
        handler(&test.buffer, MTU_LENGTH, fragment_length, &test.header);
        assert!(CALLED.load(Ordering::Relaxed));
    }

    #[test]
    fn should_reassemble_from_three_fragments() {
        let mut test = FragmentAssemblerTest::new();
        let fragment_length = MTU_LENGTH - data_frame_header::LENGTH;
        static CALLED: AtomicBool = AtomicBool::new(false);

        let mut fragment = move |buffer: &AtomicBuffer, offset: Index, length: Index, header: &Header| {
            CALLED.store(true, Ordering::Relaxed);
            assert_eq!(offset, 0);
            assert_eq!(length, fragment_length * 3);
            assert_eq!(header.session_id(), SESSION_ID);
            assert_eq!(header.stream_id(), STREAM_ID);
            assert_eq!(header.term_id(), ACTIVE_TERM_ID);
            assert_eq!(header.initial_term_id(), INITIAL_TERM_ID);
            assert_eq!(header.term_offset(), TERM_OFFSET);
            assert_eq!(header.frame_length(), data_frame_header::LENGTH + (3 * fragment_length));
            assert_eq!(header.flags(), frame_descriptor::BEGIN_FRAG |frame_descriptor::END_FRAG);
            assert_eq!(
                header.position(),
                log_buffer_descriptor::compute_position(
                    ACTIVE_TERM_ID,
                    header.term_offset() + log_buffer_descriptor::compute_fragmented_frame_length(
                        3 * fragment_length, fragment_length),
                    *POSITION_BITS_TO_SHIFT,
                    INITIAL_TERM_ID
                )
            );
            FragmentAssemblerTest::verify_payload2(buffer, offset, length);
        };

        let mut adapter = FragmentAssembler::new(&mut fragment, None);
        let mut handler = adapter.handler();

        test.fill_frame(TERM_OFFSET, frame_descriptor::BEGIN_FRAG, 0, fragment_length, 0);
        handler(&test.buffer, 0, fragment_length, &test.header);
        assert!(!CALLED.load(Ordering::Relaxed));

        test.fill_frame(TERM_OFFSET+MTU_LENGTH, 0, MTU_LENGTH, fragment_length, (fragment_length % 256) as u8);
        handler(&test.buffer, MTU_LENGTH, fragment_length, &test.header);
        assert!(!CALLED.load(Ordering::Relaxed));

        test.fill_frame(TERM_OFFSET+(MTU_LENGTH*2),frame_descriptor::END_FRAG, MTU_LENGTH * 2, fragment_length, ((fragment_length * 2) % 256) as u8);
        handler(
            &test.buffer,
            MTU_LENGTH * 2 ,
            fragment_length,
            &test.header,
        );
        assert!(CALLED.load(Ordering::Relaxed));
    }

    #[test]
    fn should_not_reassemble_if_end_first_fragment() {
        let mut test = FragmentAssemblerTest::new();
        let fragment_length = MTU_LENGTH - data_frame_header::LENGTH;
        static CALLED: AtomicBool = AtomicBool::new(false);

        let mut fragment = move |_buffer: &AtomicBuffer, _offset: Index, _length: Index, _header: &Header| {
            CALLED.store(true, Ordering::Relaxed);
        };

        let mut adapter = FragmentAssembler::new(&mut fragment, None);

        // test.fill_frame(TERM_OFFSET+MTU_LENGTH, frame_descriptor::END_FRAG, MTU_LENGTH, fragment_length, (fragment_length % 256) as u8);
        // adapter.handler()(&test.buffer, MTU_LENGTH, fragment_length, &test.header);
        test.fill_frame(TERM_OFFSET, frame_descriptor::END_FRAG, MTU_LENGTH, fragment_length, (fragment_length % 256) as u8);
        adapter.handler()(&test.buffer, MTU_LENGTH + data_frame_header::LENGTH, fragment_length, &test.header);
        assert!(!CALLED.load(Ordering::Relaxed));
    }

    #[test]
    fn should_not_reassemble_if_missing_begin() {
        let mut test = FragmentAssemblerTest::new();
        let fragment_length = MTU_LENGTH - data_frame_header::LENGTH;
        static CALLED: AtomicBool = AtomicBool::new(false);

        let mut fragment =
            move |_buffer: &AtomicBuffer, _offset: Index, _length: Index, _header: &Header| CALLED.store(true, Ordering::Relaxed);

        let mut adapter = FragmentAssembler::new(&mut fragment, None);
        let mut handler = adapter.handler();

        test.fill_frame(TERM_OFFSET, 0, MTU_LENGTH, fragment_length, (fragment_length % 256) as u8);
        handler(&test.buffer, MTU_LENGTH, fragment_length, &test.header);
        assert!(!CALLED.load(Ordering::Relaxed));

        test.fill_frame(TERM_OFFSET + MTU_LENGTH, frame_descriptor::END_FRAG, MTU_LENGTH * 2, fragment_length, ((fragment_length * 2) % 256) as u8);
        handler(&test.buffer, MTU_LENGTH * 2 , fragment_length, &test.header);
        assert!(!CALLED.load(Ordering::Relaxed));
    }
}
