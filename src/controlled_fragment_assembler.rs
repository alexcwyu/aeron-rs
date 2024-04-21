use std::collections::HashMap;

use crate::buffer_builder::BufferBuilder;
use crate::concurrent::atomic_buffer::AtomicBuffer;
use crate::concurrent::logbuffer::frame_descriptor;
use crate::concurrent::logbuffer::header::Header;
use crate::image::ControlledPollAction;
use crate::utils::types::Index;

const DEFAULT_CONTROLLED_FRAGMENT_ASSEMBLY_BUFFER_LENGTH: isize = 4096;

pub trait ControlledFragment: FnMut(&AtomicBuffer, Index, Index, &Header) -> ControlledPollAction {}

impl<T: FnMut(&AtomicBuffer, Index, Index, &Header) -> ControlledPollAction> ControlledFragment for T {}

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
pub struct ControlledFragmentAssembler<'a> {
    delegate: &'a mut dyn ControlledFragment<Output=ControlledPollAction>,
    builder_by_session_id_map: HashMap<i32, BufferBuilder>,
    initial_buffer_length: isize,
}

impl<'a> ControlledFragmentAssembler<'a> {
    /**
     * Construct an adapter to reassembly message fragments and delegate on only whole messages.
     *
     * @param delegate            onto which whole messages are forwarded.
     * @param initialBufferLength to be used for each session.
     */
    pub fn new(delegate: &'a mut impl ControlledFragment, initial_buffer_length: Option<isize>) -> Self {
        Self {
            delegate,
            builder_by_session_id_map: HashMap::new(),
            initial_buffer_length: initial_buffer_length.unwrap_or(DEFAULT_CONTROLLED_FRAGMENT_ASSEMBLY_BUFFER_LENGTH),
        }
    }

    /**
     * Compose a controlled_poll_fragment_handler_t that calls the this ControlledFragmentAssembler instance for
     * reassembly. Suitable for passing to Subscription::controlledPoll(controlled_poll_fragment_handler_t, int).
     *
     * @return controlled_poll_fragment_handler_t composed with the ControlledFragmentAssembler instance
     */
    pub fn handler(&'a mut self) -> impl ControlledFragment + 'a {
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
    fn on_fragment(&mut self, buffer: &AtomicBuffer, offset: Index, length: Index, header: &Header) -> ControlledPollAction {
        let flags = header.flags();

        let mut action = ControlledPollAction::CONTINUE;
        if (flags & frame_descriptor::UNFRAGMENTED) == frame_descriptor::UNFRAGMENTED {
            action = (self.delegate)(buffer, offset, length, header);
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
                builder.set_next_term_offset(header.term_offset());
            } else if let Some(builder) = self.builder_by_session_id_map.get_mut(&header.session_id()) {
                let limit = builder.limit();
                if header.term_offset() == builder.next_term_offset() {
                    builder.append(buffer, offset, length).expect("append failed");

                    if flags & frame_descriptor::END_FRAG == frame_descriptor::END_FRAG {
                        let msg_buffer = AtomicBuffer::new(builder.buffer(), builder.limit());
                        action = (*self.delegate)(&msg_buffer, 0, builder.limit(), builder.set_complete_header(&header));

                        if action == ControlledPollAction::ABORT {
                            let _ = builder.set_limit(limit);
                        } else {
                            builder.reset();
                        }
                    } else {
                        builder.set_next_term_offset(header.term_offset());
                    }
                } else {
                    builder.reset();
                }
            }
        }
        action
    }
}

