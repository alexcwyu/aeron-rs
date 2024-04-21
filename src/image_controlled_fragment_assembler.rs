use crate::buffer_builder::BufferBuilder;
use crate::concurrent::atomic_buffer::AtomicBuffer;
use crate::concurrent::logbuffer::frame_descriptor;
use crate::concurrent::logbuffer::header::Header;
use crate::image::ControlledPollAction;
use crate::utils::types::Index;

const DEFAULT_IMAGE_CONTROLLED_FRAGMENT_ASSEMBLY_BUFFER_LENGTH: isize = 4096;

pub trait ImageControlledFragment: FnMut(&AtomicBuffer, Index, Index, &Header) -> ControlledPollAction {}

impl<T: FnMut(&AtomicBuffer, Index, Index, &Header)-> ControlledPollAction> ImageControlledFragment for T {}

/**
 * A handler that sits in a chain-of-responsibility pattern that reassembles fragmented messages
 * so that the next handler in the chain only sees whole messages.
 * <p>
 * Unfragmented messages are delegated without copy. Fragmented messages are copied to a temporary
 * buffer for reassembly before delegation.
 * <p>
 * The Header passed to the delegate on assembling a message will be that of the last fragment.
 * <p>
 * This handler is not session aware and must only be used when polling a single Image.
 */
pub struct ImageControlledFragmentAssembler<'a> {
    delegate: &'a mut dyn ImageControlledFragment<Output=ControlledPollAction>,
    builder: BufferBuilder,
}

impl<'a> ImageControlledFragmentAssembler<'a> {
    /**
     * Construct an adapter to reassembly message fragments and delegate on only whole messages.
     *
     * @param delegate            onto which whole messages are forwarded.
     * @param initialBufferLength to be used for rebuilding.
     */
    pub fn new(delegate: &'a mut impl ImageControlledFragment, initial_buffer_length: Option<isize>) -> Self {
        Self {
            delegate,
            builder: BufferBuilder::new(initial_buffer_length.unwrap_or(DEFAULT_IMAGE_CONTROLLED_FRAGMENT_ASSEMBLY_BUFFER_LENGTH) as Index),
        }
    }

    /**
     * Compose a controlled_poll_fragment_handler_t that calls the ImageControlledFragmentAssembler instance for
     * reassembly. Suitable for passing to Image::controlledPoll(controlled_poll_fragment_handler_t, int).
     *
     * @return controlled_poll_fragment_handler_t composed with the ImageControlledFragmentAssembler instance
     */
    pub fn handler(&'a mut self) -> impl ImageControlledFragment + 'a {
        move |buffer: &AtomicBuffer, offset, length, header: &Header| self.on_fragment(buffer, offset, length, header)
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
                self.builder.reset().capture_header(&header).append(buffer, offset, length).expect("append failed");
                self.builder.set_next_term_offset(header.term_offset());
            } else if header.term_offset() == self.builder.next_term_offset() {
                let limit = self.builder.limit();
                self.builder.append(buffer, offset, length).expect("append failed");

                if flags & frame_descriptor::END_FRAG == frame_descriptor::END_FRAG {
                    let msg_buffer = AtomicBuffer::new(self.builder.buffer(), self.builder.limit());
                    action = (*self.delegate)(&msg_buffer, 0, self.builder.limit(), self.builder.set_complete_header(&header));

                    if action == ControlledPollAction::ABORT {
                        let _ = self.builder.set_limit(limit);
                    } else {
                        self.builder.reset();
                    }
                } else {
                    self.builder.set_next_term_offset(header.term_offset());
                }
            } else {
                self.builder.reset();
            }
        }
        action
    }

}