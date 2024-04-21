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

use crate::channel_uri;
use crate::concurrent::logbuffer;
use crate::utils::errors::{AeronError, IllegalArgumentError};

#[derive(Debug)]
struct Value {
    value: i64,
}

impl Value {
    pub fn new(value: i64) -> Self {
        Self { value }
    }

    fn bool_to_string(value: &Self) -> &str {
        if value.value == 1 {
            "true"
        } else {
            "false"
        }
    }
}

#[derive(Default, Debug)]
pub struct ChannelUriStringBuilder {
    prefix: Option<String>,
    media: Option<String>,
    endpoint: Option<String>,
    network_interface: Option<String>,
    control_endpoint: Option<String>,
    control_mode: Option<String>,
    tags: Option<String>,
    alias: Option<String>,
    cc: Option<String>,
    fc: Option<String>,
    gtag: Option<Value>,
    reliable: Option<Value>,
    ttl: Option<Value>,
    mtu: Option<Value>,
    term_length: Option<Value>,
    initial_term_id: Option<Value>,
    term_id: Option<Value>,
    term_offset: Option<Value>,
    session_id: Option<Value>,
    linger: Option<Value>,
    sparse: Option<Value>,
    eos: Option<Value>,
    tether: Option<Value>,
    group: Option<Value>,
    rejoin: Option<Value>,
    ssc : Option<Value>,
    socket_rcvbuf_length: Option<Value>,
    socket_sndbuf_length: Option<Value>,
    receiver_window_length: Option<Value>,
    media_receive_timestamp_offset: Option<String>,
    channel_receive_timestamp_offset: Option<String>,
    channel_send_timestamp_offset: Option<String>,
    response_correlation_id: Option<Value>,
    is_session_id_tagged: bool,



}

impl ChannelUriStringBuilder {
    #[inline]
    pub fn clear(&mut self) {
        self.prefix = None;
        self.media = None;
        self.endpoint = None;
        self.network_interface = None;
        self.control_endpoint = None;
        self.control_mode = None;
        self.tags = None;
        self.alias = None;
        self.cc = None;
        self.fc = None;
        self.gtag = None;
        self.reliable = None;
        self.ttl = None;
        self.mtu = None;
        self.term_length = None;
        self.initial_term_id = None;
        self.term_id = None;
        self.term_offset = None;
        self.session_id = None;
        self.linger = None;
        self.sparse = None;
        self.eos = None;
        self.tether = None;
        self.group = None;
        self.rejoin = None;
        self.ssc = None;
        self.socket_rcvbuf_length = None;
        self.socket_sndbuf_length = None;
        self.receiver_window_length = None;
        self.media_receive_timestamp_offset = None;
        self.channel_receive_timestamp_offset = None;
        self.channel_send_timestamp_offset = None;
        self.response_correlation_id = None;
        self.is_session_id_tagged = false;
    }

    #[inline]
    pub fn prefix(&mut self, new_prefix: Option<&str>) -> Result<&mut Self, AeronError> {
        if new_prefix.is_some() {
            if let Some(prefix) = &self.prefix {
                if !prefix.is_empty() && !prefix.eq(channel_uri::SPY_QUALIFIER) {
                    return Err(IllegalArgumentError::InvalidPrefix(new_prefix.unwrap().to_string()).into());
                }
            }

            self.prefix = Some(String::from(new_prefix.unwrap()));
        }
        else{
            self.prefix = None;
        }
        Ok(self)
    }

    #[inline]
    pub fn media(&mut self, media: &str) -> Result<&mut Self, AeronError> {
        if !media.eq(channel_uri::UDP_MEDIA) && !media.eq(channel_uri::IPC_MEDIA) {
            return Err(IllegalArgumentError::InvalidMedia(media.to_string()).into());
        }

        self.media = Some(String::from(media));
        Ok(self)
    }

    #[inline]
    pub fn endpoint(&mut self, endpoint: &str) -> &mut Self {
        self.endpoint = Some(String::from(endpoint));
        self
    }

    #[inline]
    pub fn network_interface(&mut self, network_interface: &str) -> &mut Self {
        self.network_interface = Some(String::from(network_interface));
        self
    }

    #[inline]
    pub fn control_endpoint(&mut self, control_endpoint: &str) -> &mut Self {
        self.control_endpoint = Some(String::from(control_endpoint));
        self
    }

    #[inline]
    pub fn control_mode(&mut self, control_mode: &str) -> Result<&mut Self, AeronError> {
        if !control_mode.eq(channel_uri::MDC_CONTROL_MODE_MANUAL)
            && !control_mode.eq(channel_uri::MDC_CONTROL_MODE_DYNAMIC)
            && !control_mode.eq(channel_uri::CONTROL_MODE_RESPONSE){
            return Err(IllegalArgumentError::InvalidControlMode(control_mode.to_string()).into());
        }

        self.prefix = Some(String::from(control_mode));
        Ok(self)
    }

    #[inline]
    pub fn tags(&mut self, tags: &str) -> &mut Self {
        self.tags = Some(String::from(tags));
        self
    }

    #[inline]
    pub fn alias(&mut self, alias: &str) -> &mut Self {
        self.alias = Some(String::from(alias));
        self
    }

    #[inline]
    pub fn congestion_control(&mut self, congestrion_control: &str) -> &mut Self {
        self.cc = Some(String::from(congestrion_control));
        self
    }

    #[inline]
    pub fn flow_control(&mut self, flow_control: &str) -> &mut Self {
        self.fc = Some(String::from(flow_control));
        self
    }
    #[inline]
    pub fn group_tag(&mut self, gtag: i64) -> &mut Self {
        self.gtag = Some(Value::new(gtag));
        self
    }

    #[inline]
    pub fn reliable(&mut self, reliable: Option<bool>) -> &mut Self {
        self.reliable = reliable.map(|v| {
            let value = if v { 1 } else { 0 };
            Value::new(value)
        });
        self
    }


    #[inline]
    pub fn ttl(&mut self, ttl: u8) -> &mut Self {
        self.ttl = Some(Value::new(ttl as i64));
        self
    }

    #[inline]
    pub fn mtu(&mut self, mtu: u32) -> Result<&mut Self, AeronError> {
        if !(32..=65504).contains(&mtu) {
            return Err(IllegalArgumentError::MtuIsNotInRange {
                mtu,
                left_bound: 32,
                right_bound: 65504,
            }
            .into());
        }

        if mtu & (logbuffer::frame_descriptor::FRAME_ALIGNMENT - 1) as u32 != 0 {
            return Err(IllegalArgumentError::MtuNotMultipleOfFrameAlignment {
                mtu,
                frame_alignment: logbuffer::frame_descriptor::FRAME_ALIGNMENT,
            }
            .into());
        }

        self.mtu = Some(Value::new(mtu as i64));
        Ok(self)
    }

    #[inline]
    pub fn term_length(&mut self, term_length: i32) -> Result<&mut Self, AeronError> {
        logbuffer::log_buffer_descriptor::check_term_length(term_length)?;
        self.term_length = Some(Value::new(term_length as i64));
        Ok(self)
    }

    #[inline]
    pub fn initial_term_id(&mut self, initial_term_id: i32) -> &mut Self {
        self.initial_term_id = Some(Value::new(initial_term_id as i64));
        self
    }

    #[inline]
    pub fn term_id(&mut self, term_id: i32) -> &mut Self {
        self.term_id = Some(Value::new(term_id as i64));
        self
    }

    #[inline]
    pub fn term_offset(&mut self, term_offset: u32) -> Result<&mut Self, AeronError> {
        if term_offset > logbuffer::log_buffer_descriptor::TERM_MAX_LENGTH as u32 {
            return Err(IllegalArgumentError::TermOffsetNotInRange(term_offset).into());
        }

        if term_offset & (logbuffer::frame_descriptor::FRAME_ALIGNMENT - 1) as u32 != 0 {
            return Err(IllegalArgumentError::TermOffsetNotMultipleOfFrameAlignment {
                term_offset,
                frame_alignment: logbuffer::frame_descriptor::FRAME_ALIGNMENT,
            }
            .into());
        }

        self.term_offset = Some(Value::new(term_offset as i64));
        Ok(self)
    }

    #[inline]
    pub fn session_id(&mut self, session_id: i32) -> &mut Self {
        self.term_id = Some(Value::new(session_id as i64));
        self
    }

    #[inline]
    pub fn linger(&mut self, linger_ns: i64) -> Result<&mut Self, AeronError> {
        if linger_ns < 0 {
            return Err(IllegalArgumentError::LingerValueCannotBeNegative(linger_ns).into());
        }

        self.linger = Some(Value::new(linger_ns));
        Ok(self)
    }

    #[inline]
    pub fn sparse(&mut self, sparse: bool) -> &mut Self {
        let value = if sparse { 1 } else { 0 };
        self.sparse = Some(Value::new(value));
        self
    }

    #[inline]
    pub fn eos(&mut self, eos: bool) -> &mut Self {
        let value = if eos { 1 } else { 0 };
        self.eos = Some(Value::new(value));
        self
    }

    #[inline]
    pub fn tether(&mut self, tether: bool) -> &mut Self {
        let value = if tether { 1 } else { 0 };
        self.term_id = Some(Value::new(value));
        self
    }

    #[inline]
    pub fn group(&mut self, group: bool) -> &mut Self {
        let value = if group { 1 } else { 0 };
        self.term_id = Some(Value::new(value));
        self
    }

    #[inline]
    pub fn rejoin(&mut self, rejoin: Option<bool>) -> &mut Self {
        self.rejoin = rejoin.map(|v| {
            let value = if v { 1 } else { 0 };
            Value::new(value)
        });
        self
    }

    #[inline]
    pub fn spies_simulate_connection(&mut self, ssc: Option<bool>) -> &mut Self {
        self.ssc = ssc.map(|v| {
                let value = if v { 1 } else { 0 };
                Value::new(value)
            });
        self
    }

    #[inline]
    pub fn is_session_tagged(&mut self, is_session_tagged: bool) -> &mut Self {
        self.is_session_id_tagged = is_session_tagged;
        self
    }

    pub fn socket_sndbuf_length(&mut self, socket_sndbuf_length: Option<u32>) -> &mut Self {
        self.socket_sndbuf_length = socket_sndbuf_length.map(|v| Value::new(v as i64));
        self
    }

    pub fn socket_rcvbuf_length(&mut self, socket_rcvbuf_length: Option<u32>) -> &mut Self {
        self.socket_rcvbuf_length = socket_rcvbuf_length.map(|v| Value::new(v as i64));
        self
    }

    pub fn receiver_window_length(&mut self, receiver_window_length: Option<u32>) -> &mut Self {
        self.receiver_window_length = receiver_window_length.map(|v| Value::new(v as i64));
        self
    }

    pub fn media_receive_timestamp_offset(&mut self, media_receive_timestamp_offset: String) -> &mut Self {
        self.media_receive_timestamp_offset = Some(media_receive_timestamp_offset);
        self
    }

    pub fn channel_receive_timestamp_offset(&mut self, receive_timestamp_offset: String) -> &mut Self {
        self.channel_receive_timestamp_offset = Some(receive_timestamp_offset);
        self
    }

    pub fn channel_send_timestamp_offset(&mut self, send_timestamp_offset: String) -> &mut Self {
        self.channel_send_timestamp_offset = Some(send_timestamp_offset);
        self
    }

    pub fn response_correlation_id(&mut self, response_correlation_id: i64) -> &mut Self {
        self.response_correlation_id = Some(Value::new(response_correlation_id));
        self
    }

    pub fn build(&self) -> String {
        let mut sb = String::new();

        if let Some(prefix) = &self.prefix {
            if !prefix.is_empty() {
                sb += &format!("{}:", prefix);
            }
        }

        sb += &format!(
            "{}:{}?",
            channel_uri::AERON_SCHEME,
            self.media.as_ref().expect("Media should be presented")
        );

        if let Some(tags) = &self.tags {
            sb += &format!("{}={}|", channel_uri::TAGS_PARAM_NAME, tags);
        }

        if let Some(endpoint) = &self.endpoint {
            sb += &format!("{}={}|", channel_uri::ENDPOINT_PARAM_NAME, endpoint);
        }

        if let Some(network_interface) = &self.network_interface {
            sb += &format!("{}={}|", channel_uri::INTERFACE_PARAM_NAME, network_interface);
        }

        if let Some(control_endpoint) = &self.control_endpoint {
            sb += &format!("{}={}|", channel_uri::MDC_CONTROL_PARAM_NAME, control_endpoint);
        }

        if let Some(control_mode) = &self.control_mode {
            sb += &format!("{}={}|", channel_uri::MDC_CONTROL_MODE_PARAM_NAME, control_mode);
        }

        if let Some(mtu) = &self.mtu {
            sb += &format!("{}={}|", channel_uri::MTU_LENGTH_PARAM_NAME, mtu.value);
        }

        if let Some(term_length) = &self.term_length {
            sb += &format!("{}={}|", channel_uri::TERM_LENGTH_PARAM_NAME, term_length.value);
        }

        if let Some(initial_term_id) = &self.initial_term_id {
            sb += &format!("{}={}|", channel_uri::INITIAL_TERM_ID_PARAM_NAME, initial_term_id.value);
        }

        if let Some(term_id) = &self.term_id {
            sb += &format!("{}={}|", channel_uri::TERM_ID_PARAM_NAME, term_id.value);
        }

        if let Some(term_offset) = &self.term_offset {
            sb += &format!("{}={}|", channel_uri::TERM_OFFSET_PARAM_NAME, term_offset.value);
        }

        if let Some(session_id) = &self.session_id {
            sb += &format!(
                "{}={}|",
                channel_uri::TERM_ID_PARAM_NAME,
                Self::prefix_tag(self.is_session_id_tagged, session_id)
            );
        }

        if let Some(ttl) = &self.ttl {
            sb += &format!("{}={}|", channel_uri::TTL_PARAM_NAME, ttl.value);
        }

        if let Some(reliable) = &self.reliable {
            sb += &format!(
                "{}={}|",
                channel_uri::RELIABLE_STREAM_PARAM_NAME,
                Value::bool_to_string(reliable)
            );
        }

        if let Some(linger) = &self.linger {
            sb += &format!("{}={}|", channel_uri::LINGER_PARAM_NAME, linger.value);
        }

        if let Some(alias) = &self.alias {
            sb += &format!("{}={}|", channel_uri::ALIAS_PARAM_NAME, alias);
        }

        if let Some(cc) = &self.cc {
            sb += &format!("{}={}|", channel_uri::CONGESTION_CONTROL_PARAM_NAME, cc);
        }

        if let Some(fc) = &self.fc {
            sb += &format!("{}={}|", channel_uri::FLOW_CONTROL_PARAM_NAME, fc);
        }

        if let Some(gtag) = &self.gtag {
            sb += &format!("{}={}|", channel_uri::GROUP_TAG_PARAM_NAME, gtag.value);
        }

        if let Some(sparse) = &self.sparse {
            sb += &format!("{}={}|", channel_uri::SPARSE_PARAM_NAME, Value::bool_to_string(sparse));
        }

        if let Some(eos) = &self.eos {
            sb += &format!("{}={}|", channel_uri::EOS_PARAM_NAME, Value::bool_to_string(eos));
        }

        if let Some(tether) = &self.tether {
            sb += &format!("{}={}|", channel_uri::TETHER_PARAM_NAME, Value::bool_to_string(tether));
        }

        if let Some(group) = &self.group {
            sb += &format!("{}={}|", channel_uri::GROUP_PARAM_NAME, Value::bool_to_string(group));
        }

        if let Some(rejoin) = &self.rejoin {
            sb += &format!("{}={}|", channel_uri::REJOIN_PARAM_NAME, Value::bool_to_string(rejoin));
        }

        if let Some(ssc) = &self.ssc {
            sb += &format!("{}={}|", channel_uri::SPIES_SIMULATE_CONNECTION_PARAM_NAME, Value::bool_to_string(ssc));
        }

        if let Some(socket_rcvbuf_length) = &self.socket_rcvbuf_length {
            sb += &format!("{}={}|", channel_uri::SOCKET_SNDBUF_PARAM_NAME, socket_rcvbuf_length.value);
        }


        if let Some(socket_sndbuf_length) = &self.socket_sndbuf_length {
            sb += &format!("{}={}|", channel_uri::SOCKET_RCVBUF_PARAM_NAME, socket_sndbuf_length.value);
        }

        if let Some(receiver_window_length) = &self.receiver_window_length {
            sb += &format!("{}={}|", channel_uri::RECEIVER_WINDOW_LENGTH_PARAM_NAME, receiver_window_length.value);
        }

        if let Some(media_receive_timestamp_offset) = &self.media_receive_timestamp_offset {
            sb += &format!("{}={}|", channel_uri::MEDIA_RCV_TIMESTAMP_OFFSET_PARAM_NAME, media_receive_timestamp_offset);
        }

        if let Some(channel_receive_timestamp_offset) = &self.channel_receive_timestamp_offset {
            sb += &format!("{}={}|", channel_uri::CHANNEL_RCV_TIMESTAMP_OFFSET_PARAM_NAME, channel_receive_timestamp_offset);
        }

        if let Some(channel_send_timestamp_offset) = &self.channel_send_timestamp_offset {
            sb += &format!("{}={}|", channel_uri::CHANNEL_SND_TIMESTAMP_OFFSET_PARAM_NAME, channel_send_timestamp_offset);
        }

        if let Some(response_correlation_id) = &self.response_correlation_id {
            sb += &format!("{}={}|", channel_uri::RESPONSE_CORRELATION_ID_PARAM_NAME, response_correlation_id.value);
        }

        let last_char = sb.chars().last().unwrap();

        if last_char == '|' || last_char == '?' {
            sb.pop();
        }

        sb
    }
}

impl ChannelUriStringBuilder {
    #[inline]
    fn prefix_tag(is_tagged: bool, value: &Value) -> String {
        if is_tagged {
            format!("{}{}", channel_uri::TAG_PREFIX, value.value)
        } else {
            value.value.to_string()
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::channel_uri;
    use crate::channel_uri_string_builder::ChannelUriStringBuilder;

    #[test]
    fn should_generate_basic_ipc_channel() {
        let mut builder = ChannelUriStringBuilder::default();

        builder.media(channel_uri::IPC_MEDIA).unwrap();

        assert_eq!(builder.build(), "aeron:ipc");
    }

    #[test]
    fn should_generate_basic_udp_channel() {
        let mut builder = ChannelUriStringBuilder::default();

        builder.media(channel_uri::UDP_MEDIA).unwrap().endpoint("localhost:9999");

        assert_eq!(builder.build(), "aeron:udp?endpoint=localhost:9999");
    }

    #[test]
    fn should_generate_basic_udp_channel_spy() {
        let mut builder = ChannelUriStringBuilder::default();

        builder
            .prefix(Some(channel_uri::SPY_QUALIFIER))
            .unwrap()
            .media(channel_uri::UDP_MEDIA)
            .unwrap()
            .endpoint("localhost:9999");

        assert_eq!(builder.build(), "aeron-spy:aeron:udp?endpoint=localhost:9999");
    }

    #[test]
    fn should_generate_complex_udp_channel() {
        let mut builder = ChannelUriStringBuilder::default();

        builder
            .media(channel_uri::UDP_MEDIA)
            .unwrap()
            .endpoint("localhost:9999")
            .ttl(9)
            .term_length(1024 * 128)
            .unwrap();

        assert_eq!(builder.build(), "aeron:udp?endpoint=localhost:9999|term-length=131072|ttl=9");
    }

    #[test]
    fn should_generate_replay_udp_channel() {
        let mut builder = ChannelUriStringBuilder::default();

        builder
            .media(channel_uri::UDP_MEDIA)
            .unwrap()
            .endpoint("localhost:9999")
            .term_length(1024 * 128)
            .unwrap()
            .initial_term_id(777)
            .term_id(999)
            .term_offset(64)
            .unwrap();

        assert_eq!(
            builder.build(),
            "aeron:udp?endpoint=localhost:9999|term-length=131072|init-term-id=777|term-id=999|term-offset=64"
        );
    }
}
