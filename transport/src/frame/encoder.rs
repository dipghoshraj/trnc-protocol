use bytes::{BufMut, Bytes, BytesMut};
use crate::frame;

/// Serialise a [`frame::frame::Frame`] into a contiguous [`Bytes`] buffer.
///
/// The output is ready to be written directly to a stream; it contains the
/// full 16-byte [`crate::frame::header::Header`] followed immediately by the
/// payload bytes.
///
/// # Wire layout
///
/// ```text
/// ┌──────────┬─────────┬───────┬───────────┬────────────────┬────────────┬─────────────┐
/// │ magic[4] │ ver [1] │ flags │ stream_id │ payload_length │ frame_type │ payload[..] │
/// │ "TRNC"   │         │  [2]  │    [4]    │      [4]       │    [1]     │             │
/// └──────────┴─────────┴───────┴───────────┴────────────────┴────────────┴─────────────┘
/// ```
///
/// All multi-byte integers are big-endian.
///
/// # Errors
///
/// Returns [`crate::errors::TransportError::InvalidFrame`] if
/// `frame.payload.len()` does not match `frame.header.payload_length`.
pub fn encode(frame: &frame::frame::Frame) -> Result<Bytes, crate::errors::TransportError> {
    if frame.payload.len() != frame.header.payload_length as usize {
        return Err(crate::errors::TransportError::InvalidFrame(format!(
            "Payload length mismatch: header says {}, but actual payload is {}",
            frame.header.payload_length,
            frame.payload.len()
        )));
    }

    let total_size = frame::header::HEADER_SIZE + frame.payload.len();
    let mut buffer = BytesMut::with_capacity(total_size);

    buffer.extend_from_slice(&frame.header.magic);
    buffer.put_u8(frame.header.version);
    buffer.extend_from_slice(&frame.header.flags.to_be_bytes());
    buffer.extend_from_slice(&frame.header.stream_id.to_be_bytes());
    buffer.extend_from_slice(&frame.header.payload_length.to_be_bytes());
    buffer.put_u8(frame.header.frame_type as u8);
    buffer.extend_from_slice(&frame.payload);

    Ok(buffer.freeze())
}