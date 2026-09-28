
use super::{frame};
use super::header::HEADER_SIZE;

/// Attempt to decode one [`frame::Frame`] from the front of `buffer`.
///
/// The function is designed to be called in a loop inside an async read loop
/// (see [`crate::tcp::connection::Connection::recv_frame`]).  It only borrows
/// `buffer` immutably, so the caller is responsible for advancing the cursor
/// after a successful decode.
///
/// # Return value
///
/// * `Ok((frame, consumed))` — a complete frame was decoded; `consumed` is the
///   total number of bytes read from the start of `buffer` (header + payload).
///   The caller should call `buffer.advance(consumed)` to discard those bytes.
/// * `Err(TransportError::NeedMoreData)` — `buffer` does not yet contain a
///   complete frame.  The caller should read more bytes and retry.
/// * `Err(e)` — the header or payload failed validation; the connection should
///   be torn down.
///
/// # Errors
///
/// | Error | Cause |
/// |-------|-------|
/// | [`crate::errors::TransportError::NeedMoreData`]   | Buffer is shorter than [`HEADER_SIZE`] or the declared payload |
/// | [`crate::errors::TransportError::InvalidFrame`]   | Unknown `frame_type` byte or unknown flag bits |
/// | [`crate::errors::TransportError::InvalidMagic`]   | First four bytes are not `"TRNC"` |
/// | [`crate::errors::TransportError::InvalidVersion`] | `version` field is outside the supported range |
/// | [`crate::errors::TransportError::FrameTooLarge`]  | `payload_length` exceeds [`crate::frame::header::MAX_FRAME_SIZE`] |
pub fn decode(buffer: &[u8]) -> Result<(frame::Frame, usize), crate::errors::TransportError> {
    use crate::frame::header::Header;
    use crate::frame::frame::Frametype;

    if buffer.len() < HEADER_SIZE {
        return Err(crate::errors::TransportError::NeedMoreData);
    }

    let magic = [buffer[0], buffer[1], buffer[2], buffer[3]];
    let version = buffer[4];
    let flags = u16::from_be_bytes([buffer[5], buffer[6]]);
    let stream_id = u32::from_be_bytes([buffer[7], buffer[8], buffer[9], buffer[10]]);
    let payload_length = u32::from_be_bytes([buffer[11], buffer[12], buffer[13], buffer[14]]);
    let frame_type_u8 = buffer[15];
    
    let frame_type = Frametype::from_u8(frame_type_u8)
        .ok_or_else(|| crate::errors::TransportError::InvalidFrame(format!("Unknown frame type: {}", frame_type_u8)))?;

    let header = Header {
        magic,
        version,
        flags,
        stream_id,
        payload_length,
        frame_type,
    };

    header.validate()?;

    if buffer.len() < HEADER_SIZE + payload_length as usize {
        return Err(crate::errors::TransportError::NeedMoreData);
    }

    let payload = buffer[HEADER_SIZE..HEADER_SIZE + payload_length as usize].to_vec();
    let total_len = HEADER_SIZE + payload_length as usize;

    Ok((frame::Frame { header, payload }, total_len))
}
