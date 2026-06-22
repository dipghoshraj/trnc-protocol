
use std::collections::HashMap;

use crate::errors::TransportError;
use crate::tcp::manager::Role;
use crate::tcp::stream::{Stream, StreamState};

/// Handles an incoming `Open` frame: validates ID parity and registers the new stream.
pub fn handle_open(streams: &mut HashMap<u32, Stream>, role: Role, stream_id: u32) -> Result<(), TransportError> {
    let expected_parity = match role {
        Role::Initiator => 0, // remote is Acceptor → even IDs
        Role::Acceptor  => 1, // remote is Initiator → odd IDs
    };

    if stream_id % 2 != expected_parity {
        return Err(TransportError::InvalidFrame(
            format!("remote opened stream {stream_id} but parity is wrong"),
        ));
    }
    if streams.contains_key(&stream_id) {
        return Err(TransportError::InvalidFrame(
            format!("remote opened stream {stream_id} but it already exists"),
        ));
    }

    streams.insert(stream_id, Stream::new(stream_id));
    Ok(())
}

/// Handles an incoming `Data` frame: validates stream state and enqueues the payload.
pub fn handle_data( streams: &mut HashMap<u32, Stream>, stream_id: u32, payload: Vec<u8>) -> Result<(), TransportError> {
    let stream = streams
        .get_mut(&stream_id)
        .ok_or(TransportError::UnknownStream(stream_id))?;

    if !stream.state.can_receive() {
        return Err(TransportError::InvalidFrame(
            format!("Data frame on non-receivable stream {stream_id}"),
        ));
    }

    stream.push_recv(bytes::Bytes::from(payload));
    Ok(())
}

/// Handles an incoming `Close` frame: advances the stream's half-close state,
/// removing the stream entirely if both sides have now closed.
pub fn handle_close( streams: &mut HashMap<u32, Stream>, stream_id: u32) -> Result<(), TransportError> {
    let now_closed = {
        let stream = streams
            .get_mut(&stream_id)
            .ok_or(TransportError::UnknownStream(stream_id))?;
        stream.on_remote_close();
        stream.state == StreamState::Closed
    };

    if now_closed {
        streams.remove(&stream_id);
    }
    Ok(())
}

/// Handles an incoming `Reset` frame: tears down the stream immediately.
pub fn handle_reset(streams: &mut HashMap<u32, Stream>, stream_id: u32) {
    streams.remove(&stream_id);
}

/// Handles an incoming `Window` frame: parses the 4-byte big-endian increment
/// and applies it to the stream's send window.
pub fn handle_window( streams: &mut HashMap<u32, Stream>, stream_id: u32, payload: &[u8]) -> Result<(), TransportError> {
    let stream = streams
        .get_mut(&stream_id)
        .ok_or(TransportError::UnknownStream(stream_id))?;

    let bytes: [u8; 4] = payload
        .try_into()
        .map_err(|_| TransportError::InvalidFrame(
            "Window frame payload must be exactly 4 bytes".into(),
        ))?;

    let increment = u32::from_be_bytes(bytes);
    stream.apply_window_increment(increment);
    Ok(())
}