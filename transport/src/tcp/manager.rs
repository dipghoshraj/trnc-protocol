

use crate::tcp::connection::{Connection};
use crate::tcp::stream::{Stream, StreamState};
use std::collections::HashMap;

use bytes::Bytes;
use tokio::io::{AsyncRead, AsyncWrite};
use crate::frame::{frame::Frametype, header::FLAG_CONTROL, frame::Frame};
use crate::errors::TransportError;

const FLUSH_THRESHOLD: usize = 32 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Initiator, // client side
    Acceptor, // server side
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamManager<T> {
    conn: Connection<T>,
    streams: HashMap<u32, Stream>,
    next_local_id: u32,
    role: Role,
}

impl <T: AsyncRead + AsyncWrite + Unpin> StreamManager<T> {
    pub fn new(conn: Connection<T>, role: Role) -> Self {

        let next_local_id = match role {
            Role::Initiator => 1,
            Role::Acceptor  => 2,
        };


        Self {
            conn,
            streams: HashMap::new(),
            next_local_id,
            role,
        }
    }

    pub async fn open_stream(&mut self) -> Result<u32, TransportError> {
        
        if self.next_local_id == 0 {
            return Err(TransportError::StreamIdExhausted);
        }

        let id = self.next_local_id;
        self.next_local_id = self.next_local_id.wrapping_add(2);

        self.streams.insert(id, Stream::new(id));


        let open_frame = Frame::empty(id, Frametype::Open, FLAG_CONTROL);
        self.buffer_and_maybe_flush(&open_frame).await?;

        Ok(id)
    }

    pub async fn send_data(&mut self, stream_id: u32, payload: Vec<u8>) -> Result<(), TransportError> {
        
        if payload.is_empty() {
            // Zero-byte data frames are a no-op; skip the overhead.
            return Ok(());
        }

        let stream = self.streams.get_mut(&stream_id)
            .ok_or(TransportError::UnknownStream(stream_id))?;

        // State check: can we send in the current state?
        if !stream.state.can_send() {
            return Err(TransportError::StreamNotWritable(stream_id));
        }


        let data_frame = Frame::new(Frametype::Data, 0, stream_id, payload );
        self.buffer_and_maybe_flush(&data_frame).await?;
        Ok(())
    }

    // ── Internal helpers ───────────────────────────────────────────────────────

    // Buffer a frame and auto-flush if the write buffer is large enough.
    // This is the core of the write-batching strategy.
    async fn buffer_and_maybe_flush(&mut self, frame: &Frame,) -> Result<(), TransportError> {
        self.conn.buffer_frame(frame)?;

        // Auto-flush if buffer has grown past FLUSH_THRESHOLD.
        if self.conn.write_buf_len() >= FLUSH_THRESHOLD {
            self.conn.flush().await?;
        }
        Ok(())
    }

    // ── Introspection ──────────────────────────────────────────────────────────
    pub fn stream_count(&self) -> usize {
        self.streams.len()
    }

    pub fn stream_state(&self, stream_id: u32) -> Option<StreamState> {
        self.streams.get(&stream_id).map(|s| s.state)
    }

    pub fn send_window(&self, stream_id: u32) -> Option<i64> {
        self.streams.get(&stream_id).map(|s| s.send_window)
    }


}