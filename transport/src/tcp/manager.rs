

use crate::tcp::connection::Connection;
use crate::tcp::stream::{Stream, StreamState};
use crate::tcp::receiver;
use std::collections::HashMap;

use tokio::io::{AsyncRead, AsyncWrite};
use crate::frame::{frame::Frametype, frame::Frame, header::FLAG_FIN};
use crate::errors::TransportError;
use bytes::Bytes;

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

    async fn buffer_and_maybe_flush(&mut self, frame: &Frame,) -> Result<(), TransportError> {
        self.conn.buffer_frame(frame)?;

        if self.conn.write_buf_len() >= FLUSH_THRESHOLD {
            self.conn.flush().await?;
        }
        Ok(())
    }

    pub fn stream_count(&self) -> usize {
        self.streams.len()
    }

    pub fn stream_state(&self, stream_id: u32) -> Option<StreamState> {
        self.streams.get(&stream_id).map(|s| s.state)
    }

    pub fn send_window(&self, stream_id: u32) -> Option<i64> {
        self.streams.get(&stream_id).map(|s| s.send_window)
    }

    pub async fn flush(&mut self) -> Result<(), TransportError> {
        self.conn.flush().await
    }

    pub async fn open_stream(&mut self) -> Result<u32, TransportError> {
        let id = self.next_local_id;
        self.next_local_id = self.next_local_id
            .checked_add(2)
            .ok_or(TransportError::StreamIdExhausted)?;

        self.streams.insert(id, Stream::new(id));

        let open_frame = Frame::empty(Frametype::Open, 0, id);
        self.buffer_and_maybe_flush(&open_frame).await?;

        Ok(id)
    }

    pub async fn send_data(&mut self, stream_id: u32, payload: Vec<u8>) -> Result<(), TransportError> {
        
        if payload.is_empty() {
            return Ok(());
        }

        let stream = self.streams.get_mut(&stream_id).ok_or(TransportError::UnknownStream(stream_id))?;

        if !stream.state.can_send() {
            return Err(TransportError::StreamNotWritable(stream_id));
        }

        // Flow-control check: ensure the peer's window covers this payload.
        stream.check_send_window(payload.len())?;
        stream.consume_send_window(payload.len());

        let data_frame = Frame::new(Frametype::Data, 0, stream_id, payload);
        self.buffer_and_maybe_flush(&data_frame).await?;
        Ok(())
    }

    pub async fn close_stream(&mut self, stream_id: u32) -> Result<(), TransportError> {
        let now_closed = {
            let stream = self.streams.get_mut(&stream_id).ok_or(TransportError::UnknownStream(stream_id))?;
            if stream.state.is_closed() {
                return Err(TransportError::StreamClosed(stream_id));
            }

            stream.on_local_close();
            stream.state == StreamState::Closed
        };

        let close_frame = Frame::empty( Frametype::Close, FLAG_FIN, stream_id);
        self.buffer_and_maybe_flush(&close_frame).await?;

        if now_closed {
            self.streams.remove(&stream_id);
        }

        Ok(())
    }

    pub async fn reset_stream(&mut self, stream_id: u32) -> Result<(), TransportError> {
        let stream = self.streams.get_mut(&stream_id)
            .ok_or(TransportError::UnknownStream(stream_id))?;

        stream.on_reset();

        let reset_frame = Frame::empty(Frametype::Reset, 0, stream_id);
        self.conn.send_frame(&reset_frame).await?;
        self.streams.remove(&stream_id);
        Ok(())
    }

    pub fn recv_data(&mut self, stream_id: u32) -> Result<Option<Bytes>, TransportError> {
        let stream = self.streams.get_mut(&stream_id)
            .ok_or(TransportError::UnknownStream(stream_id))?;

        Ok(stream.pop_recv())
    }

    pub async fn recv_frame(&mut self) -> Result<u32, TransportError> {
        let frame = self.conn.recv_frame().await?;
        let stream_id = frame.header.stream_id;

        match frame.header.frame_type {
            Frametype::Open    => receiver::handle_open(&mut self.streams, self.role, stream_id)?,
            Frametype::Data    => receiver::handle_data(&mut self.streams, stream_id, frame.payload)?,
            Frametype::Close   => receiver::handle_close(&mut self.streams, stream_id)?,
            Frametype::Reset   => receiver::handle_reset(&mut self.streams, stream_id),
            Frametype::Window  => receiver::handle_window(&mut self.streams, stream_id, &frame.payload)?,

            // Phase 4+: handshake and heartbeat frames handled here.
            Frametype::Ping | Frametype::Pong |
            Frametype::Settings | Frametype::Hello | Frametype::Welcome |
            Frametype::Error => {}
        }

        Ok(stream_id)
    }
}