

use crate::tcp::connection::Connection;
use crate::tcp::stream::{Stream, StreamState};
use crate::tcp::receiver;
use std::collections::HashMap;

use tokio::io::{AsyncRead, AsyncWrite};
use crate::frame::{frame::Frametype, frame::Frame, header::{FLAG_FIN, FLAG_CONTROL, MAX_FRAME_SIZE}};
use crate::errors::{ErrorCode, ErrorPayload, TransportError};
use bytes::Bytes;

const FLUSH_THRESHOLD: usize = 32 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Initiator, // client side
    Acceptor, // server side
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HandshakeState {
    /// Waiting for an initial Hello frame from the remote peer.
    AwaitingHello,
    /// The local peer sent Hello and is waiting for Welcome.
    AwaitingWelcome,
    /// The connection handshake completed successfully.
    Established,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamManager<T> {
    conn: Connection<T>,
    streams: HashMap<u32, Stream>,
    next_local_id: u32,
    role: Role,
    handshake_state: HandshakeState,
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
            handshake_state: HandshakeState::AwaitingHello,
        }
    }

    /// Returns the current connection handshake state.
    pub fn handshake_state(&self) -> HandshakeState {
        self.handshake_state
    }

    /// Begin the connection handshake from the initiator side.
    ///
    /// For `Role::Initiator`, this sends a `Hello` frame and transitions into
    /// `HandshakeState::AwaitingWelcome`.
    pub async fn start_handshake(&mut self) -> Result<(), TransportError> {
        if self.handshake_state == HandshakeState::Established {
            return Ok(());
        }
        if self.role == Role::Initiator {
            self.send_hello().await?;
            self.conn.flush().await?;
            while self.handshake_state != HandshakeState::Established {
                let frame = self.recv_frame().await?;
                if frame.header.frame_type == Frametype::Welcome {
                    break;
                }
            }
        }
        Ok(())
    }

    fn ensure_handshake_complete(&self) -> Result<(), TransportError> {
        if self.handshake_state == HandshakeState::Established {
            Ok(())
        } else {
            Err(TransportError::InvalidFrame(
                "connection handshake is not complete".into(),
            ))
        }
    }

    async fn send_hello(&mut self) -> Result<(), TransportError> {
        if self.role != Role::Initiator {
            return Err(TransportError::InvalidFrame(
                "only initiator may send Hello".into(),
            ));
        }
        if self.handshake_state == HandshakeState::AwaitingWelcome
            || self.handshake_state == HandshakeState::Established
        {
            return Ok(());
        }

        let hello = Frame::empty(Frametype::Hello, FLAG_CONTROL, 0);
        self.conn.send_frame(&hello).await?;
        self.handshake_state = HandshakeState::AwaitingWelcome;
        Ok(())
    }

    async fn send_welcome(&mut self) -> Result<(), TransportError> {
        if self.role != Role::Acceptor {
            return Err(TransportError::InvalidFrame(
                "only acceptor may send Welcome".into(),
            ));
        }
        if self.handshake_state == HandshakeState::Established {
            return Ok(());
        }

        let welcome = Frame::empty(Frametype::Welcome, FLAG_CONTROL, 0);
        self.conn.send_frame(&welcome).await?;
        self.handshake_state = HandshakeState::Established;
        Ok(())
    }

    async fn send_error_frame(&mut self, error_payload: ErrorPayload) -> Result<(), TransportError> {
        let error_frame = Frame::new(Frametype::Error, 0, error_payload.stream_id, error_payload.encode());
        self.conn.send_frame(&error_frame).await
    }

    async fn fail_handshake(&mut self, message: String) -> Result<(), TransportError> {
        let error_payload = ErrorPayload {
            error_code: ErrorCode::HandshakeRejected,
            stream_id: 0,
            message,
        };
        self.send_error_frame(error_payload).await
    }

    async fn handle_hello(&mut self, frame: &Frame) -> Result<(), TransportError> {
        if frame.header.stream_id != 0 {
            return Err(TransportError::InvalidFrame(
                "Hello frame must use stream_id 0".into(),
            ));
        }
        if self.role != Role::Acceptor {
            return Err(TransportError::InvalidFrame(
                "only acceptor may receive Hello".into(),
            ));
        }
        if self.handshake_state != HandshakeState::AwaitingHello {
            return Err(TransportError::InvalidFrame(
                "unexpected Hello frame".into(),
            ));
        }

        self.send_welcome().await
    }

    fn handle_welcome(&mut self, frame: &Frame) -> Result<(), TransportError> {
        if frame.header.stream_id != 0 {
            return Err(TransportError::InvalidFrame(
                "Welcome frame must use stream_id 0".into(),
            ));
        }
        if self.role != Role::Initiator {
            return Err(TransportError::InvalidFrame(
                "only initiator may receive Welcome".into(),
            ));
        }
        if self.handshake_state != HandshakeState::AwaitingWelcome {
            return Err(TransportError::InvalidFrame(
                "unexpected Welcome frame".into(),
            ));
        }

        self.handshake_state = HandshakeState::Established;
        Ok(())
    }

    fn handle_settings(&mut self, frame: &Frame) -> Result<(), TransportError> {
        if frame.header.stream_id != 0 {
            return Err(TransportError::InvalidFrame(
                "Settings frame must use stream_id 0".into(),
            ));
        }

        // Connection-level settings are accepted, but not yet acted on.
        Ok(())
    }

    fn handle_error(&self, frame: &Frame) -> Result<Frame, TransportError> {
        let error_payload = ErrorPayload::decode(&frame.payload)?;
        if error_payload.error_code == ErrorCode::HandshakeRejected && error_payload.stream_id == 0 {
            return Err(TransportError::HandshakeRejected {
                code: error_payload.error_code,
                message: error_payload.message,
            });
        }

        Err(TransportError::RemoteError(
            error_payload.error_code,
            error_payload.stream_id,
            error_payload.message,
        ))
    }

    /// Consume the manager and return the underlying connection.
    pub fn into_connection(self) -> Connection<T> {
        self.conn
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
        self.ensure_handshake_complete()?;

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
        self.ensure_handshake_complete()?;
        
        if payload.is_empty() {
            return Ok(());
        }

        if payload.len() > MAX_FRAME_SIZE {
            return Err(TransportError::FrameTooLarge { size: payload.len(), max: MAX_FRAME_SIZE });
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
        self.ensure_handshake_complete()?;

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
        self.ensure_handshake_complete()?;

        let stream = self.streams.get_mut(&stream_id)
            .ok_or(TransportError::UnknownStream(stream_id))?;

        stream.on_reset();

        let reset_frame = Frame::empty(Frametype::Reset, 0, stream_id);
        self.conn.send_frame(&reset_frame).await?;
        self.streams.remove(&stream_id);
        Ok(())
    }

    pub async fn recv_data(&mut self, stream_id: u32) -> Result<Option<Bytes>, TransportError> {
        self.ensure_handshake_complete()?;

        let (payload, should_send_window) = {
            let stream = self.streams.get_mut(&stream_id)
                .ok_or(TransportError::UnknownStream(stream_id))?;
            let can_recv = stream.state.can_receive();
            (stream.pop_recv(), can_recv)
        };

        if let Some(ref data) = payload {
            if should_send_window && !data.is_empty() {
                let increment = data.len() as u32;
                let window_frame = Frame::new(Frametype::Window, 0, stream_id, increment.to_be_bytes().to_vec());
                self.buffer_and_maybe_flush(&window_frame).await?;
            }
        }

        Ok(payload)
    }

    /// Send a [`Frametype::Ping`] frame and flush immediately.
    ///
    /// The remote peer echoes the payload back in a [`Frametype::Pong`] frame,
    /// which the next call to [`recv_frame`](Self::recv_frame) will return to
    /// the caller.
    pub async fn send_ping(&mut self, payload: Vec<u8>) -> Result<(), TransportError> {
        let ping = Frame::new(Frametype::Ping, FLAG_CONTROL, 0, payload);
        self.conn.send_frame(&ping).await
    }

    /// Read the next frame from the connection, dispatch it to the appropriate
    /// internal handler, and return the raw [`Frame`] to the caller.
    ///
    /// The caller can inspect [`Frame::header`] to determine what arrived
    /// (`frame_type`, `stream_id`, `flags`).  For `Data` frames the payload is
    /// also available in the returned [`Frame`]; the same bytes are queued in
    /// the stream's receive buffer and can be consumed with
    /// [`recv_data`](Self::recv_data).
    pub async fn recv_frame(&mut self) -> Result<Frame, TransportError> {
        let frame = self.conn.recv_frame().await?;
        let stream_id = frame.header.stream_id;

        if self.handshake_state != HandshakeState::Established {
            match frame.header.frame_type {
                Frametype::Hello => {
                    self.handle_hello(&frame).await?;
                }
                Frametype::Welcome => {
                    self.handle_welcome(&frame)?;
                }
                Frametype::Settings => {
                    self.handle_settings(&frame)?;
                }
                Frametype::Error => {
                    return self.handle_error(&frame);
                }
                Frametype::Ping | Frametype::Pong => {
                    // Allow ping/pong during handshake without disrupting state.
                    if frame.header.frame_type == Frametype::Ping {
                        let pong = Frame::new(Frametype::Pong, FLAG_CONTROL, 0, frame.payload.clone());
                        self.conn.send_frame(&pong).await?;
                    }
                }
                _ => {
                    if self.role == Role::Acceptor {
                        self.fail_handshake("received non-handshake frame before handshake completed".into()).await.ok();
                    }
                    return Err(TransportError::InvalidFrame(
                        "received frame before connection handshake completed".into(),
                    ));
                }
            }

            return Ok(frame);
        }

        match frame.header.frame_type {
            Frametype::Open    => receiver::handle_open(&mut self.streams, self.role, stream_id)?,
            // Clone payload: the original stays in `frame` so the caller can inspect it.
            Frametype::Data    => receiver::handle_data(&mut self.streams, stream_id, frame.payload.clone())?,
            Frametype::Close   => receiver::handle_close(&mut self.streams, stream_id)?,
            Frametype::Reset   => receiver::handle_reset(&mut self.streams, stream_id),
            Frametype::Window  => receiver::handle_window(&mut self.streams, stream_id, &frame.payload)?,

            Frametype::Ping => {
                // Reply immediately with a Pong carrying the same payload.
                // Use send_frame (flush included) so the Pong is not held
                // in the write buffer waiting for the flush threshold.
                let pong = Frame::new(Frametype::Pong, FLAG_CONTROL, 0, frame.payload.clone());
                self.conn.send_frame(&pong).await?;
            }

            Frametype::Hello => {
                self.handle_hello(&frame).await?;
            }

            Frametype::Welcome => {
                self.handle_welcome(&frame)?;
            }

            Frametype::Settings => {
                self.handle_settings(&frame)?;
            }

            Frametype::Error => {
                return self.handle_error(&frame);
            }

            Frametype::Pong => {}
        }

        Ok(frame)
    }
}