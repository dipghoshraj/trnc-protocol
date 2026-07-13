use bytes::{Buf, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use crate::{errors::TransportError, frame::frame::Frame, frame::encoder::encode, frame::decoder::decode, frame::header};

/// Maximum allowed size of the internal write buffer in bytes (256 KiB).
///
/// [`Connection::buffer_frame`] returns [`TransportError::BufferOverflow`]
/// if adding the encoded frame would push the buffer beyond this limit.
const MAX_BUFFER_SIZE: usize = 256 * 1024;

/// Initial capacity allocated for both the read and write buffers (64 KiB).
const MIN_BUFFER_SIZE: usize = 64 * 1024;

/// Number of bytes requested from the OS in each [`AsyncReadExt::read_buf`] call (8 KiB).
const CHUNK_SIZE: usize = 8 * 1024;

/// Maximum allowed size of the read buffer (one complete max-size frame).
///
/// If the buffer grows beyond this before a complete frame can be decoded,
/// the connection is torn down with [`TransportError::FrameTooLarge`] to
/// prevent a peer from forcing unbounded memory allocation.
const MAX_READ_BUFFER_SIZE: usize = header::MAX_FRAME_SIZE + header::HEADER_SIZE;


/// An async, buffered connection that sends and receives TRNC [`Frame`]s.
///
/// `Connection<T>` wraps any `T: AsyncRead + AsyncWrite + Unpin` — typically a
/// `tokio::net::TcpStream` — and provides a frame-oriented API on top of it.
///
/// ## Buffering strategy
///
/// Outgoing frames are accumulated in an internal write buffer via
/// [`buffer_frame`](Self::buffer_frame) and flushed to the stream all at once
/// by [`flush`](Self::flush).  This allows the caller to batch multiple frames
/// into a single system call.  Use [`send_frame`](Self::send_frame) when you
/// want the convenience of buffer-then-flush in one call.
///
/// Incoming bytes are read from the stream into a growing read buffer.  The
/// decoder is called each iteration until a complete frame can be extracted or
/// the stream signals EOF.
///
/// ## Example
///
/// ```no_run
/// use tokio::net::TcpStream;
/// use transport::tcp::Connection;
/// use transport::frame::frame::{Frame, Frametype};
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), transport::errors::TransportError> {
/// let stream = TcpStream::connect("127.0.0.1:4200").await?;
/// let mut conn = Connection::new(stream);
///
/// let frame = Frame::new(Frametype::Ping, 0, 0, vec![]);
/// conn.send_frame(&frame).await?;
///
/// let reply = conn.recv_frame().await?;
/// # Ok(())
/// # }
/// ```
/// 
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Connection<T> {
    stream: T,
    read_buffer: BytesMut,
    write_buffer: BytesMut,
}

impl <T: AsyncRead + AsyncWrite + Unpin> Connection<T> {
    /// Wrap `stream` in a new `Connection` with default buffer capacities.
    pub fn new(stream: T) -> Self {
        Self {
            stream,
            read_buffer: BytesMut::with_capacity(MIN_BUFFER_SIZE),
            write_buffer: BytesMut::with_capacity(MIN_BUFFER_SIZE),
        }
    }

    /// Encode `frame` and append it to the internal write buffer.
    ///
    /// The frame is **not** written to the underlying stream until
    /// [`flush`](Self::flush) (or [`send_frame`](Self::send_frame)) is called.
    ///
    /// # Errors
    ///
    /// Returns [`TransportError::BufferOverflow`] if the encoded frame would
    /// push the write buffer beyond `MAX_BUFFER_SIZE` (256 KiB).
    pub fn buffer_frame(&mut self, frame: &Frame) -> Result<(), TransportError>  {
        let encoder = encode(frame)?;
        if self.write_buffer.len() + encoder.len() > MAX_BUFFER_SIZE {
            return Err(TransportError::BufferOverflow);
        }
        self.write_buffer.extend_from_slice(&encoder);
        Ok(())
    }

    /// Write all buffered frames to the underlying stream.
    ///
    /// Does nothing if the write buffer is already empty.
    ///
    /// # Errors
    ///
    /// Propagates any [`std::io::Error`] returned by the underlying stream as
    /// [`TransportError::Io`].
    pub async fn flush(&mut self) -> Result<(), TransportError> {
        if self.write_buffer.is_empty() {
            return Ok(());
        }
        self.stream.write_all(&self.write_buffer).await?;
        self.write_buffer.clear();
        self.stream.flush().await?;
        Ok(())
    }

    /// Encode, buffer, and immediately flush a single frame to the stream.
    ///
    /// Equivalent to calling [`buffer_frame`](Self::buffer_frame) followed by
    /// [`flush`](Self::flush).
    ///
    /// # Errors
    ///
    /// Returns any error from [`buffer_frame`](Self::buffer_frame) or
    /// [`flush`](Self::flush).
    pub async fn send_frame(&mut self, frame: &Frame) -> Result<(), TransportError> {
        self.buffer_frame(frame)?;
        self.flush().await
    }
 
    /// Extract the underlying stream from the connection.
    pub fn into_inner(self) -> T {
        self.stream
    }
 
    /// Returns the number of bytes currently sitting in the write buffer
    /// (i.e. encoded but not yet flushed to the stream).
    pub fn write_buf_len(&self) -> usize {
        self.write_buffer.len()
    }

    /// Read from the stream until one complete [`Frame`] can be decoded and
    /// return it.
    ///
    /// Internally the function loops: it first attempts to decode a frame from
    /// whatever bytes are already in the read buffer; if more data is needed it
    /// reads the next `CHUNK_SIZE` (8 KiB) chunk from the stream and tries again.
    ///
    /// # Errors
    ///
    /// | Error | Cause |
    /// |-------|-------|
    /// | [`TransportError::ConnectionClosed`] | The stream returned 0 bytes (EOF) |
    /// | [`TransportError::InvalidMagic`]     | Bad frame magic bytes |
    /// | [`TransportError::InvalidVersion`]   | Unsupported protocol version |
    /// | [`TransportError::InvalidFrame`]     | Unknown frame type or flag bits |
    /// | [`TransportError::FrameTooLarge`]    | Declared payload exceeds the limit |
    /// | [`TransportError::Io`]               | Underlying I/O error |
    pub async fn recv_frame(&mut self) -> Result<Frame, TransportError> {
        loop {

            match decode(&self.read_buffer[..]) {
                Ok((frame, consumed)) => {
                    self.read_buffer.advance(consumed);
                    return Ok(frame);
                }
 
                Err(TransportError::NeedMoreData) => {
                    // Not enough bytes yet for a complete frame.
                    // Fall through to the read below.
                }
 
                Err(e) => {
                    return Err(e);
                }
            }
 
            if self.read_buffer.len() >= MAX_READ_BUFFER_SIZE {
                return Err(TransportError::FrameTooLarge {
                    size: self.read_buffer.len(),
                    max: header::MAX_FRAME_SIZE,
                });
            }

            self.read_buffer.reserve(CHUNK_SIZE);

            let n = self.stream.read_buf(&mut self.read_buffer).await?;
            if n == 0 {
                return Err(TransportError::ConnectionClosed);
            }
        }
    }
 
    /// Returns the number of bytes currently sitting in the read buffer
    /// (i.e. received from the stream but not yet consumed by the decoder).
    pub fn read_buf_len(&self) -> usize {
        self.read_buffer.len()
    }


}