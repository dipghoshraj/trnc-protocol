

/// Structured payload carried inside a wire-level [`crate::frame::frame::Frametype::Error`] frame.
#[derive(Debug)]
pub struct ErrorPayload {
    /// Application-level error code (maps to [`ErrorCode`]).
    pub error_code: u16,
    /// Stream this error applies to (`0` for connection-scoped errors).
    pub stream_id: u32,
    /// Byte-length of the human-readable message that follows in the payload.
    pub message_len: u16,
}

/// Application-level error codes sent inside [`ErrorPayload`].
///
/// These codes are transmitted over the wire as `u16` big-endian values and
/// let the remote peer distinguish recoverable stream errors from fatal
/// connection errors without parsing a free-form message.
#[derive(Debug, Clone, Copy)]
#[repr(u16)]
pub enum ErrorCode {
    /// Catch-all; used when no more-specific code applies.
    Unknown            = 0,
    /// Generic, unrecoverable protocol violation.
    ProtocolError      = 1,
    /// Version negotiation failed; the peers speak incompatible protocol versions.
    InvalidVersion     = 2,
    /// A malformed or otherwise invalid frame was received.
    InvalidFrame       = 3,
    /// The frame's declared size exceeded [`crate::frame::header::MAX_FRAME_SIZE`].
    FrameTooLarge      = 4,
    /// A frame arrived for a stream that has already been closed.
    StreamClosed       = 5,
    /// The stream was reset by the remote peer.
    StreamReset        = 6,
    /// The sender exceeded its advertised flow-control window.
    FlowControlViolation = 7,
    /// The connection handshake could not be completed.
    HandshakeRejected  = 8,
    /// The peer failed to respond within the configured timeout.
    Timeout            = 9,
    /// An internal implementation error that should not occur in production.
    InternalError      = 10,
    // Action not found
    ActionNotFound     = 11,
    // Internal error codes (not sent over the wire)
    InternalIoError     = 12
}

/// All errors that can be returned by the `transport` crate.
#[derive(Debug)]
pub enum TransportError {
    /// The frame magic bytes did not match [`crate::frame::header::FRAME_MAGIC`].
    InvalidMagic,
    /// The frame's version field is outside the supported range.
    InvalidVersion { got: u8, min: u8, max: u8 },
    /// The frame is structurally invalid (e.g. unknown flags or unknown frame type).
    InvalidFrame(String),
    /// The frame's claimed payload size exceeds [`crate::frame::header::MAX_FRAME_SIZE`].
    FrameTooLarge { size: usize, max: usize },
    /// The underlying I/O stream was closed by the remote peer.
    ConnectionClosed,
    // TlsError(rustls::Error),
    /// An operation targeted a stream that has already been closed.
    StreamClosed(u32),
    /// The identified stream was reset by the remote peer.
    StreamReset(u32),
    /// The sender exceeded its advertised flow-control window on the given stream.
    FlowControlViolation { stream_id: u32 },
    /// The connection handshake was rejected with the given code and message.
    HandshakeRejected { code: ErrorCode, message: String },
    /// The decoder needs more bytes before it can produce a complete frame.
    ///
    /// This is an internal sentinel used by [`crate::frame::decoder`]; callers
    /// should buffer more data and retry, not surface this to end-users.
    NeedMoreData,
    /// No more stream IDs are available for new streams.
    StreamIdExhausted,
    /// The write buffer would exceed its maximum capacity.
    BufferOverflow,
    /// An operation did not complete within the allowed time.
    Timeout,
    /// An underlying [`std::io::Error`].
    Io(std::io::Error),

    /// An operation targeted a stream that does not exist.
    UnknownStream(u32),

    /// An attempt was made to send data on a stream that is not currently writable.
    StreamNotWritable(u32),
    /// An action was requested that does not exist.
    ActionNotFound(String),
    /// An internal error occurred that should not happen in production.
    InternalError(String),
}

impl std::fmt::Display for TransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransportError::InvalidMagic => write!(f, "Invalid magic number in header"),
            TransportError::InvalidVersion { got, min, max } => write!(f, "Invalid version: got {}, expected between {} and {}", got, min, max),
            TransportError::InvalidFrame(msg) => write!(f, "Invalid frame: {}", msg),
            TransportError::FrameTooLarge { size, max } => write!(f, "Frame too large: size {}, max {}", size, max),
            TransportError::ConnectionClosed => write!(f, "Connection closed"),
            // TransportError::TlsError(e) => write!(f, "TLS error: {}", e),
            TransportError::StreamClosed(id) => write!(f, "Stream {} is closed", id),
            TransportError::StreamReset(id) => write!(f, "Stream {} was reset by remote", id),
            TransportError::FlowControlViolation { stream_id } => write!(f, "Flow control violation on stream {}", stream_id),
            TransportError::HandshakeRejected { code, message } => write!(f, "Handshake rejected: {:?} - {}", code, message),
            TransportError::NeedMoreData => write!(f, "Need more data to parse frame"),
            TransportError::BufferOverflow => write!(f, "Buffer overflow"),
            TransportError::Timeout => write!(f, "Operation timed out"),
            TransportError::Io(e) => write!(f, "I/O error: {}", e),
            TransportError::StreamIdExhausted => write!(f, "No more stream IDs available"),
            TransportError::UnknownStream(id) => write!(f, "Unknown stream: {}", id),
            TransportError::StreamNotWritable(id) => write!(f, "Stream {} is not writable", id),
            TransportError::ActionNotFound(action) => write!(f, "Action not found: {}", action),
            TransportError::InternalError(msg) => write!(f, "Internal error: {}", msg),
        }
    }
}

impl std::error::Error for TransportError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            TransportError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for TransportError {
    fn from(e: std::io::Error) -> Self {
        TransportError::Io(e)
    }
}