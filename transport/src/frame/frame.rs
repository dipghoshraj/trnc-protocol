use crate::frame;


/// A complete TRNC protocol frame, consisting of a fixed [`header::Header`]
/// and a variable-length payload.
///
/// Frames are the atomic unit of communication in the TRNC protocol.
/// Every message sent over a [`crate::tcp::connection::Connection`] is
/// wrapped in a `Frame` before being serialised onto the wire.
///
/// [`header::Header`]: crate::frame::header::Header
#[derive(Debug, Clone)]
pub struct Frame {
    /// The fixed-size 16-byte header describing this frame.
    pub header: frame::header::Header,
    /// The raw payload bytes; its length must always equal
    /// `header.payload_length` as a `usize`.
    pub payload: Vec<u8>,
}


/// Identifies the purpose of a [`Frame`].
///
/// Each variant maps to a single `u8` discriminant that is written into byte
/// 15 of the wire header.  The discriminant values are **stable** — changing
/// them is a breaking wire-format change.
///
/// | Variant    | Byte | Direction  | Description |
/// |------------|------|------------|-------------|
/// | `Open`     |  1   | client→srv | Open a new logical stream |
/// | `Data`     |  2   | both       | Carry opaque application data |
/// | `Close`    |  3   | both       | Gracefully close a stream |
/// | `Reset`    |  4   | both       | Abortively terminate a stream |
/// | `Ping`     |  5   | both       | Liveness probe (no payload) |
/// | `Pong`     |  6   | both       | Reply to a `Ping` |
/// | `Window`   |  7   | both       | Update the flow-control window |
/// | `Error`    |  8   | both       | Signal an error on a stream or connection |
/// | `Settings` |  9   | both       | Exchange connection-level parameters |
/// | `Hello`    | 10   | client→srv | Initiate the handshake |
/// | `Welcome`  | 11   | srv→client | Accept the handshake |
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Frametype {
    /// Open a new logical stream (`stream_id` must be unique per connection).
    Open     = 1,
    /// Carry opaque application payload on an established stream.
    Data     = 2,
    /// Gracefully half-close a stream; no more data will be sent.
    Close    = 3,
    /// Abortively reset a stream, discarding any buffered data.
    Reset    = 4,
    /// Liveness probe; the payload SHOULD be empty.
    Ping     = 5,
    /// Reply to a [`Frametype::Ping`]; echo the same payload.
    Pong     = 6,
    /// Update the peer's send window for the identified stream.
    Window   = 7,
    /// Report a stream-level or connection-level error via [`crate::errors::ErrorPayload`].
    Error    = 8,
    /// Exchange connection-level configuration parameters.
    Settings = 9,
    /// Initiate the version handshake (client → server).
    Hello    = 10,
    /// Accept the version handshake (server → client).
    Welcome  = 11,
}

impl Frametype {
    /// Convert a raw `u8` wire value into a [`Frametype`].
    ///
    /// Returns `None` for any value that does not correspond to a known
    /// variant, allowing callers to return a well-formed error rather than
    /// panicking.
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            1  => Some(Frametype::Open),
            2  => Some(Frametype::Data),
            3  => Some(Frametype::Close),
            4  => Some(Frametype::Reset),
            5  => Some(Frametype::Ping),
            6  => Some(Frametype::Pong),
            7  => Some(Frametype::Window),
            8  => Some(Frametype::Error),
            9  => Some(Frametype::Settings),
            10 => Some(Frametype::Hello),
            11 => Some(Frametype::Welcome),
            _  => None,
        }
    }
}

impl Frame {
    /// Construct a new [`Frame`], automatically building a matching
    /// [`header::Header`] from the supplied arguments.
    ///
    /// # Arguments
    ///
    /// * `frame_type` — the [`Frametype`] discriminant for this frame.
    /// * `flags`      — bitfield of `FLAG_*` constants from [`crate::frame::header`].
    /// * `stream_id`  — logical stream this frame belongs to (`0` for
    ///   connection-scoped frames such as `Ping`/`Pong`).
    /// * `payload`    — raw bytes to carry; may be empty.
    ///
    /// [`header::Header`]: crate::frame::header::Header
    pub fn new(frame_type: frame::frame::Frametype, flags: u16, stream_id: u32, payload: Vec<u8>) -> Self {
        let header = frame::header::Header::new(frame_type, flags, stream_id, payload.len() as u32);
        Frame { header, payload }
    }
    
    pub fn empty(frame_type: frame::frame::Frametype, flags: u16, stream_id: u32) -> Self {
        Self::new(frame_type, flags, stream_id, Vec::new()) 
    }
}