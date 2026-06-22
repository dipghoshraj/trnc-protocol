use crate::{errors::TransportError, frame};

/// Flag bit 0 — marks the **final** frame on a stream.
///
/// When set, the sender will not transmit any more `Data` frames for
/// `stream_id`.  The receiver SHOULD treat all subsequent data frames on
/// the same stream as a protocol error.
pub const FLAG_FIN:      u16 = 0b0000_0000_0000_0001; // bit 0

/// Flag bit 1 — acknowledgement flag.
///
/// Used in handshake and flow-control exchanges to acknowledge a previous
/// frame from the remote peer.
pub const FLAG_ACK:      u16 = 0b0000_0000_0000_0010; // bit 1

/// Flag bit 2 — marks the frame as a **control** frame.
///
/// Control frames (`Ping`, `Pong`, `Settings`, `Hello`, `Welcome`) MUST have
/// this bit set.  Data-plane frames (`Data`, `Open`, `Close`, `Reset`)
/// MUST NOT set it.
pub const FLAG_CONTROL:  u16 = 0b0000_0000_0000_0100; // bit 2


/// The 4-byte magic prefix written at the start of every frame header.
///
/// The ASCII string `"TRNC"` (`0x54 0x52 0x4E 0x43`) is used to detect
/// stream mis-alignment; any received frame whose first four bytes do not
/// match this value is rejected with [`TransportError::InvalidMagic`].
// The magic number we write into every frame we send, to help detect framing errors. TRNC
pub const FRAME_MAGIC: [u8; 4] = [0x54, 0x52, 0x4E, 0x43];
 
/// Protocol version written into every outgoing frame header.
pub const CURRENT_VERSION: u8 = 1;
 
/// The oldest protocol version this implementation will accept from a peer.
///
/// Frames advertising a version below this value are rejected with
/// [`TransportError::InvalidVersion`].
pub const MIN_SUPPORTED_VERSION: u8 = 1;
 
/// Maximum allowed payload size in bytes (16 MiB).
///
/// Frames that declare a `payload_length` larger than this constant are
/// rejected *before* any payload bytes are read, preventing a malicious
/// peer from forcing the allocation of gigabytes of memory.
pub const MAX_FRAME_SIZE: usize = 16 * 1024 * 1024;
 
/// Byte length of the fixed frame header.
///
/// Layout:
/// ```text
/// magic[4] + version[1] + flags[2] + stream_id[4] + payload_length[4] + frame_type[1]
/// = 16 bytes total
/// ```
pub const HEADER_SIZE: usize = 16;



/// The fixed 16-byte header that precedes every TRNC frame on the wire.
///
/// All multi-byte integer fields are encoded in **big-endian** order.
/// After decoding, call [`Header::validate`] to confirm that all fields
/// carry legal values before processing the rest of the frame.
#[derive(Debug, Clone)]
pub struct Header {
    /// Magic bytes — always `TRNC` (`[0x54, 0x52, 0x4E, 0x43]`).
    pub magic: [u8; 4], // "TRNC"
    /// Wire protocol version (currently [`CURRENT_VERSION`]).
    pub version: u8,
    /// Bitfield of [`FLAG_FIN`], [`FLAG_ACK`], and [`FLAG_CONTROL`].
    pub flags: u16,
    /// Logical stream identifier; `0` for connection-scoped frames.
    pub stream_id: u32,
    /// Byte length of the payload that follows this header.
    pub payload_length: u32,
    /// Identifies the purpose of this frame.
    pub frame_type: frame::frame::Frametype,
}


impl Header {
    /// Returns `true` if the [`FLAG_FIN`] bit is set in [`Header::flags`].
    pub fn is_fin(&self) -> bool {
        self.flags & FLAG_FIN != 0
    }

    /// Returns `true` if the [`FLAG_ACK`] bit is set in [`Header::flags`].
    pub fn is_ack(&self) -> bool {
        self.flags & FLAG_ACK != 0
    }

    /// Returns `true` if the [`FLAG_CONTROL`] bit is set in [`Header::flags`].
    pub fn is_control(&self) -> bool {
        self.flags & FLAG_CONTROL != 0
    }

    /// Validate that all fields in this header contain legal values.
    ///
    /// Checks performed (in order):
    ///
    /// 1. [`magic`](Header::magic) equals [`FRAME_MAGIC`].
    /// 2. [`version`](Header::version) is within `[MIN_SUPPORTED_VERSION, CURRENT_VERSION]`.
    /// 3. [`payload_length`](Header::payload_length) does not exceed [`MAX_FRAME_SIZE`].
    /// 4. [`frame_type`](Header::frame_type) is a known [`crate::frame::frame::Frametype`] variant.
    /// 5. [`flags`](Header::flags) contains no undefined bits.
    ///
    /// # Errors
    ///
    /// Returns the first [`TransportError`] encountered.
    pub fn validate(&self) -> Result<(), TransportError> {
        if self.magic != FRAME_MAGIC {
            return Err(TransportError::InvalidMagic);
        }
        if self.version < MIN_SUPPORTED_VERSION || self.version > CURRENT_VERSION {
            return Err(TransportError::InvalidVersion { got: self.version, min: MIN_SUPPORTED_VERSION, max: CURRENT_VERSION });
        }
        if self.payload_length as usize > MAX_FRAME_SIZE {
            return Err(TransportError::FrameTooLarge { size: self.payload_length as usize, max: MAX_FRAME_SIZE });
        }
        // Defense-in-depth: explicitly match every known frame type.
        // If a new variant is added to Frametype, this will fail to compile
        // until validate() is updated — preventing silent acceptance of unreviewed types.
        match self.frame_type {
            frame::frame::Frametype::Open
            | frame::frame::Frametype::Data
            | frame::frame::Frametype::Close
            | frame::frame::Frametype::Reset
            | frame::frame::Frametype::Ping
            | frame::frame::Frametype::Pong
            | frame::frame::Frametype::Window
            | frame::frame::Frametype::Error
            | frame::frame::Frametype::Settings
            | frame::frame::Frametype::Hello
            | frame::frame::Frametype::Welcome => {}
        }
        if self.flags & !(FLAG_FIN | FLAG_ACK | FLAG_CONTROL) != 0 {
            return Err(TransportError::InvalidFrame(format!("Unknown flags set: {:016b}", self.flags)));
        }
        // Validate FLAG_CONTROL consistency: control frames must have it set;
        // data-plane frames must not.
        let is_control_type = matches!(
            self.frame_type,
            frame::frame::Frametype::Ping
            | frame::frame::Frametype::Pong
            | frame::frame::Frametype::Settings
            | frame::frame::Frametype::Hello
            | frame::frame::Frametype::Welcome
        );
        if is_control_type && !self.is_control() {
            return Err(TransportError::InvalidFrame(format!(
                "{:?} frame must have FLAG_CONTROL set", self.frame_type
            )));
        }
        if !is_control_type && self.is_control() {
            return Err(TransportError::InvalidFrame(format!(
                "{:?} frame must not have FLAG_CONTROL set", self.frame_type
            )));
        }
        Ok(())
    }

    /// Construct a new [`Header`] with the supplied fields.
    ///
    /// [`magic`](Header::magic) is always set to [`FRAME_MAGIC`] and
    /// [`version`](Header::version) to [`CURRENT_VERSION`]; callers do not
    /// need to supply these values.
    pub fn new(frame_type: frame::frame::Frametype, flags: u16, stream_id: u32, payload_length: u32) -> Self {
        Header {
            magic: FRAME_MAGIC,
            version: CURRENT_VERSION,
            flags,
            stream_id,
            payload_length,
            frame_type,
        }
    }

}

