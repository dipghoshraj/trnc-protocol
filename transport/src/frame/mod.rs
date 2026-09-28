//! Wire-format framing for the TRNC binary protocol.
//!
//! This module contains all types and functions needed to construct, encode,
//! and decode TRNC frames:
//!
//! - [`frame`] — the [`frame::Frame`] container and [`frame::Frametype`] enum.
//! - [`header`] — the fixed 16-byte [`header::Header`] that precedes every frame,
//!   plus wire constants such as [`header::FRAME_MAGIC`] and [`header::MAX_FRAME_SIZE`].
//! - [`encoder`] — serialises a [`frame::Frame`] into a [`bytes::Bytes`] buffer
//!   ready to be written to a stream.
//! - [`decoder`] — deserialises a contiguous byte slice into a [`frame::Frame`],
//!   returning [`crate::errors::TransportError::NeedMoreData`] when more bytes
//!   are required to complete the frame.
pub mod header;
pub mod encoder;
pub mod decoder;
pub mod frame;