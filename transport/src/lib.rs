//! # Transport
//!
//! Binary framing and connection management layer for TrenchDB.
//!
//! This crate provides:
//!
//! - **[`frame`]** — wire-format types, encoding, and decoding for the TRNC
//!   binary framing protocol.
//! - **[`tcp`]** — async TCP connection wrappers that send and receive
//!   [`frame::frame::Frame`] values over a [`tokio`] I/O stream.
//! - **[`errors`]** — the shared [`errors::TransportError`] type returned by
//!   every fallible operation in this crate.
//!
//! ## Wire format
//!
//! Every frame begins with a fixed 16-byte header:
//!
//! ```text
//! ┌───────────┬─────────┬───────┬───────────┬────────────────┬────────────┐
//! │  magic[4] │ ver [1] │ flags │ stream_id │ payload_length │ frame_type │
//! │  "TRNC"   │         │  [2]  │    [4]    │      [4]       │    [1]     │
//! └───────────┴─────────┴───────┴───────────┴────────────────┴────────────┘
//! ```
//!
//! Followed immediately by `payload_length` bytes of opaque payload.

pub mod frame;
pub mod errors;
pub mod tcp;
pub mod client;
pub mod server;
pub mod tls;