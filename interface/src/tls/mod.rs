//! Application-side TLS adapter for the transport's secure-stream seam.
//!
//! The transport crate knows nothing about TLS. This module supplies the
//! certificates, trust policy, and handshake for the example applications.
//! Only TLS 1.3 is offered, and client certificates are not requested.

pub mod pem;
pub mod provider;
pub mod trust;

pub use provider::{TlsClientProvider, TlsServerProvider};
pub use trust::parse_spki_pin;
