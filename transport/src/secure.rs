//! Secure-stream seam between TCP sockets and the framing layer.
//!
//! A [`SecureStreamProvider`] turns a freshly connected or accepted
//! [`TcpStream`] into a byte stream that [`crate::tcp::connection::Connection`]
//! can frame. The provider owns all security concerns (certificates, trust,
//! key exchange). This trait deliberately carries no configuration or
//! identity types, so `transport` stays free of TLS policy.
//!
//! Callers must treat `accept` and `connect` as the security gate: no frame
//! may be read or written until the returned stream is available.

use std::time::Duration;

use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;

use crate::errors::TransportError;

/// Upper bound on the secure handshake. Clients also include the protocol
/// Hello/Welcome exchange; servers apply it to the secure accept only.
pub const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

#[async_trait]
pub trait SecureStreamProvider: Send + Sync {
    /// The byte stream produced after the secure handshake completes.
    type Stream: AsyncRead + AsyncWrite + Unpin + Send + 'static;

    /// Runs the client side of the secure handshake over `tcp`.
    ///
    /// `server_name` is the peer name the client expects to see; providers
    /// that authenticate peers should verify against it.
    async fn connect(&self, tcp: TcpStream, server_name: &str) -> Result<Self::Stream, TransportError>;

    /// Runs the server side of the secure handshake over an accepted `tcp`.
    async fn accept(&self, tcp: TcpStream) -> Result<Self::Stream, TransportError>;
}
