//! Test-only helpers. Compiled only with the `test-support` feature.

use async_trait::async_trait;
use tokio::net::TcpStream;

use crate::{errors::TransportError, secure::SecureStreamProvider};

/// A [`SecureStreamProvider`] that performs no handshake and no encryption.
///
/// Use it to exercise protocol logic without TLS. Never use it in production.
#[derive(Debug, Default, Clone, Copy)]
pub struct PlaintextProvider;

#[async_trait]
impl SecureStreamProvider for PlaintextProvider {
    type Stream = TcpStream;

    async fn connect(&self, tcp: TcpStream, _server_name: &str) -> Result<TcpStream, TransportError> {
        Ok(tcp)
    }

    async fn accept(&self, tcp: TcpStream) -> Result<TcpStream, TransportError> {
        Ok(tcp)
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    use super::PlaintextProvider;
    use crate::secure::SecureStreamProvider;

    #[tokio::test]
    async fn plaintext_provider_round_trips_bytes() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;

        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await?;
            let mut stream = PlaintextProvider.accept(tcp).await?;
            let mut buf = [0u8; 5];
            stream.read_exact(&mut buf).await?;
            stream.write_all(&buf).await?;
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(buf)
        });

        let tcp = TcpStream::connect(addr).await?;
        let mut stream = PlaintextProvider.connect(tcp, "localhost").await?;
        stream.write_all(b"hello").await?;
        let mut echoed = [0u8; 5];
        stream.read_exact(&mut echoed).await?;

        assert_eq!(&echoed, b"hello");
        assert_eq!(&server.await??, b"hello");
        Ok(())
    }
}
