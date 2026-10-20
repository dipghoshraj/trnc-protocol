use std::sync::Arc;

use async_trait::async_trait;
use rustls::{
    pki_types::{CertificateDer, PrivateKeyDer, ServerName},
    version::TLS13,
    ClientConfig, ServerConfig,
};
use tokio::net::TcpStream;
use tokio_rustls::{client, server, TlsAcceptor, TlsConnector};
use transport::{errors::TransportError, secure::SecureStreamProvider};

use super::{pem::BoxError, trust::PinnedWebPkiVerifier};

/// Client side: verifies the server's chain, name, and optional SPKI pin
/// before any protocol bytes are sent.
pub struct TlsClientProvider {
    connector: TlsConnector,
}

impl TlsClientProvider {
    /// `roots` is the trusted CA set; it must not be empty.
    pub fn new(
        roots: Vec<CertificateDer<'static>>,
        spki_pin: Option<[u8; 32]>,
    ) -> Result<Self, BoxError> {
        let verifier = PinnedWebPkiVerifier::new(roots, spki_pin)?;
        let config = ClientConfig::builder_with_protocol_versions(&[&TLS13])
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(verifier))
            .with_no_client_auth();
        Ok(Self {
            connector: TlsConnector::from(Arc::new(config)),
        })
    }
}

#[async_trait]
impl SecureStreamProvider for TlsClientProvider {
    type Stream = client::TlsStream<TcpStream>;

    async fn connect(&self, tcp: TcpStream, server_name: &str) -> Result<Self::Stream, TransportError> {
        let name = ServerName::try_from(server_name.to_owned())
            .map_err(|e| TransportError::SecureHandshake(format!("invalid server name {server_name:?}: {e}")))?;
        self.connector
            .connect(name, tcp)
            .await
            .map_err(|e| TransportError::SecureHandshake(e.to_string()))
    }

    async fn accept(&self, _tcp: TcpStream) -> Result<Self::Stream, TransportError> {
        Err(TransportError::SecureHandshake(
            "client TLS provider cannot accept connections".to_string(),
        ))
    }
}

/// Server side: presents a certificate chain and key over TLS 1.3.
pub struct TlsServerProvider {
    acceptor: TlsAcceptor,
}

impl TlsServerProvider {
    pub fn new(
        certs: Vec<CertificateDer<'static>>,
        key: PrivateKeyDer<'static>,
    ) -> Result<Self, BoxError> {
        let config = ServerConfig::builder_with_protocol_versions(&[&TLS13])
            .with_no_client_auth()
            .with_single_cert(certs, key)?;
        Ok(Self {
            acceptor: TlsAcceptor::from(Arc::new(config)),
        })
    }
}

#[async_trait]
impl SecureStreamProvider for TlsServerProvider {
    type Stream = server::TlsStream<TcpStream>;

    async fn connect(&self, _tcp: TcpStream, _server_name: &str) -> Result<Self::Stream, TransportError> {
        Err(TransportError::SecureHandshake(
            "server TLS provider cannot initiate connections".to_string(),
        ))
    }

    async fn accept(&self, tcp: TcpStream) -> Result<Self::Stream, TransportError> {
        self.acceptor
            .accept(tcp)
            .await
            .map_err(|e| TransportError::SecureHandshake(e.to_string()))
    }
}
