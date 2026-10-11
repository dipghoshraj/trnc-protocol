use std::io;
use std::sync::Arc;
use rustls::crypto::aws_lc_rs::default_provider;
use rustls::version::TLS13;
use rustls::{ServerConfig, Error as RustlsError};
use rustls_pki_types::{CertificateDer, PrivateKeyDer};

/// Builds a TLS 1.3 server config without client authentication.
/// The node's identity is proven at the application layer (Hello), not in TLS.
pub fn build_server_config(
    certs: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
) -> Result<Arc<ServerConfig>, RustlsError> {
    let config = ServerConfig::builder_with_provider(Arc::new(default_provider()))
        .with_protocol_versions(&[&TLS13])?
        .with_no_client_auth()
        .with_single_cert(certs, key)?;
    Ok(Arc::new(config))
}

pub fn load_server_config(cert_path: &str, key_path: &str) -> io::Result<Arc<ServerConfig>> {
    let certs = crate::tls::pem::load_certs(cert_path)?;
    let key = crate::tls::pem::load_private_key(key_path)?;
    build_server_config(certs, key).map_err(io::Error::other)
}
