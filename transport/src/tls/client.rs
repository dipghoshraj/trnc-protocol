use std::sync::Arc;
use rustls::ServerConfig;
use crate::tls::pem::{load_certs, load_private_key};

pub fn build_server_config(
    cert_path: &str,
    key_path: &str,
) -> Arc<ServerConfig> {
    let certs = load_certs(cert_path);
    let key = load_private_key(key_path);

    ServerConfig::builder()
        .with_no_client_auth() // <-- Standard TLS: client cert is NOT required
        .with_single_cert(certs, key)
        .expect("bad server certificate/key")
        .into()
}
