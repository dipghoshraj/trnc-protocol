use std::sync::Arc;

use rustls::ClientConfig;
use rustls::crypto::aws_lc_rs::default_provider;
use rustls::version::TLS13;

use super::trust_cache::TrustCache;
use super::verifier::CredentialVerifier;

/// Builds a TLS 1.3 client config. Servers are verified by credential, not by hostname.
pub fn build_client_config(
    trust_cache: Arc<dyn TrustCache>,
) -> Result<Arc<ClientConfig>, rustls::Error> {
    let provider = Arc::new(default_provider());
    let verifier = Arc::new(CredentialVerifier::new(trust_cache, provider.clone()));

    let config = ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&TLS13])?
        .dangerous()
        .with_custom_certificate_verifier(verifier)
        .with_no_client_auth();
    Ok(Arc::new(config))
}
