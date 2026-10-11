use std::sync::Arc;

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::{CertificateError, DigitallySignedStruct, Error as RustlsError, SignatureScheme};
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};
use x509_parser::oid_registry::OID_SIG_ED25519;
use x509_parser::parse_x509_certificate;

use super::credential::NodeCredential;
use super::identity::CREDENTIAL_EXTENSION_OID;
use super::trust_cache::TrustCache;
use crate::errors::TransportError;

/// Verifies a peer's TLS certificate by its embedded [`NodeCredential`] instead of Web-PKI.
///
/// The hostname (SNI) is not checked. The node identity comes from the credential.
#[derive(Debug)]
pub struct CredentialVerifier {
    trust_cache: Arc<dyn TrustCache>,
    provider: Arc<CryptoProvider>,
}

impl CredentialVerifier {
    pub fn new(trust_cache: Arc<dyn TrustCache>, provider: Arc<CryptoProvider>) -> Self {
        Self { trust_cache, provider }
    }
}

impl ServerCertVerifier for CredentialVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, RustlsError> {
        if !intermediates.is_empty() {
            return Err(general("peer sent intermediate certificates; expected a single self-issued certificate"));
        }

        let credential = extract_credential(end_entity).map_err(to_rustls)?;
        check_spki_matches(end_entity, &credential).map_err(to_rustls)?;
        authorize_credential(&credential, self.trust_cache.as_ref(), now.as_secs()).map_err(to_rustls)?;
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![SignatureScheme::ED25519]
    }
}

/// Decides whether a credential is accepted.
///
/// A credential is accepted when it is inside its validity window and either matches the
/// trust cache exactly or is signed by a trusted issuer. Issuer-signed credentials are
/// cached on success.
pub(crate) fn authorize_credential(
    credential: &NodeCredential,
    trust_cache: &dyn TrustCache,
    now: u64,
) -> Result<(), TransportError> {
    credential.validate(now)?;

    if trust_cache.get(&credential.node_id).as_ref() == Some(credential) {
        return Ok(());
    }

    let trusted = trust_cache
        .issuers()
        .iter()
        .any(|issuer| credential.verify_signature(issuer).is_ok());
    if !trusted {
        return Err(TransportError::UntrustedPeer(credential.node_id));
    }

    trust_cache.insert(credential.clone());
    Ok(())
}

pub(crate) fn extract_credential(cert_der: &[u8]) -> Result<NodeCredential, TransportError> {
    let (_, cert) = parse_x509_certificate(cert_der)
        .map_err(|e| TransportError::InvalidCredential(format!("certificate parse failed: {e}")))?;

    let expected_oid = oid_string(CREDENTIAL_EXTENSION_OID);
    let mut found = cert
        .extensions()
        .iter()
        .filter(|ext| ext.oid.to_id_string() == expected_oid);

    let extension = found
        .next()
        .ok_or_else(|| TransportError::InvalidCredential("certificate has no node credential extension".into()))?;
    if found.next().is_some() {
        return Err(TransportError::InvalidCredential("certificate has multiple node credential extensions".into()));
    }

    NodeCredential::decode(extension.value)
}

fn check_spki_matches(cert_der: &[u8], credential: &NodeCredential) -> Result<(), TransportError> {
    let (_, cert) = parse_x509_certificate(cert_der)
        .map_err(|e| TransportError::InvalidCredential(format!("certificate parse failed: {e}")))?;
    let spki = cert.public_key();
    if spki.algorithm.algorithm != OID_SIG_ED25519 {
        return Err(TransportError::InvalidCredential("certificate key is not Ed25519".into()));
    }
    if spki.subject_public_key.data.as_ref() != credential.public_key.bytes.as_slice() {
        return Err(TransportError::InvalidCredential(
            "certificate public key does not match credential".into(),
        ));
    }
    Ok(())
}

fn oid_string(arcs: &[u64]) -> String {
    arcs.iter().map(u64::to_string).collect::<Vec<_>>().join(".")
}

fn general(msg: &str) -> RustlsError {
    RustlsError::General(msg.to_string())
}

fn to_rustls(err: TransportError) -> RustlsError {
    match err {
        TransportError::CredentialExpired(_) => RustlsError::InvalidCertificate(CertificateError::Expired),
        TransportError::TlsError(e) => e,
        other => RustlsError::General(other.to_string()),
    }
}
