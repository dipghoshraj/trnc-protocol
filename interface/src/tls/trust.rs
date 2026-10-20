use std::sync::Arc;

use rustls::{
    client::{
        danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
        WebPkiServerVerifier,
    },
    pki_types::{CertificateDer, ServerName, UnixTime},
    CertificateError, DigitallySignedStruct, DistinguishedName, Error, RootCertStore,
    SignatureScheme,
};
use sha2::{Digest, Sha256};
use x509_parser::prelude::FromDer;

use super::pem::BoxError;

/// Verifies the server chain and name through WebPKI, then optionally pins
/// the SHA-256 of the leaf's SubjectPublicKeyInfo.
#[derive(Debug)]
pub(crate) struct PinnedWebPkiVerifier {
    inner: Arc<WebPkiServerVerifier>,
    spki_pin: Option<[u8; 32]>,
}

impl PinnedWebPkiVerifier {
    pub(crate) fn new(
        roots: Vec<CertificateDer<'static>>,
        spki_pin: Option<[u8; 32]>,
    ) -> Result<Self, BoxError> {
        if roots.is_empty() {
            return Err("client trust requires at least one root CA certificate".into());
        }
        let mut store = RootCertStore::empty();
        for root in roots {
            store.add(root)?;
        }
        let inner = WebPkiServerVerifier::builder(Arc::new(store)).build()?;
        Ok(Self { inner, spki_pin })
    }
}

impl ServerCertVerifier for PinnedWebPkiVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, Error> {
        let verified = self
            .inner
            .verify_server_cert(end_entity, intermediates, server_name, ocsp_response, now)?;

        if let Some(pin) = &self.spki_pin {
            let (_, cert) = x509_parser::certificate::X509Certificate::from_der(end_entity)
                .map_err(|_| Error::InvalidCertificate(CertificateError::BadEncoding))?;
            let digest: [u8; 32] = Sha256::digest(cert.public_key().raw).into();
            if &digest != pin {
                return Err(Error::InvalidCertificate(
                    CertificateError::ApplicationVerificationFailure,
                ));
            }
        }

        Ok(verified)
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.inner.supported_verify_schemes()
    }

    fn root_hint_subjects(&self) -> Option<&[DistinguishedName]> {
        self.inner.root_hint_subjects()
    }
}

/// Parses a 64-character hex SHA-256 digest, e.g. from `openssl dgst -sha256`.
pub fn parse_spki_pin(hex: &str) -> Result<[u8; 32], BoxError> {
    let hex = hex.trim();
    if hex.len() != 64 || !hex.is_ascii() {
        return Err(format!("SPKI pin must be 64 hex characters, got {hex:?}").into());
    }
    let mut out = [0u8; 32];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[2 * i..2 * i + 2], 16)
            .map_err(|_| format!("SPKI pin contains non-hex characters: {hex}"))?;
    }
    Ok(out)
}
