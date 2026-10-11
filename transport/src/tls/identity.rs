use aws_lc_rs::digest::{SHA256, digest};
use aws_lc_rs::signature::Ed25519KeyPair;
use rcgen::{CertificateParams, CustomExtension, DnType, KeyPair, PKCS_ED25519};
use rustls_pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use uuid::Uuid;

use super::credential::{NodeCredential, ed25519_public_key, parse_ed25519_pkcs8};
use crate::errors::TransportError;

/// X.509 extension OID carrying the encoded [`NodeCredential`] inside a node certificate.
///
/// PLACEHOLDER: this arc (1.3.6.1.4.1.55555) is not registered to this project. Replace it
/// with an assigned private-enterprise OID before deployment.
pub const CREDENTIAL_EXTENSION_OID: &[u64] = &[1, 3, 6, 1, 4, 1, 55555, 1];

/// A node's long-term identity: its credential and the matching Ed25519 private key.
pub struct NodeIdentity {
    credential: NodeCredential,
    pkcs8: Vec<u8>,
    key_pair: Ed25519KeyPair,
    tls_certificate: CertificateDer<'static>,
}

impl NodeIdentity {
    /// Builds an identity, checking that `pkcs8` is the key the credential certifies.
    pub fn new(credential: NodeCredential, pkcs8: Vec<u8>) -> Result<Self, TransportError> {
        let key_pair = parse_ed25519_pkcs8(&pkcs8)?;
        let derived = ed25519_public_key(&pkcs8)?;
        if derived != credential.public_key {
            return Err(TransportError::InvalidCredential(
                "private key does not match credential public key".into(),
            ));
        }
        let tls_certificate = build_tls_certificate(&credential, &pkcs8)?;
        Ok(Self {
            credential,
            pkcs8,
            key_pair,
            tls_certificate,
        })
    }

    pub fn node_id(&self) -> Uuid {
        self.credential.node_id
    }

    pub fn credential(&self) -> &NodeCredential {
        &self.credential
    }

    /// Signs `message` with the node's Ed25519 key.
    pub fn sign(&self, message: &[u8]) -> Vec<u8> {
        self.key_pair.sign(message).as_ref().to_vec()
    }

    /// The self-signed TLS certificate that embeds this node's credential.
    pub fn tls_certificate(&self) -> CertificateDer<'static> {
        self.tls_certificate.clone()
    }

    /// The TLS private key. It is the same Ed25519 key as the node identity.
    pub fn tls_private_key(&self) -> PrivateKeyDer<'static> {
        PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(self.pkcs8.clone()))
    }
}

impl std::fmt::Debug for NodeIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeIdentity")
            .field("node_id", &self.credential.node_id)
            .finish_non_exhaustive()
    }
}

/// An authenticated remote node, as seen by the local side of a connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerIdentity {
    pub node_id: Uuid,
    /// SHA-256 of the peer's DER-encoded TLS certificate. `None` when the peer
    /// did not present one (the acceptor side, since client auth is not used).
    pub cert_fingerprint: Option<[u8; 32]>,
}

/// SHA-256 of a DER-encoded certificate.
pub fn cert_fingerprint(der: &[u8]) -> [u8; 32] {
    let mut out = [0u8; 32];
    out.copy_from_slice(digest(&SHA256, der).as_ref());
    out
}

pub(crate) fn build_tls_certificate(
    credential: &NodeCredential,
    pkcs8: &[u8],
) -> Result<CertificateDer<'static>, TransportError> {
    let key_der = PrivatePkcs8KeyDer::from(pkcs8.to_vec());
    let key_pair = KeyPair::from_pkcs8_der_and_sign_algo(&key_der, &PKCS_ED25519)
        .map_err(|e| TransportError::InternalError(format!("TLS key import failed: {e}")))?;

    let mut params = CertificateParams::new(Vec::<String>::new())
        .map_err(|e| TransportError::InternalError(format!("certificate params: {e}")))?;
    params
        .distinguished_name
        .push(DnType::CommonName, credential.node_id.to_string());

    let mut extension = CustomExtension::from_oid_content(CREDENTIAL_EXTENSION_OID, credential.encode()?);
    extension.set_criticality(false);
    params.custom_extensions.push(extension);

    let certificate = params
        .self_signed(&key_pair)
        .map_err(|e| TransportError::InternalError(format!("certificate signing failed: {e}")))?;
    Ok(certificate.der().clone())
}
