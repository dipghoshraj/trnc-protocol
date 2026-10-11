//! Node credentials: an issuer-signed binding of a node's UUID to its Ed25519 public key.

use aws_lc_rs::rand::SystemRandom;
use aws_lc_rs::signature::{ED25519, Ed25519KeyPair, KeyPair as _, UnparsedPublicKey};
use uuid::Uuid;

use crate::errors::TransportError;

/// Version of the credential wire format.
pub const CREDENTIAL_VERSION: u8 = 1;

const ED25519_PUBLIC_KEY_LEN: usize = 32;
const KEY_ALGORITHM_ED25519: u8 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyAlgorithm {
    Ed25519,
}

impl KeyAlgorithm {
    fn wire_id(self) -> u8 {
        match self {
            KeyAlgorithm::Ed25519 => KEY_ALGORITHM_ED25519,
        }
    }

    fn from_wire_id(id: u8) -> Result<Self, TransportError> {
        match id {
            KEY_ALGORITHM_ED25519 => Ok(KeyAlgorithm::Ed25519),
            other => Err(invalid(format!("unknown key algorithm id {other}"))),
        }
    }

    fn public_key_len(self) -> usize {
        match self {
            KeyAlgorithm::Ed25519 => ED25519_PUBLIC_KEY_LEN,
        }
    }

    fn verification_algorithm(self) -> &'static dyn aws_lc_rs::signature::VerificationAlgorithm {
        match self {
            KeyAlgorithm::Ed25519 => &ED25519,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicKey {
    pub algorithm: KeyAlgorithm,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeCredential {
    /// Stable node identity. Must be a UUID v4.
    pub node_id: Uuid,
    pub public_key: PublicKey,
    /// Unix seconds.
    pub issued_at: u64,
    /// Unix seconds. The credential is invalid at and after this time.
    pub expires_at: u64,
    pub issuer_id: Uuid,
    /// Issuer's Ed25519 signature over [`NodeCredential::canonical_bytes`].
    pub signature: Vec<u8>,
}

impl NodeCredential {
    /// Creates a credential for `public_key` and signs it with the issuer's key.
    pub fn issue(
        node_id: Uuid,
        public_key: PublicKey,
        issued_at: u64,
        expires_at: u64,
        issuer_id: Uuid,
        issuer_key: &Ed25519KeyPair,
    ) -> Result<Self, TransportError> {
        let mut credential = Self {
            node_id,
            public_key,
            issued_at,
            expires_at,
            issuer_id,
            signature: Vec::new(),
        };
        let message = credential.canonical_bytes()?;
        credential.signature = issuer_key.sign(&message).as_ref().to_vec();
        Ok(credential)
    }

    /// Returns the bytes covered by the issuer signature: every field except the signature.
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, TransportError> {
        let key_len = u16::try_from(self.public_key.bytes.len())
            .map_err(|_| invalid("public key too long"))?;

        let mut out = Vec::with_capacity(64 + self.public_key.bytes.len());
        out.push(CREDENTIAL_VERSION);
        out.extend_from_slice(self.node_id.as_bytes());
        out.push(self.public_key.algorithm.wire_id());
        out.extend_from_slice(&key_len.to_be_bytes());
        out.extend_from_slice(&self.public_key.bytes);
        out.extend_from_slice(&self.issued_at.to_be_bytes());
        out.extend_from_slice(&self.expires_at.to_be_bytes());
        out.extend_from_slice(self.issuer_id.as_bytes());
        Ok(out)
    }

    /// Encodes the credential as `canonical_bytes || u16 signature length || signature`.
    pub fn encode(&self) -> Result<Vec<u8>, TransportError> {
        let sig_len = u16::try_from(self.signature.len())
            .map_err(|_| invalid("signature too long"))?;
        let mut out = self.canonical_bytes()?;
        out.extend_from_slice(&sig_len.to_be_bytes());
        out.extend_from_slice(&self.signature);
        Ok(out)
    }

    /// Decodes a credential produced by [`NodeCredential::encode`]. Rejects trailing bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, TransportError> {
        let mut reader = Reader::new(bytes);

        let version = reader.u8()?;
        if version != CREDENTIAL_VERSION {
            return Err(invalid(format!("unsupported credential version {version}")));
        }
        let node_id = Uuid::from_bytes(reader.array()?);
        let algorithm = KeyAlgorithm::from_wire_id(reader.u8()?)?;
        let key_len = reader.u16()? as usize;
        let key_bytes = reader.take(key_len)?.to_vec();
        let issued_at = reader.u64()?;
        let expires_at = reader.u64()?;
        let issuer_id = Uuid::from_bytes(reader.array()?);
        let sig_len = reader.u16()? as usize;
        let signature = reader.take(sig_len)?.to_vec();
        reader.finish()?;

        Ok(Self {
            node_id,
            public_key: PublicKey { algorithm, bytes: key_bytes },
            issued_at,
            expires_at,
            issuer_id,
            signature,
        })
    }

    /// Checks the signature against an issuer's public key.
    pub fn verify_signature(&self, issuer: &PublicKey) -> Result<(), TransportError> {
        let message = self.canonical_bytes()?;
        UnparsedPublicKey::new(issuer.algorithm.verification_algorithm(), &issuer.bytes)
            .verify(&message, &self.signature)
            .map_err(|_| invalid("signature does not verify against trusted issuer"))
    }

    /// Checks that `signature` was produced by this credential's private key over `message`.
    pub fn verify_possession(&self, message: &[u8], signature: &[u8]) -> Result<(), TransportError> {
        UnparsedPublicKey::new(self.public_key.algorithm.verification_algorithm(), &self.public_key.bytes)
            .verify(message, signature)
            .map_err(|_| invalid("proof of key possession does not verify"))
    }

    /// Checks structure and lifetime: UUID v4 node id, key length, validity window.
    pub fn validate(&self, now: u64) -> Result<(), TransportError> {
        if self.node_id.get_version_num() != 4 {
            return Err(invalid("node_id must be a UUID v4"));
        }
        if self.public_key.bytes.len() != self.public_key.algorithm.public_key_len() {
            return Err(invalid("public key length does not match its algorithm"));
        }
        if self.issued_at > self.expires_at {
            return Err(invalid("issued_at is after expires_at"));
        }
        if now >= self.expires_at {
            return Err(TransportError::CredentialExpired(self.node_id));
        }
        Ok(())
    }
}

/// Current time in Unix seconds.
pub fn now_unix_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Generates a new Ed25519 key as PKCS#8 DER.
pub fn generate_ed25519_pkcs8() -> Result<Vec<u8>, TransportError> {
    let doc = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new())
        .map_err(|_| TransportError::InternalError("Ed25519 key generation failed".into()))?;
    Ok(doc.as_ref().to_vec())
}

/// Parses a PKCS#8 Ed25519 key.
pub fn parse_ed25519_pkcs8(pkcs8: &[u8]) -> Result<Ed25519KeyPair, TransportError> {
    Ed25519KeyPair::from_pkcs8(pkcs8).map_err(|_| invalid("malformed Ed25519 PKCS#8 key"))
}

/// Returns the public key for a PKCS#8 Ed25519 key.
pub fn ed25519_public_key(pkcs8: &[u8]) -> Result<PublicKey, TransportError> {
    let key = parse_ed25519_pkcs8(pkcs8)?;
    Ok(PublicKey {
        algorithm: KeyAlgorithm::Ed25519,
        bytes: key.public_key().as_ref().to_vec(),
    })
}

fn invalid(msg: impl Into<String>) -> TransportError {
    TransportError::InvalidCredential(msg.into())
}

struct Reader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], TransportError> {
        let end = self.pos.checked_add(n).filter(|&end| end <= self.data.len());
        match end {
            Some(end) => {
                let slice = &self.data[self.pos..end];
                self.pos = end;
                Ok(slice)
            }
            None => Err(invalid("credential truncated")),
        }
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], TransportError> {
        let mut out = [0u8; N];
        out.copy_from_slice(self.take(N)?);
        Ok(out)
    }

    fn u8(&mut self) -> Result<u8, TransportError> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, TransportError> {
        Ok(u16::from_be_bytes(self.array()?))
    }

    fn u64(&mut self) -> Result<u64, TransportError> {
        Ok(u64::from_be_bytes(self.array()?))
    }

    fn finish(&self) -> Result<(), TransportError> {
        if self.pos == self.data.len() {
            Ok(())
        } else {
            Err(invalid("trailing bytes after credential"))
        }
    }
}
