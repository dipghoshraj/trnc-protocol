//! Shared fixtures for TLS unit and wire tests.

use std::sync::Arc;

use uuid::Uuid;

use super::credential::{
    NodeCredential, PublicKey, ed25519_public_key, generate_ed25519_pkcs8, now_unix_secs,
    parse_ed25519_pkcs8,
};
use super::identity::NodeIdentity;
use super::trust_cache::{InMemoryTrustCache, TrustCache};

pub(crate) struct Issuer {
    pub(crate) id: Uuid,
    pub(crate) pkcs8: Vec<u8>,
    pub(crate) public: PublicKey,
}

pub(crate) fn issuer() -> Issuer {
    let pkcs8 = generate_ed25519_pkcs8().unwrap();
    let public = ed25519_public_key(&pkcs8).unwrap();
    Issuer { id: Uuid::new_v4(), pkcs8, public }
}

pub(crate) fn issue(
    issuer: &Issuer,
    node_id: Uuid,
    public_key: PublicKey,
    issued_at: u64,
    expires_at: u64,
) -> NodeCredential {
    let key = parse_ed25519_pkcs8(&issuer.pkcs8).unwrap();
    NodeCredential::issue(node_id, public_key, issued_at, expires_at, issuer.id, &key).unwrap()
}

/// A node identity with a valid one-hour credential from `issuer`.
pub(crate) fn node_identity(issuer: &Issuer) -> NodeIdentity {
    let now = now_unix_secs();
    let pkcs8 = generate_ed25519_pkcs8().unwrap();
    let credential = issue(
        issuer,
        Uuid::new_v4(),
        ed25519_public_key(&pkcs8).unwrap(),
        now - 60,
        now + 3600,
    );
    NodeIdentity::new(credential, pkcs8).unwrap()
}

pub(crate) fn cache_trusting(issuers: &[&Issuer]) -> Arc<dyn TrustCache> {
    Arc::new(InMemoryTrustCache::new(issuers.iter().map(|i| i.public.clone()).collect()))
}
