use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::RwLock;

use uuid::Uuid;

use super::credential::{NodeCredential, PublicKey};

/// Local store of node credentials and the issuer keys that are trusted to sign them.
pub trait TrustCache: Send + Sync + Debug {
    fn is_trusted(&self, node_id: &Uuid) -> bool;
    fn get(&self, node_id: &Uuid) -> Option<NodeCredential>;
    fn insert(&self, credential: NodeCredential);
    /// Public keys of the network issuers whose signatures are accepted.
    fn issuers(&self) -> Vec<PublicKey>;
}

#[derive(Debug, Default)]
pub struct InMemoryTrustCache {
    credentials: RwLock<HashMap<Uuid, NodeCredential>>,
    issuers: Vec<PublicKey>,
}

impl InMemoryTrustCache {
    pub fn new(issuers: Vec<PublicKey>) -> Self {
        Self {
            credentials: RwLock::new(HashMap::new()),
            issuers,
        }
    }
}

impl TrustCache for InMemoryTrustCache {
    fn is_trusted(&self, node_id: &Uuid) -> bool {
        self.credentials
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .contains_key(node_id)
    }

    fn get(&self, node_id: &Uuid) -> Option<NodeCredential> {
        self.credentials
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(node_id)
            .cloned()
    }

    fn insert(&self, credential: NodeCredential) {
        self.credentials
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(credential.node_id, credential);
    }

    fn issuers(&self) -> Vec<PublicKey> {
        self.issuers.clone()
    }
}
