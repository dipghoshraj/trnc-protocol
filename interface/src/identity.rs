use std::error::Error;
use std::path::PathBuf;
use std::sync::Arc;

use transport::tls::credential::{KeyAlgorithm, NodeCredential, PublicKey};
use transport::tls::endpoint::{ClientTls, ServerTls};
use transport::tls::identity::NodeIdentity;
use transport::tls::trust_cache::{InMemoryTrustCache, TrustCache};

/// Files that give a node its TLS identity. Flattened into each binary's CLI.
#[derive(Debug, clap::Args)]
pub struct IdentityPaths {
    /// Encoded node credential, as produced by `NodeCredential::encode`.
    #[arg(long)]
    pub credential: PathBuf,
    /// The node's Ed25519 private key in PKCS#8 DER form.
    #[arg(long)]
    pub key: PathBuf,
    /// Raw 32-byte Ed25519 public key of a trusted issuer. Repeat for multiple issuers.
    #[arg(long = "trusted-issuer", required = true)]
    pub trusted_issuers: Vec<PathBuf>,
}

impl IdentityPaths {
    fn load(&self) -> Result<(Arc<NodeIdentity>, Arc<dyn TrustCache>), Box<dyn Error>> {
        let credential = NodeCredential::decode(&std::fs::read(&self.credential)?)?;
        let pkcs8 = std::fs::read(&self.key)?;
        let identity = Arc::new(NodeIdentity::new(credential, pkcs8)?);

        let mut issuers = Vec::with_capacity(self.trusted_issuers.len());
        for path in &self.trusted_issuers {
            let bytes = std::fs::read(path)?;
            if bytes.len() != 32 {
                return Err(format!(
                    "trusted issuer key {} must be 32 bytes, found {}",
                    path.display(),
                    bytes.len()
                )
                .into());
            }
            issuers.push(PublicKey {
                algorithm: KeyAlgorithm::Ed25519,
                bytes,
            });
        }
        let trust: Arc<dyn TrustCache> = Arc::new(InMemoryTrustCache::new(issuers));
        Ok((identity, trust))
    }

    pub fn client_tls(&self) -> Result<ClientTls, Box<dyn Error>> {
        let (identity, trust) = self.load()?;
        Ok(ClientTls::new(identity, trust)?)
    }

    pub fn server_tls(&self) -> Result<ServerTls, Box<dyn Error>> {
        let (identity, trust) = self.load()?;
        Ok(ServerTls::new(identity, trust)?)
    }
}
