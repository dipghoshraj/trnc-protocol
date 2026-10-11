//! TLS settings for the connection endpoints: the local node identity plus the TLS connector or
//! acceptor built from it.

use std::fmt;
use std::sync::Arc;

use tokio_rustls::{TlsAcceptor, TlsConnector};

use super::client_config::build_client_config;
use super::identity::NodeIdentity;
use super::server_config::build_server_config;
use super::trust_cache::TrustCache;
use crate::errors::TransportError;

/// Client-side TLS settings used by [`ResilientClient`](crate::client::resilient_client::ResilientClient).
#[derive(Clone)]
pub struct ClientTls {
    pub(crate) identity: Arc<NodeIdentity>,
    pub(crate) trust_cache: Arc<dyn TrustCache>,
    pub(crate) connector: TlsConnector,
}

impl ClientTls {
    pub fn new(
        identity: Arc<NodeIdentity>,
        trust_cache: Arc<dyn TrustCache>,
    ) -> Result<Self, TransportError> {
        let config = build_client_config(trust_cache.clone()).map_err(TransportError::TlsError)?;
        Ok(Self {
            identity,
            trust_cache,
            connector: TlsConnector::from(config),
        })
    }
}

impl fmt::Debug for ClientTls {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientTls")
            .field("identity", &self.identity)
            .finish_non_exhaustive()
    }
}

/// Server-side TLS settings used by [`ResilientServer`](crate::server::resilient_server::ResilientServer).
///
/// The server presents the node's own TLS certificate, which embeds its credential.
#[derive(Clone)]
pub struct ServerTls {
    pub(crate) identity: Arc<NodeIdentity>,
    pub(crate) trust_cache: Arc<dyn TrustCache>,
    pub(crate) acceptor: TlsAcceptor,
}

impl ServerTls {
    pub fn new(
        identity: Arc<NodeIdentity>,
        trust_cache: Arc<dyn TrustCache>,
    ) -> Result<Self, TransportError> {
        let config = build_server_config(
            vec![identity.tls_certificate()],
            identity.tls_private_key(),
        )
        .map_err(TransportError::TlsError)?;
        Ok(Self {
            identity,
            trust_cache,
            acceptor: TlsAcceptor::from(config),
        })
    }
}

impl fmt::Debug for ServerTls {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServerTls")
            .field("identity", &self.identity)
            .finish_non_exhaustive()
    }
}
