use std::sync::Arc;

use rustls::client::danger::ServerCertVerifier;
use rustls_pki_types::{ServerName, UnixTime};
use tokio::io::duplex;
use uuid::Uuid;

use super::credential::{ed25519_public_key, generate_ed25519_pkcs8, now_unix_secs};
use super::hello::{decode_hello, encode_hello};
use super::identity::NodeIdentity;
use super::test_support::{cache_trusting, issue, issuer, node_identity};
use super::trust_cache::{InMemoryTrustCache, TrustCache};
use super::verifier::{CredentialVerifier, authorize_credential};
use crate::errors::TransportError;
use crate::tcp::connection::Connection;
use crate::tcp::manager::{Role, SessionAuth, StreamManager};

const SESSION_SECRET: [u8; 32] = [7; 32];

fn session_auth(identity: Arc<NodeIdentity>, trust_cache: Arc<dyn TrustCache>, secret: [u8; 32]) -> SessionAuth {
    SessionAuth {
        identity,
        trust_cache,
        session_secret: secret,
        tls_peer: None,
    }
}

#[test]
fn hello_payload_round_trips_and_rejects_malformed_input() {
    let iss = issuer();
    let node = node_identity(&iss);
    let proof = node.sign(&SESSION_SECRET);

    let payload = encode_hello(node.credential(), &proof).unwrap();
    let (credential, decoded_proof) = decode_hello(&payload).unwrap();
    assert_eq!(&credential, node.credential());
    assert_eq!(decoded_proof, proof);

    assert!(decode_hello(&[]).is_err());
    assert!(decode_hello(&payload[..payload.len() - 1]).is_err());
    let mut trailing = payload.clone();
    trailing.push(0);
    assert!(decode_hello(&trailing).is_err());
}

#[test]
fn expired_credential_is_rejected() {
    let iss = issuer();
    let node = generate_ed25519_pkcs8().unwrap();
    let credential = issue(&iss, Uuid::new_v4(), ed25519_public_key(&node).unwrap(), 1, 1000);
    let cache = InMemoryTrustCache::new(vec![iss.public.clone()]);

    let result = authorize_credential(&credential, &cache, 2000);
    assert!(matches!(result, Err(TransportError::CredentialExpired(id)) if id == credential.node_id));
    assert!(cache.get(&credential.node_id).is_none());
}

#[test]
fn credential_from_untrusted_issuer_is_rejected_and_not_cached() {
    let trusted = issuer();
    let other = issuer();
    let node = generate_ed25519_pkcs8().unwrap();
    let now = now_unix_secs();
    let credential = issue(&other, Uuid::new_v4(), ed25519_public_key(&node).unwrap(), now, now + 3600);
    let cache = InMemoryTrustCache::new(vec![trusted.public.clone()]);

    let result = authorize_credential(&credential, &cache, now);
    assert!(matches!(result, Err(TransportError::UntrustedPeer(_))));
    assert!(cache.get(&credential.node_id).is_none());
}

#[test]
fn issuer_signed_credential_is_cached_on_first_use() {
    let iss = issuer();
    let node = generate_ed25519_pkcs8().unwrap();
    let now = now_unix_secs();
    let credential = issue(&iss, Uuid::new_v4(), ed25519_public_key(&node).unwrap(), now, now + 3600);
    let cache = InMemoryTrustCache::new(vec![iss.public.clone()]);

    authorize_credential(&credential, &cache, now).unwrap();
    assert_eq!(cache.get(&credential.node_id), Some(credential.clone()));
    authorize_credential(&credential, &cache, now).unwrap();
}

#[test]
fn verifier_accepts_identity_certificate_from_trusted_issuer() {
    let iss = issuer();
    let node = node_identity(&iss);
    let verifier = CredentialVerifier::new(
        cache_trusting(&[&iss]),
        Arc::new(rustls::crypto::aws_lc_rs::default_provider()),
    );
    let name = ServerName::try_from("localhost").unwrap();

    let result = verifier.verify_server_cert(&node.tls_certificate(), &[], &name, &[], UnixTime::now());
    assert!(result.is_ok(), "{result:?}");
}

#[test]
fn verifier_rejects_identity_certificate_from_untrusted_issuer() {
    let iss = issuer();
    let other = issuer();
    let node = node_identity(&iss);
    let verifier = CredentialVerifier::new(
        cache_trusting(&[&other]),
        Arc::new(rustls::crypto::aws_lc_rs::default_provider()),
    );
    let name = ServerName::try_from("localhost").unwrap();

    let result = verifier.verify_server_cert(&node.tls_certificate(), &[], &name, &[], UnixTime::now());
    assert!(result.is_err());
}

async fn run_handshake(
    initiator_auth: Option<SessionAuth>,
    acceptor_auth: SessionAuth,
) -> (Result<(), TransportError>, Result<(), TransportError>, Option<Uuid>) {
    let (a, b) = duplex(64 * 1024);
    let mut initiator = match initiator_auth {
        Some(auth) => StreamManager::with_auth(Connection::new(a), Role::Initiator, auth),
        None => StreamManager::new(Connection::new(a), Role::Initiator),
    };
    let mut acceptor = StreamManager::with_auth(Connection::new(b), Role::Acceptor, acceptor_auth);

    let (client_result, server_result) = tokio::join!(initiator.start_handshake(), acceptor.recv_frame());
    let peer = acceptor.peer_identity().map(|p| p.node_id);
    (client_result, server_result.map(|_| ()), peer)
}

#[tokio::test]
async fn hello_handshake_accepts_authenticated_initiator() {
    let iss = issuer();
    let client = Arc::new(node_identity(&iss));
    let server = Arc::new(node_identity(&iss));
    let trust = cache_trusting(&[&iss]);

    let (client_result, server_result, peer) = run_handshake(
        Some(session_auth(client.clone(), trust.clone(), SESSION_SECRET)),
        session_auth(server, trust, SESSION_SECRET),
    )
    .await;

    assert!(client_result.is_ok(), "{client_result:?}");
    assert!(server_result.is_ok(), "{server_result:?}");
    assert_eq!(peer, Some(client.node_id()));
}

#[tokio::test]
async fn hello_handshake_rejects_proof_for_a_different_session() {
    let iss = issuer();
    let client = Arc::new(node_identity(&iss));
    let server = Arc::new(node_identity(&iss));
    let trust = cache_trusting(&[&iss]);

    let (client_result, server_result, peer) = run_handshake(
        Some(session_auth(client, trust.clone(), [8; 32])),
        session_auth(server, trust, SESSION_SECRET),
    )
    .await;

    assert!(matches!(server_result, Err(TransportError::HandshakeRejected { .. })));
    assert!(matches!(client_result, Err(TransportError::HandshakeRejected { .. })));
    assert!(peer.is_none());
}

#[tokio::test]
async fn hello_handshake_rejects_initiator_from_untrusted_issuer() {
    let iss = issuer();
    let other = issuer();
    let client = Arc::new(node_identity(&other));
    let server = Arc::new(node_identity(&iss));

    let (client_result, server_result, peer) = run_handshake(
        Some(session_auth(client, cache_trusting(&[&other]), SESSION_SECRET)),
        session_auth(server, cache_trusting(&[&iss]), SESSION_SECRET),
    )
    .await;

    assert!(matches!(server_result, Err(TransportError::HandshakeRejected { .. })));
    assert!(matches!(client_result, Err(TransportError::HandshakeRejected { .. })));
    assert!(peer.is_none());
}

#[tokio::test]
async fn hello_handshake_rejects_unauthenticated_hello() {
    let iss = issuer();
    let server = Arc::new(node_identity(&iss));

    let (client_result, server_result, peer) = run_handshake(
        None,
        session_auth(server, cache_trusting(&[&iss]), SESSION_SECRET),
    )
    .await;

    assert!(matches!(server_result, Err(TransportError::HandshakeRejected { .. })));
    assert!(matches!(client_result, Err(TransportError::HandshakeRejected { .. })));
    assert!(peer.is_none());
}
