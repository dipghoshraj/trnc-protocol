//! End-to-end tests over real localhost TCP and TLS (plan §10).

use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use rustls_pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_rustls::TlsAcceptor;

use super::credential::generate_ed25519_pkcs8;
use super::endpoint::{ClientTls, ServerTls};
use super::identity::build_tls_certificate;
use super::server_config::build_server_config;
use super::test_support::{cache_trusting, issuer, node_identity};
use crate::client::resilient_client::ResilientClient;
use crate::errors::TransportError;
use crate::server::{Actions, Handler, RequestEnvelope, ResilientServer, ResponseEnvelope};

type ServerTask = JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>>;

struct EchoHandler;

#[async_trait]
impl Handler for EchoHandler {
    async fn call(&self, payload: Vec<u8>) -> Result<Vec<u8>, TransportError> {
        Ok(payload)
    }
}

fn echo_request(payload: &[u8]) -> RequestEnvelope {
    RequestEnvelope {
        action: "echo".to_string(),
        payload: payload.to_vec(),
    }
}

/// Starts a `ResilientServer` that handles one connection on an ephemeral localhost port.
async fn spawn_server(tls: ServerTls) -> std::io::Result<(SocketAddr, ServerTask)> {
    let mut actions = Actions::new();
    actions.register_action("echo", EchoHandler);
    let actions = Arc::new(actions);

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let task = tokio::spawn(async move {
        let (stream, peer) = listener.accept().await?;
        ResilientServer::new(stream, peer, actions, tls).run().await
    });
    Ok((addr, task))
}

#[tokio::test]
async fn connect_to_trusted_peer_succeeds() {
    let issuer = issuer();
    let trust = cache_trusting(&[&issuer]);
    let server_tls = ServerTls::new(Arc::new(node_identity(&issuer)), trust.clone()).unwrap();
    let client_tls = ClientTls::new(Arc::new(node_identity(&issuer)), trust).unwrap();

    let (addr, server) = spawn_server(server_tls).await.unwrap();
    let mut client = ResilientClient::new(addr.ip().to_string(), addr.port(), client_tls);
    client.build_stream().await.unwrap();

    let response: ResponseEnvelope = client.send_message(&echo_request(b"hello")).await.unwrap();
    assert_eq!(response.payload, b"hello".to_vec());

    client.close().await.unwrap();
    let _ = server.await;
}

#[tokio::test]
async fn untrusted_server_is_rejected() {
    let trusted = issuer();
    let rogue = issuer();
    // The server's credential comes from an issuer the client does not trust.
    let server_tls = ServerTls::new(Arc::new(node_identity(&rogue)), cache_trusting(&[&rogue])).unwrap();
    let client_tls = ClientTls::new(
        Arc::new(node_identity(&trusted)),
        cache_trusting(&[&trusted]),
    )
    .unwrap();

    let (addr, server) = spawn_server(server_tls).await.unwrap();
    let mut client = ResilientClient::new(addr.ip().to_string(), addr.port(), client_tls);
    assert!(client.build_stream().await.is_err());

    let _ = server.await;
}

#[tokio::test]
async fn untrusted_client_is_rejected() {
    let trusted = issuer();
    let rogue = issuer();
    let trust = cache_trusting(&[&trusted]);
    let server_tls = ServerTls::new(Arc::new(node_identity(&trusted)), trust).unwrap();
    // The client's TLS handshake succeeds, but its Hello credential is from an untrusted issuer.
    let client_tls = ClientTls::new(
        Arc::new(node_identity(&rogue)),
        cache_trusting(&[&trusted]),
    )
    .unwrap();

    let (addr, server) = spawn_server(server_tls).await.unwrap();
    let mut client = ResilientClient::new(addr.ip().to_string(), addr.port(), client_tls);
    client.build_stream().await.unwrap();

    let result: Result<ResponseEnvelope, _> = client.send_message(&echo_request(b"hello")).await;
    assert!(result.is_err());

    let _ = server.await;
}

/// An attacker terminates TLS with a certificate that copies a trusted node's credential but is
/// signed with the attacker's own key. The client must reject it because the certified key does
/// not match the credential.
#[tokio::test]
async fn mitm_proxy_is_rejected() {
    let trusted = issuer();
    let victim = node_identity(&trusted);

    let attacker_pkcs8 = generate_ed25519_pkcs8().unwrap();
    let forged_cert = build_tls_certificate(victim.credential(), &attacker_pkcs8).unwrap();
    let attacker_key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(attacker_pkcs8));
    let acceptor = TlsAcceptor::from(build_server_config(vec![forged_cert], attacker_key).unwrap());

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let proxy = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await?;
        acceptor.accept(tcp).await.map(|_| ())
    });

    let client_tls = ClientTls::new(Arc::new(node_identity(&trusted)), cache_trusting(&[&trusted])).unwrap();
    let mut client = ResilientClient::new(addr.ip().to_string(), addr.port(), client_tls);
    assert!(client.build_stream().await.is_err());

    let _ = proxy.await;
}
