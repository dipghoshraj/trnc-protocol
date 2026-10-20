//! Invalid TLS peers must fail during the secure handshake. The protocol handler
//! must never run, and a rejected client must not receive protocol bytes.

mod common;

use std::{net::SocketAddr, sync::atomic::Ordering, time::Duration};

use common::{
    assert_echo_over_reused_connection, spawn_echo_server, EchoServer,
    pki::{
        client_provider, self_signed_leaf, server_provider, Leaf, TestCa, CURRENT, EXPIRED,
        NOT_YET_VALID,
    },
};
use interface::tls::TlsClientProvider;
use rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::timeout,
};
use tokio_rustls::TlsAcceptor;
use transport::{
    client::resilient_client::ResilientClient, secure::SecureStreamProvider,
    testing::PlaintextProvider,
};

const NAMES: &[&str] = &["localhost", "127.0.0.1"];

/// Asserts the client fails to build a stream and the handler never ran.
async fn assert_client_rejected<C: SecureStreamProvider>(
    server: &EchoServer,
    provider: C,
    host: &str,
) {
    let mut client = ResilientClient::new(provider, host.to_string(), server.addr.port());
    assert!(
        client.build_stream().await.is_err(),
        "client accepted a peer it should have rejected"
    );
    assert_eq!(server.calls.load(Ordering::SeqCst), 0, "handler ran for a rejected peer");
}

/// Starts a server that only speaks TLS 1.2, which the client does not offer.
async fn spawn_tls12_only_server(leaf: &Leaf) -> SocketAddr {
    let key = PrivateKeyDer::from(PrivatePkcs8KeyDer::from(leaf.key_pkcs8.clone()));
    let config = rustls::ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS12])
        .with_no_client_auth()
        .with_single_cert(vec![leaf.cert.clone()], key)
        .expect("TLS 1.2 server config");
    let acceptor = TlsAcceptor::from(std::sync::Arc::new(config));

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local addr");
    tokio::spawn(async move {
        loop {
            let Ok((tcp, _)) = listener.accept().await else { break };
            let acceptor = acceptor.clone();
            // Never serves protocol traffic; the handshake is expected to fail.
            tokio::spawn(async move {
                let _ = acceptor.accept(tcp).await;
            });
        }
    });
    addr
}

#[tokio::test]
async fn control_valid_chain_is_accepted() {
    let ca = TestCa::new("trnc test root");
    let leaf = ca.issue_leaf(NAMES, CURRENT);
    let server = spawn_echo_server(server_provider(&leaf)).await.unwrap();

    assert_echo_over_reused_connection(&server, client_provider(&[&ca], None), "localhost")
        .await
        .unwrap();
}

#[tokio::test]
async fn untrusted_issuer_is_rejected() {
    let trusted = TestCa::new("trusted root");
    let other = TestCa::new("untrusted root");
    let leaf = other.issue_leaf(NAMES, CURRENT);
    let server = spawn_echo_server(server_provider(&leaf)).await.unwrap();

    assert_client_rejected(&server, client_provider(&[&trusted], None), "localhost").await;
}

#[tokio::test]
async fn self_signed_leaf_is_rejected() {
    let ca = TestCa::new("trnc test root");
    let leaf = self_signed_leaf(NAMES, CURRENT);
    let server = spawn_echo_server(server_provider(&leaf)).await.unwrap();

    assert_client_rejected(&server, client_provider(&[&ca], None), "localhost").await;
}

#[tokio::test]
async fn expired_leaf_is_rejected() {
    let ca = TestCa::new("trnc test root");
    let leaf = ca.issue_leaf(NAMES, EXPIRED);
    let server = spawn_echo_server(server_provider(&leaf)).await.unwrap();

    assert_client_rejected(&server, client_provider(&[&ca], None), "localhost").await;
}

#[tokio::test]
async fn not_yet_valid_leaf_is_rejected() {
    let ca = TestCa::new("trnc test root");
    let leaf = ca.issue_leaf(NAMES, NOT_YET_VALID);
    let server = spawn_echo_server(server_provider(&leaf)).await.unwrap();

    assert_client_rejected(&server, client_provider(&[&ca], None), "localhost").await;
}

#[tokio::test]
async fn hostname_mismatch_is_rejected() {
    let ca = TestCa::new("trnc test root");
    let leaf = ca.issue_leaf(&["other.example"], CURRENT);
    let server = spawn_echo_server(server_provider(&leaf)).await.unwrap();

    assert_client_rejected(&server, client_provider(&[&ca], None), "localhost").await;
}

#[tokio::test]
async fn spki_pin_mismatch_is_rejected() {
    let ca = TestCa::new("trnc test root");
    let leaf = ca.issue_leaf(NAMES, CURRENT);
    let other = ca.issue_leaf(NAMES, CURRENT);
    let server = spawn_echo_server(server_provider(&leaf)).await.unwrap();

    let wrong_pin = client_provider(&[&ca], Some(other.spki_sha256));
    assert_client_rejected(&server, wrong_pin, "localhost").await;
}

#[test]
fn empty_root_set_is_refused_at_construction() {
    assert!(TlsClientProvider::new(Vec::new(), None).is_err());
}

#[tokio::test]
async fn tls12_only_peer_is_rejected() {
    let ca = TestCa::new("trnc test root");
    let leaf = ca.issue_leaf(NAMES, CURRENT);
    let addr = spawn_tls12_only_server(&leaf).await;

    let mut client =
        ResilientClient::new(client_provider(&[&ca], None), "localhost".to_string(), addr.port());
    assert!(client.build_stream().await.is_err(), "TLS 1.2 peer was accepted");
}

#[tokio::test]
async fn plaintext_server_is_rejected_by_tls_client() {
    let ca = TestCa::new("trnc test root");
    let server = spawn_echo_server(PlaintextProvider).await.unwrap();

    assert_client_rejected(&server, client_provider(&[&ca], None), "localhost").await;
}

#[tokio::test]
async fn plaintext_client_is_rejected_by_tls_server() {
    let ca = TestCa::new("trnc test root");
    let leaf = ca.issue_leaf(NAMES, CURRENT);
    let server = spawn_echo_server(server_provider(&leaf)).await.unwrap();

    assert_client_rejected(&server, PlaintextProvider, "localhost").await;
}

#[tokio::test]
async fn garbage_bytes_do_not_reach_the_protocol() {
    let ca = TestCa::new("trnc test root");
    let leaf = ca.issue_leaf(NAMES, CURRENT);
    let server = spawn_echo_server(server_provider(&leaf)).await.unwrap();

    let mut raw = TcpStream::connect(server.addr).await.unwrap();
    raw.write_all(b"\x00\x01TRNC not a TLS record\r\n").await.unwrap();

    let mut received = Vec::new();
    // The server may reset the socket, so read errors are ignored and only the bytes matter.
    let read = timeout(Duration::from_secs(5), raw.read_to_end(&mut received))
        .await
        .expect("server did not close the connection");
    let _ = read;

    assert!(
        received.is_empty() || received[0] == 0x15,
        "server sent non-alert bytes to an unauthenticated peer: {received:?}"
    );
    assert_eq!(server.calls.load(Ordering::SeqCst), 0, "handler ran for garbage input");
}
