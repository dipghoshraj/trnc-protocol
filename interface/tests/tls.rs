mod common;

use common::{
    assert_echo_over_reused_connection, spawn_echo_server,
    pki::{client_provider, server_provider, TestCa, CURRENT},
};

#[tokio::test]
async fn protocol_echoes_over_reused_tls_connection() {
    let ca = TestCa::new("trnc test root");
    let leaf = ca.issue_leaf(&["localhost", "127.0.0.1"], CURRENT);
    let server = spawn_echo_server(server_provider(&leaf)).await.unwrap();

    assert_echo_over_reused_connection(&server, client_provider(&[&ca], None), "127.0.0.1")
        .await
        .unwrap();
}

#[tokio::test]
async fn protocol_echoes_over_tls_with_dns_name_and_matching_pin() {
    let ca = TestCa::new("trnc test root");
    let leaf = ca.issue_leaf(&["localhost", "127.0.0.1"], CURRENT);
    let server = spawn_echo_server(server_provider(&leaf)).await.unwrap();

    let pinned = client_provider(&[&ca], Some(leaf.spki_sha256));
    assert_echo_over_reused_connection(&server, pinned, "localhost")
        .await
        .unwrap();
}
