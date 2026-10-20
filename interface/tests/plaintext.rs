mod common;

use transport::testing::PlaintextProvider;

#[tokio::test]
async fn protocol_echoes_over_reused_plaintext_connection() -> Result<(), Box<dyn std::error::Error>> {
    let server = common::spawn_echo_server(PlaintextProvider).await?;
    common::assert_echo_over_reused_connection(&server, PlaintextProvider, "127.0.0.1").await
}
