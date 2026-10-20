//! Provider-agnostic protocol checks shared by the plaintext and TLS integration tests.

use std::{
    error::Error,
    net::SocketAddr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use async_trait::async_trait;
use tokio::net::TcpListener;
use transport::{
    client::resilient_client::ResilientClient,
    errors::TransportError,
    secure::SecureStreamProvider,
    server::{Actions, Handler, RequestEnvelope, ResilientServer, ResponseEnvelope},
};

#[allow(dead_code)]
pub mod pki;

/// Echoes the request payload and counts how many times it ran.
struct CountingEcho {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl Handler for CountingEcho {
    async fn call(&self, payload: Vec<u8>) -> Result<Vec<u8>, TransportError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(payload)
    }
}

pub struct EchoServer {
    pub addr: SocketAddr,
    pub calls: Arc<AtomicUsize>,
}

/// Starts a `ResilientServer` accept loop on an ephemeral loopback port.
pub async fn spawn_echo_server<S>(provider: S) -> std::io::Result<EchoServer>
where
    S: SecureStreamProvider + Send + Sync + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let calls = Arc::new(AtomicUsize::new(0));

    let mut actions = Actions::new();
    actions.register_action("echo", CountingEcho { calls: calls.clone() });
    let actions = Arc::new(actions);
    let provider = Arc::new(provider);

    tokio::spawn(async move {
        loop {
            let Ok((tcp, peer)) = listener.accept().await else { break };
            let server = ResilientServer::new(provider.clone(), tcp, peer, actions.clone());
            tokio::spawn(async move {
                let _ = server.run().await;
            });
        }
    });

    Ok(EchoServer { addr, calls })
}

/// Sends several echo requests over one connection and checks every reply.
pub async fn assert_echo_over_reused_connection<C>(
    server: &EchoServer,
    client_provider: C,
    server_name: &str,
) -> Result<(), Box<dyn Error>>
where
    C: SecureStreamProvider,
{
    let mut client = ResilientClient::new(client_provider, server_name.to_string(), server.addr.port());
    client.build_stream().await?;

    for i in 0..3u8 {
        let payload = vec![b'a' + i; 8];
        let request = RequestEnvelope {
            action: "echo".to_string(),
            payload: payload.clone(),
        };
        let response: ResponseEnvelope = client.send_message(&request).await?;
        assert_eq!(response.payload, payload);
    }

    client.close().await?;
    assert_eq!(server.calls.load(Ordering::SeqCst), 3);
    Ok(())
}
