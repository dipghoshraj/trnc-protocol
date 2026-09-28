use std::sync::Arc;
use std::net::SocketAddr;
use byteser::ByteSerializable;
use tokio::net::TcpStream;
use crate::server::actions::Actions;
use byteser_derive::ByteSerializable;
use crate::server::dispatcher;

use crate::{
    errors::{ErrorPayload, TransportError},
    frame::frame::Frametype,
    tcp::{connection::Connection, manager::{Role, StreamManager}},
};

pub struct ResilientServer {
    stream: TcpStream,
    peer: SocketAddr,
    actions: Arc<Actions>,
}

#[derive(ByteSerializable)]
pub struct RequestEnvelope {
    pub action: String,
    pub payload: Vec<u8>,
}

#[derive(ByteSerializable)]
pub struct ResponseEnvelope {
    pub payload: Vec<u8>,
}



impl ResilientServer {
    pub fn new(stream: TcpStream, peer: SocketAddr, actions: Arc<Actions>) -> Self {
        ResilientServer { stream, peer, actions }
    }

    pub async fn handle_request(
        dispatcher: &dispatcher::Dispatcher,
        request: RequestEnvelope,
    ) -> Result<ResponseEnvelope, TransportError> {
        dispatcher.dispatch(request).await.map(|response| ResponseEnvelope {
            payload: response.payload,
        })
    }

    pub async fn run(self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut manager = StreamManager::new(Connection::new(self.stream), Role::Acceptor);
        let peer = self.peer;
        let dispatcher = dispatcher::Dispatcher::new(self.actions.clone());

        loop {
            let frame = match manager.recv_frame().await {
                Ok(frame) => frame,
                Err(crate::errors::TransportError::ConnectionClosed) => {
                    println!("[server {}] client disconnected", peer);
                    return Ok(());
                }
                Err(err) => return Err(Box::new(err)),
            };

            let stream_id = frame.header.stream_id;
            match frame.header.frame_type {
                Frametype::Hello => {
                    println!("[server {}] received handshake Hello", peer);
                    continue;
                }
                Frametype::Welcome => {
                    println!("[server {}] received unexpected Welcome", peer);
                    continue;
                }
                Frametype::Settings => {
                    println!("[server {}] received connection settings", peer);
                    continue;
                }
                Frametype::Open => {
                    println!("[server {}] stream {} opened", peer, stream_id);
                }
                Frametype::Close => {
                    println!("[server {}] stream {} closed by client", peer, stream_id);
                }
                Frametype::Reset => {
                    println!("[server {}] stream {} reset by client", peer, stream_id);
                }
                Frametype::Data => {
                    let payload = manager.recv_data(stream_id).await?.unwrap_or_default();
                    if payload.is_empty() {
                        continue;
                    }

                    let mut slice: &[u8] = &payload;
                    let request: RequestEnvelope = RequestEnvelope::byte_deserialize(&mut slice)
                        .map_err(|e| TransportError::InternalError(format!("Failed to deserialize request: {}", e)))?;

                    let response = Self::handle_request(&dispatcher, request).await?;
                    let mut response_payload = Vec::<u8>::new();
                    response.byte_serialize(&mut response_payload);
                    manager.send_data(stream_id, response_payload).await?;
                    manager.close_stream(stream_id).await?;
                    manager.flush().await?;
                }
                Frametype::Error => {
                    let error_payload = ErrorPayload::decode(&frame.payload)
                        .map_err(|e| TransportError::InternalError(format!("Failed to decode error frame: {}", e)))?;
                    return Err(TransportError::RemoteError(
                        error_payload.error_code,
                        error_payload.stream_id,
                        error_payload.message,
                    ).into());
                }
                _ => {
                    println!("[server {}] received unexpected frame type: {:?}", peer, frame.header.frame_type);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use std::sync::Arc;
    use tokio::net::TcpListener;

    use super::{RequestEnvelope, ResponseEnvelope};
    use crate::client::resilient_client::ResilientClient;
    use crate::errors::TransportError;
    use super::super::actions::{Actions, Handler};

    struct EchoHandler;

    #[async_trait]
    impl Handler for EchoHandler {
        async fn call(&self, payload: Vec<u8>) -> Result<Vec<u8>, TransportError> {
            Ok(payload)
        }
    }

    #[tokio::test]
    async fn resilient_server_echoes_request() -> Result<(), Box<dyn std::error::Error>> {
        let mut actions = Actions::new();
        actions.register_action("echo", EchoHandler);
        let actions = Arc::new(actions);

        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;

        let server_handle = tokio::spawn({
            let actions = actions.clone();
            async move {
                let (stream, peer) = listener.accept().await?;
                let server = super::ResilientServer::new(stream, peer, actions);
                server.run().await
            }
        });

        let mut client = ResilientClient::new(addr.ip().to_string(), addr.port());
        client.build_stream().await?;

        let request = RequestEnvelope {
            action: "echo".to_string(),
            payload: b"hello".to_vec(),
        };

        let response: ResponseEnvelope = client.send_message(&request).await?;
        assert_eq!(response.payload, b"hello".to_vec());

        client.close().await?;
        let _ = server_handle.await?;
        Ok(())
    }
}


