use std::{error::Error, net::SocketAddr, sync::Arc};

use async_trait::async_trait;
use byteser::ByteSerializable;
use byteser_derive::ByteSerializable;
use tokio::net::TcpListener;
use transport::{
    errors::TransportError,
    server::{Actions, Handler, ResilientServer},
};

#[derive(Debug, ByteSerializable)]
struct UserMessage {
    message: String,
}

#[derive(Debug, ByteSerializable)]
struct ServerResponse {
    response: String,
    user: User,
}

#[derive(Debug, ByteSerializable)]
struct User {
    name: String,
    age: u32,
}

struct EchoHandler;

#[async_trait]
impl Handler for EchoHandler {
    async fn call(&self, payload: Vec<u8>) -> Result<Vec<u8>, TransportError> {
        let mut slice: &[u8] = &payload;
        let request: UserMessage = UserMessage::byte_deserialize(&mut slice)
            .map_err(|msg| TransportError::InternalError(format!("deserialization failed: {}", msg)))?;

        let response = ServerResponse {
            response: format!("ECHO: {}", request.message),
            user: User {
                name: "Alice".to_string(),
                age: 30,
            },
        };

        let mut response_bytes = Vec::new();
        response.byte_serialize(&mut response_bytes);
        Ok(response_bytes)
    }
}

pub async fn run_server(addr: SocketAddr) -> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind(addr).await?;
    let mut actions = Actions::new();
    actions.register_action("echo", EchoHandler);
    let actions = Arc::new(actions);

    println!("[server] listening on {addr}");

    loop {
        let (socket, peer_addr) = listener.accept().await?;
        println!("[server] accepted connection from {peer_addr}");

        let actions = actions.clone();
        tokio::spawn(async move {
            let server = ResilientServer::new(socket, peer_addr, actions);
            if let Err(err) = server.run().await {
                eprintln!("[server {peer_addr}] connection error: {err}");
            }
        });
    }
}
