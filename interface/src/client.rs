use std::{error::Error, net::SocketAddr};

use byteser::ByteSerializable;
use byteser_derive::ByteSerializable;
use transport::client::resilient_client::ResilientClient;
use transport::server::RequestEnvelope;

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

pub async fn resilient_client_run(addr: SocketAddr, message: String) -> Result<(), Box<dyn Error>> {
    let host = addr.ip().to_string();
    let port = addr.port();

    let mut client = ResilientClient::new(host, port);
    client.build_stream().await?;
    println!("[client] connected to {addr}");

    let mut request_payload = Vec::new();
    let request_message = UserMessage { message };
    request_message.byte_serialize(&mut request_payload);

    let request = RequestEnvelope {
        action: "echo".to_string(),
        payload: request_payload,
    };

    let response: transport::server::ResponseEnvelope = client.send_message(&request).await?;
    let mut response_slice: &[u8] = &response.payload;
    let response_message: ServerResponse = ServerResponse::byte_deserialize(&mut response_slice)?;

    println!("[client] response: {}", response_message.response);
    println!("[client] user: {} ({})", response_message.user.name, response_message.user.age);
    client.close().await?;
    Ok(())
}
