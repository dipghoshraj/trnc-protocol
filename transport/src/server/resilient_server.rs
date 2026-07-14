use std::sync::Arc;
use std::net::SocketAddr;
use byteser::ByteSerializable;
use tokio::net::TcpStream;
use crate::server::actions::Actions;
use byteser_derive::ByteSerializable;
use crate::server::dispatcher;

use crate::{
    errors::TransportError,
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

    pub async fn run(self) -> Result<(), Box<dyn std::error::Error>> {
        let mut manager = StreamManager::new(Connection::new(self.stream), Role::Acceptor);
        let peer = self.peer;
        let actions = self.actions;
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

                    
                    let response = Self::handle_request(&dispatcher::Dispatcher::new(actions.clone()), request).await?;
                    let mut response_payload = Vec::<u8>::new();
                    response.byte_serialize(&mut response_payload);
                    manager.send_data(stream_id, response_payload).await?;
                }
                _ => {
                    println!("[server {}] received unexpected frame type: {:?}", peer, frame.header.frame_type);
                }
            }
        }
    }
}


