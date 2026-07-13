use std::net::SocketAddr;
use tokio::net::TcpStream;
use crate::{
    frame::frame::Frametype,
    tcp::{connection::Connection, manager::{Role, StreamManager}},
};


struct ResilientClient {
    // Fields for the ResilientClient struct
    host: String,
    port: u16,
    tls_enabled: bool,
    sockaddr: SocketAddr,
    stream_id: Option<u32>,
    stream_manager: Option<StreamManager<TcpStream>>,
}

impl ResilientClient {
    pub fn new(host: String, port: u16, tls_enabled: bool) -> Self {
        let sockaddr = format!("{}:{}", host, port)
            .parse()
            .expect("Invalid socket address");
        ResilientClient {
            host,
            port,
            tls_enabled,
            sockaddr,
            stream_id: None,
            stream_manager: None,
        }
    }

    pub fn get_socket_addr(&self) -> SocketAddr {
        self.sockaddr
    }

    pub async fn connect(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let tcp = TcpStream::connect(self.sockaddr).await.expect("Failed to connect to server");
        println!("[client] connected to {}", self.sockaddr);

        let mut manager = StreamManager::new(Connection::new(tcp), Role::Initiator);
        let stream_id = manager.open_stream().await.expect("Failed to open stream");

        println!("[client] opened stream {stream_id}");
        self.stream_id = Some(stream_id);
        self.stream_manager = Some(manager);
        Ok(())
    }

    pub async fn send_message(&mut self, message: Vec<u8>) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(manager) = &mut self.stream_manager {
            if let Some(stream_id) = self.stream_id {
                manager.send_data(stream_id, message).await?;
                manager.close_stream(stream_id).await?;
                manager.flush().await?;
                println!("[client] message sent, waiting for response...");

                let response_payload = loop {
                    let frame = manager.recv_frame().await?;
                    if frame.header.stream_id == stream_id && frame.header.frame_type == Frametype::Data {
                        break frame.payload;
                    }
                };

                println!("[client] response: {}", String::from_utf8_lossy(&response_payload));
            } else {
                return Err("Stream ID is not set. Please connect first.".into());
            }
        } else {
            return Err("StreamManager is not initialized. Please connect first.".into());
        }
        Ok(())
    }

    pub async fn close(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(manager) = &mut self.stream_manager {
            if let Some(stream_id) = self.stream_id {
                manager.close_stream(stream_id).await?;
                manager.flush().await?;
                println!("[client] stream {stream_id} closed");
            }
        }
        Ok(())
    }
}