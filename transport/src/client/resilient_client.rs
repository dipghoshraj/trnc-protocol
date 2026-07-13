use std::net::SocketAddr;
use tokio::net::TcpStream;
use byteser::ByteSerializable;
use crate::{
    frame::frame::Frametype,
    tcp::{connection::Connection, manager::{Role, StreamManager}},
};

#[derive(Debug)]
pub struct ResilientClient {
    // Fields for the ResilientClient struct
    // tls_enabled: bool, 
    sockaddr: SocketAddr,
    tcp_stream: Option<TcpStream>,
}

impl ResilientClient {
    pub fn new(host: String, port: u16) -> Self {
        let sockaddr = format!("{}:{}", host, port)
            .parse()
            .expect("Invalid socket address");
        ResilientClient {
            // tls_enabled,
            sockaddr,
            tcp_stream: None,
        }
    }

    pub fn get_socket_addr(&self) -> SocketAddr {
        self.sockaddr
    }

    pub async fn build_stream(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let tcp = TcpStream::connect(self.sockaddr).await?;
        println!("[client] connected to {}", self.sockaddr);
        self.tcp_stream = Some(tcp);
        Ok(())
    }

    pub async fn send_message<T: ByteSerializable, R: ByteSerializable>(&mut self, message: &T) -> Result<R, Box<dyn std::error::Error>> {
        let tcp = self.tcp_stream.take().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotConnected, "TCP stream is not initialized")
        })?;

        let mut manager = StreamManager::new(Connection::new(tcp), Role::Initiator);
        let stream_id = manager.open_stream().await?;
        println!("[client] opened stream {stream_id}");

        let mut message_bytes = Vec::new();
        message.byte_serialize(&mut message_bytes);

        manager.send_data(stream_id, message_bytes).await?;
        manager.close_stream(stream_id).await?;
        manager.flush().await?;
        println!("[client] message sent, waiting for response...");

        let response_payload = loop {
            let frame = manager.recv_frame().await?;
            if frame.header.stream_id != stream_id {
                continue;
            }

            match frame.header.frame_type {
                Frametype::Data => break frame.payload,
                Frametype::Close => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::BrokenPipe,
                        "remote closed stream before sending response",
                    )
                    .into())
                }
                Frametype::Reset => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::ConnectionReset,
                        "stream was reset by remote",
                    )
                    .into())
                }
                Frametype::Error => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "error frame received on stream",
                    )
                    .into())
                }
                _ => continue,
            }
        };

        let tcp = manager.into_connection().into_inner();
        self.tcp_stream = Some(tcp);

        let mut slice: &[u8] = &response_payload;
        let response = R::byte_deserialize(&mut slice)?;
        Ok(response)
    }

    pub async fn close(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.tcp_stream.take(); // Drop the TcpStream to close the connection
        self.tcp_stream = None;
        Ok(())
    }
}