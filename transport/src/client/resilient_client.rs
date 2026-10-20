use tokio::net::TcpStream;
use tokio::time::timeout;
use byteser::ByteSerializable;
use crate::{
    errors::{ErrorPayload, TransportError},
    frame::frame::Frametype,
    secure::{HANDSHAKE_TIMEOUT, SecureStreamProvider},
    tcp::{connection::Connection, manager::{Role, StreamManager}},
};

/// Request/response client that runs every exchange over a stream from `P`.
pub struct ResilientClient<P: SecureStreamProvider> {
    host: String,
    port: u16,
    provider: P,
    manager: Option<StreamManager<P::Stream>>,
}

impl<P: SecureStreamProvider> ResilientClient<P> {
    /// `host` is resolved when connecting and is also the server name the provider verifies the peer against.
    pub fn new(provider: P, host: String, port: u16) -> Self {
        ResilientClient {
            host,
            port,
            provider,
            manager: None,
        }
    }

    /// Connects, runs the secure handshake, and completes the protocol Hello/Welcome exchange.
    ///
    /// The connection is reused by every subsequent [`send_message`](Self::send_message) call.
    pub async fn build_stream(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let tcp = TcpStream::connect((self.host.as_str(), self.port)).await?;
        let stream = timeout(HANDSHAKE_TIMEOUT, self.provider.connect(tcp, &self.host))
            .await
            .map_err(|_| TransportError::Timeout)??;

        let mut manager = StreamManager::new(Connection::new(stream), Role::Initiator);
        timeout(HANDSHAKE_TIMEOUT, manager.start_handshake())
            .await
            .map_err(|_| TransportError::Timeout)??;

        println!("[client] connected to {}:{}", self.host, self.port);
        self.manager = Some(manager);
        Ok(())
    }

    pub async fn send_message<T: ByteSerializable, R: ByteSerializable>(&mut self, message: &T) -> Result<R, Box<dyn std::error::Error>> {
        let manager = self.manager.as_mut().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotConnected, "TCP stream is not initialized")
        })?;

        let result = Self::exchange(manager, message).await;
        if result.is_err() {
            // A failed exchange can leave unread frames on the wire, so the connection is discarded.
            self.manager = None;
        }
        result
    }

    async fn exchange<T: ByteSerializable, R: ByteSerializable>(
        manager: &mut StreamManager<P::Stream>,
        message: &T,
    ) -> Result<R, Box<dyn std::error::Error>> {
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
                    let error_payload = ErrorPayload::decode(&frame.payload)?;
                    return Err(Box::new(TransportError::RemoteError(
                        error_payload.error_code,
                        error_payload.stream_id,
                        error_payload.message,
                    )));
                }
                _ => continue,
            }
        };

        let mut slice: &[u8] = &response_payload;
        let response = R::byte_deserialize(&mut slice)?;
        Ok(response)
    }

    /// Drops the connection. Dropping the underlying stream closes the socket.
    pub async fn close(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.manager = None;
        Ok(())
    }
}
