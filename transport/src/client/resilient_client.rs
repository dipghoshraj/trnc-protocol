use std::net::SocketAddr;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio_rustls::client::TlsStream;
use rustls_pki_types::ServerName;
use byteser::ByteSerializable;
use crate::{
    errors::{ErrorPayload, TransportError},
    frame::frame::Frametype,
    tcp::{connection::Connection, manager::{Role, SessionAuth, StreamManager}},
    tls::{
        endpoint::ClientTls,
        hello::session_secret,
        identity::{PeerIdentity, cert_fingerprint},
        verifier::extract_credential,
    },
};

#[derive(Debug)]
pub struct ResilientClient {
    sockaddr: SocketAddr,
    tls: ClientTls,
    session: Option<ClientSession>,
}

/// An established TLS connection and the authentication state for its `Hello` exchange.
#[derive(Debug)]
struct ClientSession {
    stream: TlsStream<TcpStream>,
    auth: SessionAuth,
}

impl ResilientClient {
    pub fn new(host: String, port: u16, tls: ClientTls) -> Self {
        let sockaddr = format!("{}:{}", host, port)
            .parse()
            .expect("Invalid socket address");
        ResilientClient {
            sockaddr,
            tls,
            session: None,
        }
    }

    pub fn get_socket_addr(&self) -> SocketAddr {
        self.sockaddr
    }

    /// Connects over TCP and completes the TLS handshake.
    ///
    /// The server is accepted only if its certificate carries a credential that is trusted. The
    /// server's node id is taken from that credential.
    pub async fn build_stream(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let tcp = TcpStream::connect(self.sockaddr).await?;
        let server_name = ServerName::IpAddress(self.sockaddr.ip().into());
        let stream = self.tls.connector.connect(server_name, tcp).await?;

        let (_, conn) = stream.get_ref();
        let leaf = conn
            .peer_certificates()
            .and_then(|certs| certs.first())
            .ok_or_else(|| std::io::Error::other("server presented no certificate"))?;
        let credential = extract_credential(leaf.as_ref())?;
        let peer = PeerIdentity {
            node_id: credential.node_id,
            cert_fingerprint: Some(cert_fingerprint(leaf.as_ref())),
        };
        let secret = session_secret(&**conn, &peer.node_id)?;

        println!("[client] connected to {} (server node {})", self.sockaddr, peer.node_id);
        let auth = SessionAuth {
            identity: self.tls.identity.clone(),
            trust_cache: self.tls.trust_cache.clone(),
            session_secret: secret,
            tls_peer: Some(peer),
        };
        self.session = Some(ClientSession { stream, auth });
        Ok(())
    }

    pub async fn send_message<T: ByteSerializable, R: ByteSerializable>(&mut self, message: &T) -> Result<R, Box<dyn std::error::Error>> {
        let ClientSession { stream, auth } = self.session.take().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotConnected, "TLS session is not initialized")
        })?;

        let mut manager = StreamManager::with_auth(Connection::new(stream), Role::Initiator, auth.clone());
        manager.start_handshake().await?;
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

        let stream = manager.into_connection().into_inner();
        self.session = Some(ClientSession { stream, auth });

        let mut slice: &[u8] = &response_payload;
        let response = R::byte_deserialize(&mut slice)?;
        Ok(response)
    }

    /// Sends TLS close_notify so the server sees a clean shutdown.
    pub async fn close(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(mut session) = self.session.take() {
            session.stream.shutdown().await?;
        }
        Ok(())
    }
}