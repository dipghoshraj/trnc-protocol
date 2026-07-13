use std::{error::Error, net::SocketAddr};

use tokio::net::TcpStream;
use transport::{
    frame::frame::Frametype,
    tcp::{connection::Connection, manager::{Role, StreamManager}},
};

pub async fn run_client(addr: SocketAddr, message: String) -> Result<(), Box<dyn Error>> {
    let tcp = TcpStream::connect(addr).await?;
    println!("[client] connected to {addr}");

    let mut manager = StreamManager::new(Connection::new(tcp), Role::Initiator);
    let stream_id = manager.open_stream().await?;
    println!("[client] opened stream {stream_id}");

    manager.send_data(stream_id, message.as_bytes().to_vec()).await?;
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
    Ok(())
}
