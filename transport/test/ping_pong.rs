//! Smoke-test: Ping/Pong handshake and raw-bytes round-trip over a loopback
//! TCP connection.
//!
//! ```
//! cargo run --example ping_pong
//! ```
//!
//! The example spawns a server task that accepts a single connection and then
//! runs a client in the main task.  Two scenarios are exercised in sequence on
//! the same connection:
//!
//! 1. **Ping/Pong** — client sends a `Ping`, server auto-replies with a `Pong`
//!    (handled inside [`StreamManager::recv_frame`]), client verifies the
//!    echoed payload.
//!
//! 2. **Raw-bytes echo** — client opens a logical stream, sends a payload,
//!    server echoes it back, client verifies the round-trip.

use std::net::SocketAddr;

use tokio::net::{TcpListener, TcpStream};
use transport::{
    errors::TransportError,
    frame::frame::Frametype,
    tcp::{
        connection::Connection,
        manager::{Role, StreamManager},
    },
};

// ── Entry point ───────────────────────────────────────────────────────────────

// Build the runtime manually to avoid the `macros` feature of tokio
// (which requires tokio-macros and may not be available in all registries).
fn main() -> Result<(), Box<dyn std::error::Error>> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_io()
        .build()?
        .block_on(async_main())
}

async fn async_main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    println!("[main]   Server bound to {addr}");

    // Server runs in a background task; client runs in the main task so that
    // any client assertion failure surfaces directly as a process error.
    let server = tokio::spawn(run_server(listener));
    run_client(addr).await?;
    server.await??; // propagate any panic or error from the server task

    println!("\nAll checks passed — transport layer is ready.");
    Ok(())
}

// ── Server ────────────────────────────────────────────────────────────────────

async fn run_server(listener: TcpListener) -> Result<(), TransportError> {
    let (tcp, peer) = listener.accept().await?;
    println!("[server] Accepted connection from {peer}");

    let mut mgr = StreamManager::new(Connection::new(tcp), Role::Acceptor);

    // ── 1. Ping/Pong ─────────────────────────────────────────────────────────
    //
    // recv_frame() handles Ping internally: it sends the Pong (flush included)
    // before returning, so no extra flush call is needed here.
    let f = mgr.recv_frame().await?;
    assert_eq!(f.header.frame_type, Frametype::Ping, "expected Ping");
    println!(
        "[server] Ping received (payload: {:?}), Pong sent",
        std::str::from_utf8(&f.payload).unwrap_or("<binary>"),
    );

    // ── 2. Raw-bytes echo ─────────────────────────────────────────────────────
    let f = mgr.recv_frame().await?;
    assert_eq!(f.header.frame_type, Frametype::Open, "expected Open");
    let sid = f.header.stream_id;
    println!("[server] Stream {sid} opened by client");

    let f = mgr.recv_frame().await?;
    assert_eq!(f.header.frame_type, Frametype::Data, "expected Data");
    // Use the payload carried in the returned Frame directly so we avoid
    // sending an unsolicited Window update before the echo.
    let echo_payload = f.payload;
    println!(
        "[server] Received {} byte(s): {:?}",
        echo_payload.len(),
        std::str::from_utf8(&echo_payload).unwrap_or("<binary>"),
    );

    mgr.send_data(sid, echo_payload).await?;
    mgr.flush().await?;
    println!("[server] Echoed payload back on stream {sid}");

    Ok(())
}

// ── Client ────────────────────────────────────────────────────────────────────

async fn run_client(addr: SocketAddr) -> Result<(), TransportError> {
    let tcp = TcpStream::connect(addr).await?;
    println!("[client] Connected to {addr}");

    let mut mgr = StreamManager::new(Connection::new(tcp), Role::Initiator);

    // ── 1. Ping/Pong ─────────────────────────────────────────────────────────
    let ping_payload = b"trench-probe";
    mgr.send_ping(ping_payload.to_vec()).await?;
    println!("[client] Ping sent");

    let f = mgr.recv_frame().await?;
    assert_eq!(f.header.frame_type, Frametype::Pong, "expected Pong");
    assert_eq!(
        &f.payload[..],
        &ping_payload[..],
        "Pong payload does not match Ping payload",
    );
    println!("[client] Pong received, payload matches ✓");

    // ── 2. Raw-bytes round-trip ───────────────────────────────────────────────
    let sid = mgr.open_stream().await?;
    println!("[client] Opened stream {sid}");

    let message = b"hello from trench-db transport layer";
    mgr.send_data(sid, message.to_vec()).await?;
    // Explicit flush: open_stream and send_data buffer internally and only
    // auto-flush at 32 KiB; we need to push the frames now.
    mgr.flush().await?;
    println!("[client] Sent {} byte(s) on stream {sid}", message.len());

    // The server may send a Window update before the echo Data frame if it
    // calls recv_data internally. Loop until we see the Data frame.
    let echo_frame = loop {
        let f = mgr.recv_frame().await?;
        if f.header.frame_type == Frametype::Data {
            break f;
        }
    };

    assert_eq!(
        &echo_frame.payload[..],
        &message[..],
        "echoed payload does not match sent payload",
    );
    println!(
        "[client] Echo received: {:?} ✓",
        std::str::from_utf8(&echo_frame.payload).unwrap_or("<binary>"),
    );

    Ok(())
}
