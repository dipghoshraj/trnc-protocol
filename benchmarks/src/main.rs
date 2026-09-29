//! Repeatable loopback benchmark for the TRNC transport protocol, gRPC, and REST.
//!
//! TLS is deliberately disabled for every transport. Each measured operation is a
//! sequential echo request using a persistent client connection where the protocol
//! supports it. The benchmark reports successful-operation throughput and latency.

use std::{
    convert::Infallible,
    net::SocketAddr,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use axum::{Router, body::Bytes, routing::post};
use clap::{Parser, ValueEnum};
use hdrhistogram::Histogram;
use tokio::{
    net::{TcpListener, TcpStream},
    time::sleep,
};
use tonic::{Request, Response, Status, transport::Server};
use transport::{
    frame::frame::Frametype,
    tcp::{
        connection::Connection,
        manager::{Role, StreamManager},
    },
};

pub mod benchmark {
    tonic::include_proto!("benchmark");
}

use benchmark::{
    EchoRequest, EchoResponse,
    echo_client::EchoClient,
    echo_server::{Echo, EchoServer},
};

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Protocol {
    Trnc,
    Grpc,
    Rest,
    All,
}

#[derive(Debug, Parser)]
#[command(about = "Benchmark TRNC, gRPC, and REST echo round trips (without TLS)")]
struct Args {
    /// Protocol to measure. `all` runs every implementation sequentially.
    #[arg(long, value_enum, default_value_t = Protocol::All)]
    protocol: Protocol,
    /// Number of warm-up round trips per protocol; excluded from the result.
    #[arg(long, default_value_t = 100)]
    warmup: u64,
    /// Number of measured round trips per protocol.
    #[arg(long, default_value_t = 1_000)]
    requests: u64,
    /// Echo request payload size in bytes.
    #[arg(long, default_value_t = 256)]
    payload_bytes: usize,
}

#[derive(Debug)]
struct Metrics {
    completed: u64,
    elapsed: Duration,
    min_us: u64,
    p50_us: u64,
    p95_us: u64,
    p99_us: u64,
    max_us: u64,
}

impl Metrics {
    fn print_table(&self, protocol: Protocol, payload_bytes: usize) {
        let ops_per_second = self.completed as f64 / self.elapsed.as_secs_f64();
        let elapsed_ms = self.elapsed.as_secs_f64() * 1_000.0;

        println!("\n┌─────────────────────────────────────────────────────────────────────────────┐");
        println!("│ 📊 Benchmark Result: {:?} Protocol", protocol);
        println!("├─────────────────────────────────────────────────────────────────────────────┤");
        println!("│ Configuration:");
        println!("│   • Payload Size:      {} bytes", payload_bytes);
        println!("│   • Transport Layer:   TLS OFF");
        println!("├─────────────────────────────────────────────────────────────────────────────┤");
        println!("│ Performance Metrics:");
        println!("│   • Total Operations:  {}", self.completed);
        println!("│   • Elapsed Time:      {:.3} ms", elapsed_ms);
        println!("│   • Throughput:        {:.2} ops/sec", ops_per_second);
        println!("├─────────────────────────────────────────────────────────────────────────────┤");
        println!("│ Latency Analysis (in microseconds):");
        println!("│   • Min:               {} µs", self.min_us);
        println!("│   • P50 (Median):      {} µs", self.p50_us);
        println!("│   • P95:               {} µs", self.p95_us);
        println!("│   • P99:               {} µs", self.p99_us);
        println!("│   • Max:               {} µs", self.max_us);
        println!("└─────────────────────────────────────────────────────────────────────────────┘");
        
        // Also print the raw RESULT line for logging/parsing
        println!("\nRESULT protocol={protocol:?} tls=off payload_bytes={payload_bytes} operations={} elapsed_ms={:.3} ops_per_sec={:.2} latency_us_min={} latency_us_p50={} latency_us_p95={} latency_us_p99={} latency_us_max={}", 
            self.completed, elapsed_ms, ops_per_second, self.min_us, self.p50_us, self.p95_us, self.p99_us, self.max_us);
    }
}

fn print_comparison_table(results: &[(Protocol, Metrics)], _payload_bytes: usize) {
    println!("\n╔═════════════════════════════════════════════════════════════════════════════╗");
    println!("║                         COMPARISON TABLE                                    ║");
    println!("╠════════════╦════════════╦═══════════╦═════════════╦═════════╦═════════╦═════╣");
    println!("║ Protocol   ║  Ops/Sec   ║ Min (µs)  ║ P50 (µs)    ║ P95 (µs)║ P99 (µs)║Max  ║");
    println!("╠════════════╬════════════╬═══════════╬═════════════╬═════════╬═════════╬═════╣");
    
    let mut best_throughput = 0.0;
    let mut best_protocol = "";
    
    for (protocol, metrics) in results {
        let ops_per_second = metrics.completed as f64 / metrics.elapsed.as_secs_f64();
        if ops_per_second > best_throughput {
            best_throughput = ops_per_second;
            best_protocol = match protocol {
                Protocol::Trnc => "TRNC",
                Protocol::Grpc => "gRPC",
                Protocol::Rest => "REST",
                Protocol::All => unreachable!(),
            };
        }
        
        let protocol_name = match protocol {
            Protocol::Trnc => "Trnc",
            Protocol::Grpc => "Grpc",
            Protocol::Rest => "Rest",
            Protocol::All => unreachable!(),
        };
        
        let marker = if (ops_per_second - best_throughput).abs() < 0.01 { "🏆" } else { "  " };
        println!(
            "║ {:<9} {} ║ {:>10.2} ║ {:>9} ║ {:>11} ║ {:>7} ║ {:>7} ║ {:>4} ║",
            protocol_name,
            marker,
            ops_per_second,
            metrics.min_us,
            metrics.p50_us,
            metrics.p95_us,
            metrics.p99_us,
            metrics.max_us,
        );
    }
    
    println!("╚════════════╩════════════╩═══════════╩═════════════╩═════════╩═════════╩═════╝");
    println!("\n📈 Key Insights:");
    println!("  • {} has the best throughput ({:.2} ops/sec) 🏆", best_protocol, best_throughput);
    println!("  • All measurements performed on 127.0.0.1 (loopback) without TLS");
    println!("  • Results are sequential (concurrency = 1)");
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    if args.requests == 0 {
        bail!("--requests must be greater than zero");
    }
    if args.payload_bytes == 0 {
        bail!("--payload-bytes must be greater than zero");
    }

    println!("\n🚀 TRNC Benchmark Suite - Performance Analysis");
    println!("═══════════════════════════════════════════════════════════════════════════════");
    println!("Configuration:");
    println!("  • Warmup requests:    {}", args.warmup);
    println!("  • Measured requests:  {}", args.requests);
    println!("  • Payload size:       {} bytes", args.payload_bytes);
    println!("  • TLS:                OFF");
    println!("═══════════════════════════════════════════════════════════════════════════════\n");
    
    let mut results = Vec::new();
    
    for protocol in selected(args.protocol) {
        let metrics = match protocol {
            Protocol::Trnc => measure_trnc(args.warmup, args.requests, args.payload_bytes).await?,
            Protocol::Grpc => measure_grpc(args.warmup, args.requests, args.payload_bytes).await?,
            Protocol::Rest => measure_rest(args.warmup, args.requests, args.payload_bytes).await?,
            Protocol::All => unreachable!("all expands before measurement"),
        };
        metrics.print_table(protocol, args.payload_bytes);
        results.push((protocol, metrics));
    }

    // Print comparison table if multiple protocols were run
    if results.len() > 1 {
        print_comparison_table(&results, args.payload_bytes);
    }

    Ok(())
}

fn selected(protocol: Protocol) -> Vec<Protocol> {
    match protocol {
        Protocol::All => vec![Protocol::Trnc, Protocol::Grpc, Protocol::Rest],
        protocol => vec![protocol],
    }
}

fn payload(size: usize) -> Vec<u8> {
    (0..size).map(|i| (i % 251) as u8).collect()
}

fn record(histogram: &mut Histogram<u64>, started: Instant) -> Result<()> {
    histogram.record(started.elapsed().as_micros().max(1) as u64)?;
    Ok(())
}

fn metrics(completed: u64, elapsed: Duration, histogram: Histogram<u64>) -> Metrics {
    Metrics {
        completed,
        elapsed,
        min_us: histogram.min(),
        p50_us: histogram.value_at_quantile(0.50),
        p95_us: histogram.value_at_quantile(0.95),
        p99_us: histogram.value_at_quantile(0.99),
        max_us: histogram.max(),
    }
}

async fn measure_trnc(warmup: u64, requests: u64, payload_size: usize) -> Result<Metrics> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let server = tokio::spawn(trnc_server(listener));
    let tcp = TcpStream::connect(addr).await?;
    let mut client = StreamManager::new(Connection::new(tcp), Role::Initiator);
    client.start_handshake().await?;
    let body = payload(payload_size);
    for _ in 0..warmup {
        trnc_round_trip(&mut client, &body).await?;
    }
    let started = Instant::now();
    let mut histogram = Histogram::<u64>::new(3)?;
    for _ in 0..requests {
        let operation = Instant::now();
        trnc_round_trip(&mut client, &body).await?;
        record(&mut histogram, operation)?;
    }
    drop(client);
    server.await.context("TRNC server task failed")??;
    Ok(metrics(requests, started.elapsed(), histogram))
}

async fn trnc_round_trip(client: &mut StreamManager<TcpStream>, body: &[u8]) -> Result<()> {
    let stream_id = client.open_stream().await?;
    client.send_data(stream_id, body.to_vec()).await?;
    client.close_stream(stream_id).await?;
    client.flush().await?;
    loop {
        let frame = client.recv_frame().await?;
        if frame.header.stream_id == stream_id && frame.header.frame_type == Frametype::Data {
            if frame.payload != body {
                bail!("TRNC echo payload differs from request");
            }
            return Ok(());
        }
    }
}

async fn trnc_server(listener: TcpListener) -> Result<()> {
    let (tcp, _) = listener.accept().await?;
    let mut server = StreamManager::new(Connection::new(tcp), Role::Acceptor);
    loop {
        let frame = match server.recv_frame().await {
            Ok(frame) => frame,
            Err(transport::errors::TransportError::ConnectionClosed) => return Ok(()),
            Err(error) => return Err(error.into()),
        };
        if frame.header.frame_type == Frametype::Data {
            let stream_id = frame.header.stream_id;
            let request = server
                .recv_data(stream_id)
                .await?
                .context("TRNC data frame was not queued")?;
            server.send_data(stream_id, request.to_vec()).await?;
            server.close_stream(stream_id).await?;
            server.flush().await?;
        }
    }
}

#[derive(Default)]
struct GrpcEcho;

#[tonic::async_trait]
impl Echo for GrpcEcho {
    async fn echo(&self, request: Request<EchoRequest>) -> Result<Response<EchoResponse>, Status> {
        Ok(Response::new(EchoResponse {
            payload: request.into_inner().payload,
        }))
    }
}

async fn measure_grpc(warmup: u64, requests: u64, payload_size: usize) -> Result<Metrics> {
    let addr = reserve_addr().await?;
    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(EchoServer::new(GrpcEcho))
            .serve(addr)
            .await
            .context("gRPC server failed")
    });
    let endpoint = format!("http://{addr}"); // h2c: TLS intentionally disabled.
    let mut client = connect_grpc(&endpoint).await?;
    let body = payload(payload_size);
    for _ in 0..warmup {
        grpc_round_trip(&mut client, &body).await?;
    }
    let started = Instant::now();
    let mut histogram = Histogram::<u64>::new(3)?;
    for _ in 0..requests {
        let operation = Instant::now();
        grpc_round_trip(&mut client, &body).await?;
        record(&mut histogram, operation)?;
    }
    server.abort();
    Ok(metrics(requests, started.elapsed(), histogram))
}

async fn connect_grpc(endpoint: &str) -> Result<EchoClient<tonic::transport::Channel>> {
    for _ in 0..100 {
        match EchoClient::connect(endpoint.to_owned()).await {
            Ok(client) => return Ok(client),
            Err(_) => sleep(Duration::from_millis(5)).await,
        }
    }
    bail!("gRPC server did not become ready")
}

async fn grpc_round_trip(
    client: &mut EchoClient<tonic::transport::Channel>,
    body: &[u8],
) -> Result<()> {
    let response = client
        .echo(EchoRequest {
            payload: body.to_vec(),
        })
        .await?
        .into_inner();
    if response.payload != body {
        bail!("gRPC echo payload differs from request");
    }
    Ok(())
}

async fn measure_rest(warmup: u64, requests: u64, payload_size: usize) -> Result<Metrics> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let server = tokio::spawn(async move {
        axum::serve(listener, Router::new().route("/echo", post(rest_echo)))
            .await
            .context("REST server failed")
    });
    let client = reqwest::Client::builder().build()?;
    let url = format!("http://{addr}/echo"); // HTTP: TLS intentionally disabled.
    let body = payload(payload_size);
    for _ in 0..warmup {
        rest_round_trip(&client, &url, &body).await?;
    }
    let started = Instant::now();
    let mut histogram = Histogram::<u64>::new(3)?;
    for _ in 0..requests {
        let operation = Instant::now();
        rest_round_trip(&client, &url, &body).await?;
        record(&mut histogram, operation)?;
    }
    server.abort();
    Ok(metrics(requests, started.elapsed(), histogram))
}

async fn rest_echo(body: Bytes) -> Result<Bytes, Infallible> {
    Ok(body)
}

async fn rest_round_trip(client: &reqwest::Client, url: &str, body: &[u8]) -> Result<()> {
    let response = client
        .post(url)
        .body(body.to_vec())
        .send()
        .await?
        .error_for_status()?;
    let echoed = response.bytes().await?;
    if echoed.as_ref() != body {
        bail!("REST echo payload differs from request");
    }
    Ok(())
}

async fn reserve_addr() -> Result<SocketAddr> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    Ok(listener.local_addr()?)
}
