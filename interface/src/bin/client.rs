use std::{error::Error, net::SocketAddr, path::PathBuf};

use clap::Parser;

use interface::{
    resilient_client_run,
    tls::{parse_spki_pin, pem, TlsClientProvider},
};

#[derive(Parser)]
struct Args {
    #[arg(short, long, default_value = "127.0.0.1:5000")]
    addr: SocketAddr,

    #[arg(short, long, default_value = "hello from interface")]
    message: String,

    /// PEM CA certificate(s) that must have signed the server's chain.
    #[arg(long)]
    ca: PathBuf,

    /// Optional SHA-256 (64 hex chars) of the server leaf's SubjectPublicKeyInfo.
    #[arg(long)]
    pin: Option<String>,
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    let spki_pin = args.pin.as_deref().map(parse_spki_pin).transpose()?;
    let provider = TlsClientProvider::new(pem::load_certs(&args.ca)?, spki_pin)?;

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(resilient_client_run(args.addr, args.message, provider))
}
