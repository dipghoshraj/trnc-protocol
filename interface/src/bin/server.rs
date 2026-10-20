use std::{error::Error, net::SocketAddr, path::PathBuf};

use clap::Parser;

use interface::{
    run_server,
    tls::{pem, TlsServerProvider},
};

#[derive(Parser)]
struct Args {
    #[arg(short, long, default_value = "127.0.0.1:5000")]
    addr: SocketAddr,

    /// PEM certificate chain presented to clients (leaf first).
    #[arg(long)]
    cert: PathBuf,

    /// PEM private key matching the leaf certificate.
    #[arg(long)]
    key: PathBuf,
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    let provider = TlsServerProvider::new(
        pem::load_certs(&args.cert)?,
        pem::load_private_key(&args.key)?,
    )?;

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(run_server(args.addr, provider))
}
