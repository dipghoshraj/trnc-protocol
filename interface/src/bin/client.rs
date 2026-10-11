use std::{error::Error, net::SocketAddr};

use clap::Parser;

use interface::{resilient_client_run, IdentityPaths};

#[derive(Parser)]
struct Args {
    #[arg(short, long, default_value = "127.0.0.1:5000")]
    addr: SocketAddr,

    #[arg(short, long, default_value = "hello from interface")]
    message: String,

    #[command(flatten)]
    identity: IdentityPaths,
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    let tls = args.identity.client_tls()?;
    tokio::runtime::Builder::new_multi_thread()
        .enable_io()
        .build()?
        .block_on(resilient_client_run(args.addr, args.message, tls))
}
