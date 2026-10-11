use std::{error::Error, net::SocketAddr};

use clap::Parser;

use interface::{run_server, IdentityPaths};

#[derive(Parser)]
struct Args {
    #[arg(short, long, default_value = "127.0.0.1:5000")]
    addr: SocketAddr,

    #[command(flatten)]
    identity: IdentityPaths,
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    let tls = args.identity.server_tls()?;
    tokio::runtime::Builder::new_multi_thread()
        .enable_io()
        .build()?
        .block_on(run_server(args.addr, tls))
}
