use std::{error::Error, net::SocketAddr};

use clap::Parser;

use interface::run_server;

#[derive(Parser)]
struct Args {
    #[arg(short, long, default_value = "127.0.0.1:5000")]
    addr: SocketAddr,
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    tokio::runtime::Builder::new_multi_thread()
        .enable_io()
        .build()?
        .block_on(run_server(args.addr))
}
