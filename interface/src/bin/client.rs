use std::{error::Error, net::SocketAddr};

use clap::Parser;

use interface::resilient_client_run;

#[derive(Parser)]
struct Args {
    #[arg(short, long, default_value = "127.0.0.1:5000")]
    addr: SocketAddr,

    #[arg(short, long, default_value = "hello from interface")]
    message: String,
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    tokio::runtime::Builder::new_multi_thread()
        .enable_io()
        .build()?
        .block_on(resilient_client_run(args.addr, args.message))
}
