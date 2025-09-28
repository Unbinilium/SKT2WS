use clap::Parser;
use skt2ws::{Config, run};
use std::net::SocketAddr;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[arg(long, default_value = "127.0.0.1:3000")]
    ws_addr: SocketAddr,

    #[arg(long, default_value = "/ws")]
    ws_path: PathBuf,

    #[arg(long, default_value = "/dev/shm/skt2ws.sock")]
    socket_path: PathBuf,

    #[arg(long, default_value = "info")]
    log_level: String,

    #[arg(long, default_value = "16384")]
    buffer_size: usize,

    #[arg(long, default_value = "4096")]
    channel_capacity: usize,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    let config = Config {
        ws_addr: cli.ws_addr,
        ws_path: cli.ws_path,
        socket_path: cli.socket_path,
        log_level: cli.log_level,
        buffer_size: cli.buffer_size,
        channel_capacity: cli.channel_capacity,
    };

    run(config).await
}
