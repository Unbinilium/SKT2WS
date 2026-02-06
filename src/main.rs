use skt2ws::Config;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cfg_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "config.json".to_string());

    let config = Config::load_from_file(&cfg_path)?;

    skt2ws::run(config).await
}
