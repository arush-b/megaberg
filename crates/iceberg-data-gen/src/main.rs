use anyhow::{Context, Result};
use clap::Parser;
use std::path::PathBuf;

use iceberg_data_gen::{run_generation, AppConfig};

#[derive(Parser, Debug)]
#[command(name = "iceberg-data-gen")]
struct Args {
    /// Path to config JSON file
    #[arg(short, long)]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    let config_content =
        std::fs::read_to_string(&args.config).context("Failed to read config file")?;
    let config: AppConfig =
        serde_json::from_str(&config_content).context("Failed to parse config")?;

    run_generation(config).await?;

    Ok(())
}
