use cardano_conformance_harness::scenario::{self, runner::ScenarioRunner};
use clap::Parser;
use std::path::PathBuf;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "cardano-conformance-harness")]
#[command(about = "Cardano node conformance testing harness")]
#[command(version)]
struct Cli {
    /// Path to the scenario JSON file to execute
    #[arg(long, default_value = "scenarios/default.json")]
    scenario: PathBuf,

    /// Write Chain-Sync headers received during chain_sync steps to this JSONL
    /// fixture file (for use as fixture_path in serve_chain_sync steps).
    #[arg(long)]
    capture_fixture: Option<PathBuf>,

    /// Write block bodies received during block_fetch steps to this JSONL
    /// fixture file (for use as block_fetch_fixture_path in serve_block_fetch steps).
    #[arg(long)]
    capture_block_fixture: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();

    tracing::info!(scenario = %cli.scenario.display(), "Loading scenario");

    let parsed = scenario::load(&cli.scenario)?;
    tracing::info!(name = %parsed.name, steps = parsed.steps.len(), "Running scenario");

    ScenarioRunner::new(parsed)
        .with_capture_fixture(cli.capture_fixture)
        .with_capture_block_fixture(cli.capture_block_fixture)
        .run()
        .await
}
