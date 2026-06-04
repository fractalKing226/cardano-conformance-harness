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

    /// When set, every RollForward header received by chain_sync steps is
    /// also written to this file as a fixture entry (JSONL format). The
    /// resulting file can be used as fixture_path in serve_chain_sync steps.
    #[arg(long)]
    capture_fixture: Option<PathBuf>,
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
        .run()
        .await
}
