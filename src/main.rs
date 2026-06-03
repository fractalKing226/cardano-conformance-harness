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

    ScenarioRunner::new(parsed).run().await
}
