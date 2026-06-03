pub mod runner;

use std::path::PathBuf;

use anyhow::Context as _;
use pallas_network::miniprotocols::Point;
use serde::{Deserialize, Serialize};
use serde_json::Value;

// ── Public types ─────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct Scenario {
    pub name: String,
    pub description: Option<String>,
    pub target_address: String,
    pub network_magic: u64,
    pub trace_output_path: PathBuf,
    pub expected_outcome: Option<String>,
    pub steps: Vec<StepDef>,
}

#[derive(Debug, Deserialize)]
pub struct StepDef {
    pub kind: StepKind,
    #[serde(flatten)]
    pub params: StepParams,
    pub expect: Option<Assertions>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StepKind {
    Connect,
    Handshake,
    ChainSync,
    BlockFetch,
    Disconnect,
    Sleep,
}

impl StepKind {
    pub fn as_str(self) -> &'static str {
        match self {
            StepKind::Connect => "connect",
            StepKind::Handshake => "handshake",
            StepKind::ChainSync => "chain_sync",
            StepKind::BlockFetch => "block_fetch",
            StepKind::Disconnect => "disconnect",
            StepKind::Sleep => "sleep",
        }
    }
}

/// All possible step parameters in a single flat struct. Fields that don't
/// apply to the current step kind are simply ignored by the runner.
#[derive(Debug, Default, Deserialize)]
pub struct StepParams {
    /// Chain-Sync: list of points to intersect at. Each is "origin" or
    /// "slot:hex_hash". Defaults to ["origin"].
    pub intersection_points: Option<Vec<String>>,

    /// Chain-Sync: number of headers to consume. Default 10.
    pub count: Option<u64>,

    /// Chain-Sync: seconds to wait in MustReply state. Default 30.
    pub await_timeout_secs: Option<u64>,

    /// Block-Fetch: which points to fetch. "from_chain_sync" (default) or an
    /// array of "slot:hex_hash" strings.
    pub points: Option<BlockFetchPoints>,

    /// Block-Fetch: points per range request. Default 1.
    pub batch_size: Option<usize>,

    /// Sleep: seconds to sleep.
    pub duration_secs: Option<u64>,
}

/// How Block-Fetch obtains its point list.
#[derive(Debug)]
pub enum BlockFetchPoints {
    /// Use the points collected by the most recent Chain-Sync step.
    FromChainSync,
    /// Fetch these specific points (each encoded as "slot:hex_hash").
    Explicit(Vec<String>),
}

impl<'de> Deserialize<'de> for BlockFetchPoints {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let v = Value::deserialize(d)?;
        match &v {
            Value::String(s) if s == "from_chain_sync" => Ok(BlockFetchPoints::FromChainSync),
            Value::Array(_) => {
                let pts: Vec<String> =
                    serde_json::from_value(v).map_err(serde::de::Error::custom)?;
                Ok(BlockFetchPoints::Explicit(pts))
            }
            _ => Err(serde::de::Error::custom(
                "expected \"from_chain_sync\" or an array of \"slot:hex_hash\" strings",
            )),
        }
    }
}

#[derive(Debug, Default, Deserialize)]
pub struct Assertions {
    pub min_events: Option<usize>,
    pub must_contain_kind: Option<Vec<String>>,
    pub must_not_contain_kind: Option<Vec<String>>,
}

// ── Parsing ───────────────────────────────────────────────────────────────────

/// Load and validate a scenario from a JSON file.
pub fn load(path: &std::path::Path) -> anyhow::Result<Scenario> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("reading scenario file {}", path.display()))?;
    let scenario: Scenario = serde_json::from_str(&text)
        .with_context(|| format!("parsing scenario file {}", path.display()))?;
    validate(&scenario)?;
    Ok(scenario)
}

/// Validate scenario-level constraints that serde cannot enforce.
///
/// Connection lifecycle is unrestricted — scenarios may have multiple
/// connect/disconnect cycles in any order. The harness subscribes to all
/// supported protocols on every connect, so no scan-ahead is needed.
///
/// What IS validated:
/// - `block_fetch` with `from_chain_sync` requires a preceding `chain_sync`
///   in the same linear sequence (tracks the most recent chain_sync seen so far)
/// - `sleep` requires `duration_secs`
/// - Point strings in `intersection_points` and explicit `points` must parse
pub fn validate(scenario: &Scenario) -> anyhow::Result<()> {
    let mut errors: Vec<String> = Vec::new();
    let mut has_chain_sync = false;

    for (i, step) in scenario.steps.iter().enumerate() {
        let pos = format!("step[{i}] ({})", step.kind.as_str());

        match step.kind {
            StepKind::Connect | StepKind::Disconnect | StepKind::Handshake => {}

            StepKind::ChainSync => {
                has_chain_sync = true;
                if let Some(pts) = &step.params.intersection_points {
                    for (j, s) in pts.iter().enumerate() {
                        if let Err(e) = parse_point(s) {
                            errors.push(format!(
                                "{pos}: intersection_points[{j}] \"{s}\" is invalid: {e}"
                            ));
                        }
                    }
                }
            }

            StepKind::BlockFetch => {
                let uses_from_cs = matches!(
                    step.params.points,
                    None | Some(BlockFetchPoints::FromChainSync)
                );
                if uses_from_cs && !has_chain_sync {
                    errors.push(format!(
                        "{pos}: points = \"from_chain_sync\" but no chain_sync step precedes this"
                    ));
                }
                if let Some(BlockFetchPoints::Explicit(pts)) = &step.params.points {
                    for (j, s) in pts.iter().enumerate() {
                        if let Err(e) = parse_point(s) {
                            errors.push(format!(
                                "{pos}: points[{j}] \"{s}\" is invalid: {e}"
                            ));
                        }
                    }
                }
            }

            StepKind::Sleep => {
                if step.params.duration_secs.is_none() {
                    errors.push(format!("{pos}: sleep step requires duration_secs"));
                }
            }
        }
    }

    if !errors.is_empty() {
        anyhow::bail!(
            "scenario \"{}\" has validation errors:\n  - {}",
            scenario.name,
            errors.join("\n  - ")
        );
    }
    Ok(())
}

// ── Point parsing ─────────────────────────────────────────────────────────────

/// Parse a point string: "origin" → `Point::Origin`, "slot:hex_hash" → `Point::Specific`.
pub fn parse_point(s: &str) -> anyhow::Result<Point> {
    if s == "origin" {
        return Ok(Point::Origin);
    }
    let (slot_str, hash_str) = s
        .split_once(':')
        .ok_or_else(|| anyhow::anyhow!("expected \"origin\" or \"slot:hex_hash\", got \"{s}\""))?;
    let slot: u64 = slot_str
        .parse()
        .with_context(|| format!("invalid slot in \"{s}\""))?;
    let hash = decode_hex(hash_str)
        .with_context(|| format!("invalid hash hex in \"{s}\""))?;
    Ok(Point::Specific(slot, hash))
}

fn decode_hex(s: &str) -> anyhow::Result<Vec<u8>> {
    anyhow::ensure!(s.len() % 2 == 0, "odd-length hex string");
    (0..s.len())
        .step_by(2)
        .map(|i| {
            u8::from_str_radix(&s[i..i + 2], 16)
                .map_err(|e| anyhow::anyhow!("invalid hex byte at offset {i}: {e}"))
        })
        .collect()
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(json: &str) -> anyhow::Result<Scenario> {
        let s: Scenario = serde_json::from_str(json)?;
        validate(&s)?;
        Ok(s)
    }

    const MINIMAL: &str = r#"{
        "name": "test",
        "target_address": "localhost:3001",
        "network_magic": 42,
        "trace_output_path": "trace.jsonl",
        "steps": [
            {"kind": "connect"},
            {"kind": "disconnect"}
        ]
    }"#;

    #[test]
    fn minimal_scenario_parses() {
        let s = parse(MINIMAL).unwrap();
        assert_eq!(s.name, "test");
        assert_eq!(s.steps.len(), 2);
    }

    #[test]
    fn full_scenario_parses() {
        let json = r#"{
            "name": "full",
            "description": "A complete scenario",
            "target_address": "localhost:3001",
            "network_magic": 42,
            "trace_output_path": "trace.jsonl",
            "expected_outcome": "success",
            "steps": [
                {"kind": "connect"},
                {"kind": "handshake"},
                {"kind": "chain_sync", "count": 5, "await_timeout_secs": 10},
                {"kind": "block_fetch", "points": "from_chain_sync", "batch_size": 2},
                {"kind": "disconnect"}
            ]
        }"#;
        let s = parse(json).unwrap();
        assert_eq!(s.steps.len(), 5);
        assert_eq!(s.steps[2].params.count, Some(5));
        assert_eq!(s.steps[3].params.batch_size, Some(2));
    }

    #[test]
    fn from_chain_sync_points_parses() {
        let json = r#"{
            "name": "t", "target_address": "x", "network_magic": 1,
            "trace_output_path": "t.jsonl",
            "steps": [
                {"kind": "connect"}, {"kind": "handshake"},
                {"kind": "chain_sync", "count": 3},
                {"kind": "block_fetch", "points": "from_chain_sync"},
                {"kind": "disconnect"}
            ]
        }"#;
        let s: Scenario = serde_json::from_str(json).unwrap();
        assert!(matches!(
            s.steps[3].params.points,
            Some(BlockFetchPoints::FromChainSync)
        ));
    }

    #[test]
    fn explicit_points_parse() {
        let json = r#"{
            "name": "t", "target_address": "x", "network_magic": 1,
            "trace_output_path": "t.jsonl",
            "steps": [
                {"kind": "connect"}, {"kind": "handshake"},
                {"kind": "chain_sync"},
                {"kind": "block_fetch", "points": ["62:aabbcc"]},
                {"kind": "disconnect"}
            ]
        }"#;
        let s: Scenario = serde_json::from_str(json).unwrap();
        assert!(matches!(
            &s.steps[3].params.points,
            Some(BlockFetchPoints::Explicit(v)) if v[0] == "62:aabbcc"
        ));
    }

    #[test]
    fn missing_name_fails() {
        let json = r#"{"target_address":"x","network_magic":1,"trace_output_path":"t.jsonl","steps":[]}"#;
        assert!(serde_json::from_str::<Scenario>(json).is_err());
    }

    #[test]
    fn invalid_step_kind_fails() {
        let json = r#"{
            "name":"t","target_address":"x","network_magic":1,"trace_output_path":"t.jsonl",
            "steps":[{"kind":"fly_to_moon"}]
        }"#;
        assert!(serde_json::from_str::<Scenario>(json).is_err());
    }

    #[test]
    fn multiple_connect_disconnect_cycles_are_valid() {
        // Option C: no restriction on connection lifecycle.
        let json = r#"{
            "name":"t","target_address":"x","network_magic":1,"trace_output_path":"t.jsonl",
            "steps":[
                {"kind":"connect"},{"kind":"handshake"},{"kind":"disconnect"},
                {"kind":"connect"},{"kind":"handshake"},{"kind":"disconnect"}
            ]
        }"#;
        let s: Scenario = serde_json::from_str(json).unwrap();
        assert!(validate(&s).is_ok());
    }

    #[test]
    fn validate_rejects_block_fetch_without_prior_chain_sync() {
        let json = r#"{
            "name":"t","target_address":"x","network_magic":1,"trace_output_path":"t.jsonl",
            "steps":[
                {"kind":"connect"},{"kind":"handshake"},
                {"kind":"block_fetch"},
                {"kind":"disconnect"}
            ]
        }"#;
        let s: Scenario = serde_json::from_str(json).unwrap();
        assert!(validate(&s).is_err());
    }

    #[test]
    fn validate_sleep_requires_duration() {
        let json = r#"{
            "name":"t","target_address":"x","network_magic":1,"trace_output_path":"t.jsonl",
            "steps":[{"kind":"connect"},{"kind":"sleep"},{"kind":"disconnect"}]
        }"#;
        let s: Scenario = serde_json::from_str(json).unwrap();
        assert!(validate(&s).is_err());
    }

    #[test]
    fn parse_point_origin() {
        assert!(matches!(parse_point("origin").unwrap(), Point::Origin));
    }

    #[test]
    fn parse_point_specific() {
        let p = parse_point("62:aabb").unwrap();
        assert!(matches!(p, Point::Specific(62, ref h) if h == &[0xaa, 0xbb]));
    }

    #[test]
    fn parse_point_invalid_format() {
        assert!(parse_point("notapoint").is_err());
        assert!(parse_point("abc:gg").is_err()); // invalid hex
        assert!(parse_point("notanumber:aabb").is_err());
    }
}
