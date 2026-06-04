pub mod fixture;
pub mod response_rules;
pub mod runner;
pub mod vars;

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

/// A single scenario step, storing raw JSON params for uniform variable
/// substitution at execution time.
#[derive(Debug)]
pub struct StepDef {
    pub kind: StepKind,
    /// Raw JSON params map — used for variable substitution before execution.
    /// Keys for `kind`, `output`, `expect` are stripped; everything else lives here.
    pub raw_params: Value,
    /// Variable name to store this step's output in (e.g. `"tip"`, `"pts"`).
    pub output: Option<String>,
    pub expect: Option<Assertions>,
}

impl<'de> Deserialize<'de> for StepDef {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let mut map: serde_json::Map<String, Value> = Deserialize::deserialize(d)?;

        let kind: StepKind = map
            .remove("kind")
            .ok_or_else(|| serde::de::Error::missing_field("kind"))
            .and_then(|v| serde_json::from_value(v).map_err(serde::de::Error::custom))?;

        let output: Option<String> = map
            .remove("output")
            .map(|v| serde_json::from_value(v).map_err(serde::de::Error::custom))
            .transpose()?;

        let expect: Option<Assertions> = map
            .remove("expect")
            .map(|v| serde_json::from_value(v).map_err(serde::de::Error::custom))
            .transpose()?;

        Ok(StepDef {
            kind,
            raw_params: Value::Object(map),
            output,
            expect,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StepKind {
    // ── Client-side ───────────────────────────────────────────────────────────
    Connect,
    Handshake,
    ChainSync,
    BlockFetch,
    QueryTip,
    Repeat,
    Disconnect,
    Sleep,
    // ── Server-side ───────────────────────────────────────────────────────────
    /// Bind a TCP listener socket. Params: `bind_address` (default `"0.0.0.0:3001"`).
    Listen,
    /// Accept the next TCP connection and complete the N2N Handshake as responder.
    AcceptHandshake,
    /// Serve a pre-captured chain fixture to the connected client via Chain-Sync.
    /// Params: `fixture_path` (required), `await_at_tip_secs` (default 30, max 300).
    ServeChainSync,
    /// Stop the listener and close any active server connections.
    CloseListener,
}

impl StepKind {
    pub fn as_str(self) -> &'static str {
        match self {
            StepKind::Connect => "connect",
            StepKind::Handshake => "handshake",
            StepKind::ChainSync => "chain_sync",
            StepKind::BlockFetch => "block_fetch",
            StepKind::QueryTip => "query_tip",
            StepKind::Repeat => "repeat",
            StepKind::Disconnect => "disconnect",
            StepKind::Sleep => "sleep",
            StepKind::Listen => "listen",
            StepKind::AcceptHandshake => "accept_handshake",
            StepKind::ServeChainSync => "serve_chain_sync",
            StepKind::CloseListener => "close_listener",
        }
    }

    pub fn is_client_side(self) -> bool {
        matches!(
            self,
            StepKind::Connect
                | StepKind::Handshake
                | StepKind::ChainSync
                | StepKind::BlockFetch
                | StepKind::QueryTip
                | StepKind::Disconnect
        )
    }

    pub fn is_server_side(self) -> bool {
        matches!(
            self,
            StepKind::Listen
                | StepKind::AcceptHandshake
                | StepKind::ServeChainSync
                | StepKind::CloseListener
        )
    }
}

/// Typed convenience view of a step's parameters. Populated by deserializing
/// the variable-substituted `raw_params` at step-execution time.
#[derive(Debug, Default, Deserialize)]
pub struct StepParams {
    /// Chain-Sync / QueryTip: intersection points. Each is `"origin"`,
    /// `"slot:hex_hash"`, or a `$ref` string.
    pub intersection_points: Option<Vec<String>>,

    /// Chain-Sync: headers to consume. Default 10.
    pub count: Option<u64>,

    /// Chain-Sync: seconds to wait in MustReply. Default 30.
    pub await_timeout_secs: Option<u64>,

    /// Block-Fetch: points to fetch — `"from_chain_sync"` (deprecated),
    /// `"$varname"`, or an array.
    pub points: Option<BlockFetchPoints>,

    /// Block-Fetch: points per range request. Default 1.
    pub batch_size: Option<usize>,

    /// Sleep: seconds to sleep.
    pub duration_secs: Option<u64>,

    /// Repeat: number of iterations (resolved from raw_params after substitution).
    pub times: Option<u64>,

    /// Repeat: inner steps to execute each iteration. Parsed eagerly at load
    /// time so the validator can recurse into the body.
    pub body: Option<Vec<StepDef>>,

    // ── Server-side params ────────────────────────────────────────────────────

    /// Listen: address to bind. Default `"0.0.0.0:3001"`.
    pub bind_address: Option<String>,

    /// ServeChainSync: path to the fixture JSONL file.
    /// Mutually exclusive with `responses`.
    pub fixture_path: Option<String>,

    /// ServeChainSync: explicit response rule list.
    /// Mutually exclusive with `fixture_path`.
    /// Each element is a JSON object matching the `ResponseRuleDef` schema.
    pub responses: Option<Vec<serde_json::Value>>,

    /// ServeChainSync: seconds to hold in MustReply at tip before closing.
    /// Default 30, max 300. Only used when `fixture_path` is set (the auto-
    /// generated script uses this for its AwaitReply rule).
    pub await_at_tip_secs: Option<u64>,
}

/// How Block-Fetch obtains its point list.
#[derive(Debug)]
pub enum BlockFetchPoints {
    /// Use the points collected by the most recent Chain-Sync step.
    /// Deprecated: prefer `output` on the chain_sync step and `"$varname"`.
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
pub fn validate(scenario: &Scenario) -> anyhow::Result<()> {
    let mut errors: Vec<String> = Vec::new();

    // A scenario is either client-mode or server-mode — the two must not mix.
    let has_client = scenario.steps.iter().any(|s| s.kind.is_client_side());
    let has_server = scenario.steps.iter().any(|s| s.kind.is_server_side());
    if has_client && has_server {
        errors.push(
            "scenario mixes client-side steps (connect/handshake/…) and server-side \
             steps (listen/accept_handshake/…) — these must not appear in the same scenario"
                .into(),
        );
    }

    validate_steps(&scenario.steps, &mut errors, &mut false);
    if !errors.is_empty() {
        anyhow::bail!(
            "scenario \"{}\" has validation errors:\n  - {}",
            scenario.name,
            errors.join("\n  - ")
        );
    }
    Ok(())
}

fn validate_steps(steps: &[StepDef], errors: &mut Vec<String>, has_chain_sync: &mut bool) {
    for (i, step) in steps.iter().enumerate() {
        let pos = format!("step[{i}] ({})", step.kind.as_str());
        validate_step(step, &pos, errors, has_chain_sync);
    }
}

fn validate_step(
    step: &StepDef,
    pos: &str,
    errors: &mut Vec<String>,
    has_chain_sync: &mut bool,
) {
    match step.kind {
        StepKind::Connect | StepKind::Disconnect | StepKind::Handshake | StepKind::QueryTip => {}

        StepKind::ChainSync => {
            *has_chain_sync = true;
            if let Some(Value::Array(pts)) = step.raw_params.get("intersection_points") {
                for (j, pv) in pts.iter().enumerate() {
                    if let Value::String(s) = pv {
                        if !s.starts_with('$') {
                            if let Err(e) = parse_point(s) {
                                errors.push(format!(
                                    "{pos}: intersection_points[{j}] \"{s}\" is invalid: {e}"
                                ));
                            }
                        }
                    }
                }
            }
        }

        StepKind::BlockFetch => {
            let points_val = step.raw_params.get("points");
            let is_ref = matches!(points_val, Some(Value::String(s)) if s.starts_with('$'));
            let is_from_cs = points_val.is_none()
                || matches!(points_val, Some(Value::String(s)) if s == "from_chain_sync");
            if is_from_cs && !*has_chain_sync {
                errors.push(format!(
                    "{pos}: points = \"from_chain_sync\" but no chain_sync step precedes this"
                ));
            }
            if !is_ref {
                if let Some(Value::Array(pts)) = points_val {
                    for (j, pv) in pts.iter().enumerate() {
                        if let Value::String(s) = pv {
                            if !s.starts_with('$') {
                                if let Err(e) = parse_point(s) {
                                    errors.push(format!(
                                        "{pos}: points[{j}] \"{s}\" is invalid: {e}"
                                    ));
                                }
                            }
                        }
                    }
                }
            }
        }

        StepKind::Sleep => {
            let d = step.raw_params.get("duration_secs");
            let is_ref = matches!(d, Some(Value::String(s)) if s.starts_with('$'));
            if d.is_none() && !is_ref {
                errors.push(format!("{pos}: sleep step requires duration_secs"));
            }
        }

        StepKind::Repeat => {
            if step.raw_params.get("times").is_none() {
                errors.push(format!("{pos}: repeat step requires times"));
            }
            match step.raw_params.get("body") {
                None => errors.push(format!("{pos}: repeat step requires body")),
                Some(body_val) => match serde_json::from_value::<Vec<StepDef>>(body_val.clone()) {
                    Ok(body_steps) => {
                        // Recurse — body shares outer has_chain_sync so variables
                        // set in a preceding chain_sync are visible inside repeat.
                        validate_steps(&body_steps, errors, has_chain_sync);
                    }
                    Err(e) => errors.push(format!("{pos}: invalid body: {e}")),
                },
            }
        }

        StepKind::Listen | StepKind::AcceptHandshake | StepKind::CloseListener => {
            // No required params for these steps.
        }

        StepKind::ServeChainSync => {
            let has_fixture   = step.raw_params.get("fixture_path").is_some();
            let has_responses = step.raw_params.get("responses").is_some();
            if !has_fixture && !has_responses {
                errors.push(format!(
                    "{pos}: serve_chain_sync requires fixture_path, responses, or both"
                ));
            }
            // fixture_path + responses is allowed: responses drives the script,
            // fixture_path is loaded to resolve header_from_fixture references.
            if let Some(secs) = step.raw_params.get("await_at_tip_secs").and_then(|v| v.as_u64()) {
                if secs > 300 {
                    errors.push(format!("{pos}: await_at_tip_secs {secs} exceeds maximum of 300"));
                }
            }
        }
    }
}

// ── Point parsing ─────────────────────────────────────────────────────────────

/// Parse a point string: `"origin"` → `Point::Origin`, `"slot:hex_hash"` → `Point::Specific`.
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
    let hash = decode_hex(hash_str).with_context(|| format!("invalid hash hex in \"{s}\""))?;
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
            "target_address": "localhost:3001",
            "network_magic": 42,
            "trace_output_path": "trace.jsonl",
            "steps": [
                {"kind": "connect"},
                {"kind": "handshake"},
                {"kind": "chain_sync", "count": 5},
                {"kind": "block_fetch", "points": "from_chain_sync", "batch_size": 2},
                {"kind": "disconnect"}
            ]
        }"#;
        let s = parse(json).unwrap();
        assert_eq!(s.steps.len(), 5);
        assert_eq!(s.steps[2].raw_params["count"], 5);
        assert_eq!(s.steps[3].raw_params["batch_size"], 2);
    }

    #[test]
    fn output_field_parsed_from_step() {
        let json = r#"{
            "name":"t","target_address":"x","network_magic":1,"trace_output_path":"t.jsonl",
            "steps":[
                {"kind":"connect"},{"kind":"handshake"},
                {"kind":"chain_sync","count":5,"output":"my_points"},
                {"kind":"disconnect"}
            ]
        }"#;
        let s: Scenario = serde_json::from_str(json).unwrap();
        assert_eq!(s.steps[2].output.as_deref(), Some("my_points"));
        // output must not leak into raw_params
        assert!(s.steps[2].raw_params.get("output").is_none());
    }

    #[test]
    fn variable_reference_in_intersection_points_is_valid() {
        let json = r#"{
            "name":"t","target_address":"x","network_magic":1,"trace_output_path":"t.jsonl",
            "steps":[
                {"kind":"connect"},{"kind":"handshake"},
                {"kind":"query_tip","output":"tip"},
                {"kind":"chain_sync","intersection_points":["$tip.point"],"count":5},
                {"kind":"disconnect"}
            ]
        }"#;
        parse(json).unwrap();
    }

    #[test]
    fn block_fetch_with_variable_ref_is_valid() {
        let json = r#"{
            "name":"t","target_address":"x","network_magic":1,"trace_output_path":"t.jsonl",
            "steps":[
                {"kind":"connect"},{"kind":"handshake"},
                {"kind":"chain_sync","count":3,"output":"pts"},
                {"kind":"block_fetch","points":"$pts"},
                {"kind":"disconnect"}
            ]
        }"#;
        parse(json).unwrap();
    }

    #[test]
    fn repeat_step_parses_with_body() {
        let json = r#"{
            "name":"t","target_address":"x","network_magic":1,"trace_output_path":"t.jsonl",
            "steps":[
                {"kind":"connect"},{"kind":"handshake"},
                {"kind":"repeat","times":3,"body":[
                    {"kind":"chain_sync","count":2}
                ]},
                {"kind":"disconnect"}
            ]
        }"#;
        let s = parse(json).unwrap();
        assert_eq!(s.steps[2].raw_params["times"], 3);
        let body = s.steps[2].raw_params["body"].as_array().unwrap();
        assert_eq!(body.len(), 1);
    }

    #[test]
    fn repeat_step_with_variable_count_parses() {
        let json = r#"{
            "name":"t","target_address":"x","network_magic":1,"trace_output_path":"t.jsonl",
            "steps":[
                {"kind":"connect"},{"kind":"handshake"},
                {"kind":"repeat","times":"$n","body":[{"kind":"sleep","duration_secs":1}]},
                {"kind":"disconnect"}
            ]
        }"#;
        let s = parse(json).unwrap();
        assert_eq!(s.steps[2].raw_params["times"], "$n");
    }

    #[test]
    fn query_tip_step_parses() {
        let json = r#"{
            "name":"t","target_address":"x","network_magic":1,"trace_output_path":"t.jsonl",
            "steps":[
                {"kind":"connect"},{"kind":"handshake"},
                {"kind":"query_tip","output":"tip"},
                {"kind":"disconnect"}
            ]
        }"#;
        let s = parse(json).unwrap();
        assert_eq!(s.steps[2].kind, StepKind::QueryTip);
        assert_eq!(s.steps[2].output.as_deref(), Some("tip"));
    }

    #[test]
    fn repeat_requires_times() {
        let json = r#"{
            "name":"t","target_address":"x","network_magic":1,"trace_output_path":"t.jsonl",
            "steps":[{"kind":"connect"},{"kind":"repeat","body":[]},{"kind":"disconnect"}]
        }"#;
        let s: Scenario = serde_json::from_str(json).unwrap();
        assert!(validate(&s).is_err());
    }

    #[test]
    fn repeat_requires_body() {
        let json = r#"{
            "name":"t","target_address":"x","network_magic":1,"trace_output_path":"t.jsonl",
            "steps":[{"kind":"connect"},{"kind":"repeat","times":3},{"kind":"disconnect"}]
        }"#;
        let s: Scenario = serde_json::from_str(json).unwrap();
        assert!(validate(&s).is_err());
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
        assert_eq!(s.steps[3].raw_params["points"], "from_chain_sync");
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
        assert!(parse_point("abc:gg").is_err());
        assert!(parse_point("notanumber:aabb").is_err());
    }
}
