pub mod block_fixture;
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

/// One peer in the imaginary network.
/// Each peer holds an identity (`id`) and optional fixture paths for the
/// protocols it can serve. Steps reference peers by `id` via `as_peer:`.
#[derive(Debug, Clone, Deserialize)]
pub struct Peer {
    pub id: String,
    /// Path to the chain-sync fixture this peer serves (`serve_chain_sync as_peer:`).
    pub chain_sync_fixture:  Option<String>,
    /// Path to the block-fetch fixture this peer serves (`serve_block_fetch as_peer:`).
    pub block_fetch_fixture: Option<String>,
    pub description:         Option<String>,
}

/// Top-level imaginary-network declaration — a list of named peers with their
/// assigned chains, and optional temporal parameters. Scenarios without this
/// field behave exactly as before.
#[derive(Debug, Clone, Deserialize)]
pub struct Network {
    pub peers: Vec<Peer>,
    /// The slot the imaginary network starts at. Defaults to 0.
    /// Initialized into `RunnerState::current_slot` at scenario start.
    pub start_slot:     Option<u64>,
    /// Wall-clock milliseconds per slot — vocabulary reserved for future
    /// wall-clock-driven slot advancement. Not acted on in this slice.
    pub slot_length_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct Scenario {
    pub name: String,
    pub description: Option<String>,
    pub target_address: Option<String>,
    pub network_magic: u64,
    pub trace_output_path: PathBuf,
    pub expected_outcome: Option<String>,
    /// Optional imaginary-network declaration. When present, peers are named
    /// here and `serve_*` steps can reference them via `as_peer:`.
    pub network: Option<Network>,
    pub steps: Vec<StepDef>,
}

/// A single scenario step, storing raw JSON params for uniform variable
/// substitution at execution time.
#[derive(Debug, Clone)]
pub struct StepDef {
    pub kind: StepKind,
    /// Raw JSON params map — used for variable substitution before execution.
    /// Keys for `kind`, `output`, `expect`, `as`, `on` are stripped.
    pub raw_params: Value,
    /// Variable name to store this step's output in (e.g. `"tip"`, `"pts"`).
    pub output: Option<String>,
    /// Connection name to create (`connect`, `listen`, `accept_handshake`).
    /// Absent → use `"default"`.
    pub as_name: Option<String>,
    /// Connection name to act on (most protocol steps).
    /// Absent → use `"default"`.
    pub on_name: Option<String>,
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

        // `as` is a Rust keyword, deserialized via string key.
        let as_name: Option<String> = map
            .remove("as")
            .map(|v| serde_json::from_value(v).map_err(serde::de::Error::custom))
            .transpose()?;

        let on_name: Option<String> = map
            .remove("on")
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
            as_name,
            on_name,
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
    /// Serve block bodies to the connected client via Block-Fetch.
    ServeBlockFetch,
    /// Stop the listener and close any active server connections.
    CloseListener,
    /// Run multiple step branches concurrently. Completes when all branches
    /// complete, or aborts remaining branches if one fails.
    Parallel,
    /// Emit a peer-identity event into the trace without touching any
    /// connection. Models imaginary-network state changes (a peer produced a
    /// block, cast a vote, etc.) as first-class trace entries.
    EmitPeerEvent,
    /// Advance the imaginary network clock to an absolute slot number.
    /// Errors if the target slot is not strictly greater than the current slot.
    AdvanceToSlot,
    /// Advance the imaginary network clock by a relative number of slots.
    /// `count` must be at least 1.
    TickSlots,
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
            StepKind::ServeChainSync  => "serve_chain_sync",
            StepKind::ServeBlockFetch => "serve_block_fetch",
            StepKind::CloseListener   => "close_listener",
            StepKind::Parallel        => "parallel",
            StepKind::EmitPeerEvent   => "emit_peer_event",
            StepKind::AdvanceToSlot   => "advance_to_slot",
            StepKind::TickSlots       => "tick_slots",
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
                | StepKind::ServeBlockFetch
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

    /// ServeBlockFetch: path to the Block-Fetch fixture JSONL file.
    pub block_fetch_fixture_path: Option<String>,

    /// ServeBlockFetch: if true, auto-generated script declines all ranges with NoBlocks.
    pub no_blocks_default: Option<bool>,

    /// ServeChainSync: path to the Chain-Sync fixture JSONL file.
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

    /// Connect: override the scenario-level target address for this connection only.
    pub target_address: Option<String>,

    /// connect / accept_handshake / listen: peer identity label for trace
    /// attribution. Stored on the connection and propagated to all wire events.
    /// emit_peer_event: the peer this event describes.
    pub peer_id: Option<String>,

    /// emit_peer_event: the kind of network event (e.g. "peer_produced_block").
    pub event_kind: Option<String>,

    /// serve_chain_sync / serve_block_fetch: resolve fixture and peer_id from a
    /// named peer in the scenario's network declaration. Mutually exclusive with
    /// the corresponding `fixture_path` / `block_fetch_fixture_path` parameter.
    /// When present, the serving connection automatically acquires the peer's id
    /// as its `peer_id`, overriding any peer_id set at accept_handshake time.
    pub as_peer: Option<String>,

    /// advance_to_slot: absolute target slot number.
    pub slot: Option<u64>,
}

/// How Block-Fetch obtains its point list.
#[derive(Debug, Clone)]
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

#[derive(Debug, Clone, Default, Deserialize)]
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
    validate_connection_names(&scenario.steps, &mut errors);
    validate_connect_addresses(scenario, &mut errors);
    validate_network_declaration(scenario, &mut errors);
    validate_as_peer_refs(&scenario.steps, scenario.network.as_ref(), &mut errors);
    if !errors.is_empty() {
        anyhow::bail!(
            "scenario \"{}\" has validation errors:\n  - {}",
            scenario.name,
            errors.join("\n  - ")
        );
    }
    Ok(())
}

/// Validates `as`/`on` connection-name references across a step list.
///
/// Rules:
/// - `as` names must be unique (no duplicate creation in the same lifecycle).
/// - `on` names must refer to a connection opened by an earlier step.
/// - Connections left open at the end produce a warning (not an error).
fn validate_connection_names(steps: &[StepDef], errors: &mut Vec<String>) {
    use std::collections::HashSet;
    let mut open_clients: HashSet<String> = HashSet::new();
    let mut open_listeners: HashSet<String> = HashSet::new();
    let mut open_servers: HashSet<String> = HashSet::new();

    for (i, step) in steps.iter().enumerate() {
        let pos = format!("step[{i}] ({})", step.kind.as_str());
        let as_name = step.as_name.as_deref().unwrap_or("default").to_string();
        let on_name = step.on_name.as_deref().unwrap_or("default").to_string();

        match step.kind {
            StepKind::Connect => {
                if open_clients.contains(&as_name) {
                    errors.push(format!(
                        "{pos}: connection name \"{as_name}\" is already open (use a different `as` name or disconnect first)"
                    ));
                }
                open_clients.insert(as_name);
            }
            StepKind::Listen => {
                if open_listeners.contains(&as_name) {
                    errors.push(format!(
                        "{pos}: listener name \"{as_name}\" is already open"
                    ));
                }
                open_listeners.insert(as_name);
            }
            StepKind::AcceptHandshake => {
                // `on` refers to the listener; `as` names the new server connection.
                if !open_listeners.contains(&on_name) {
                    errors.push(format!(
                        "{pos}: no listener named \"{on_name}\" (create it with `listen as: \"{on_name}\"`)"
                    ));
                }
                if open_servers.contains(&as_name) {
                    errors.push(format!(
                        "{pos}: server connection name \"{as_name}\" is already open"
                    ));
                }
                open_servers.insert(as_name);
            }
            StepKind::Handshake
            | StepKind::ChainSync
            | StepKind::BlockFetch
            | StepKind::QueryTip => {
                if !open_clients.contains(&on_name) {
                    errors.push(format!(
                        "{pos}: no connection named \"{on_name}\" (create it with `connect as: \"{on_name}\"`)"
                    ));
                }
            }
            StepKind::Disconnect => {
                if !open_clients.contains(&on_name) {
                    errors.push(format!(
                        "{pos}: no connection named \"{on_name}\" to disconnect"
                    ));
                }
                open_clients.remove(&on_name);
            }
            StepKind::ServeChainSync | StepKind::ServeBlockFetch => {
                if !open_servers.contains(&on_name) {
                    errors.push(format!(
                        "{pos}: no server connection named \"{on_name}\" (create it with `accept_handshake as: \"{on_name}\"`)"
                    ));
                }
            }
            StepKind::CloseListener => {
                if !open_listeners.contains(&on_name) {
                    errors.push(format!(
                        "{pos}: no listener named \"{on_name}\" to close"
                    ));
                }
                open_listeners.remove(&on_name);
                // Server connections from this listener remain valid until closed.
            }
            StepKind::Repeat | StepKind::Parallel => {
                // Body/branch validation is deferred; connection-name tracking
                // across nested step lists is not enforced here.
            }
            StepKind::Sleep | StepKind::EmitPeerEvent
            | StepKind::AdvanceToSlot | StepKind::TickSlots => {}
        }
    }
    // Warn about unclosed connections (not an error — cleanup handles them).
    // We intentionally omit these from `errors` to avoid breaking valid scenarios
    // that rely on the runner's cleanup path rather than explicit disconnect steps.
}

/// Checks that every `connect` step has a usable target address: either its own
/// `target_address` param or the scenario-level `target_address` field.
fn validate_connect_addresses(scenario: &Scenario, errors: &mut Vec<String>) {
    let mut connect_steps: Vec<(usize, bool)> = Vec::new(); // (flat index, has per-step addr)
    collect_connect_steps(&scenario.steps, &mut 0, &mut connect_steps);
    let needs_default = connect_steps.iter().any(|(_, has_own)| !has_own);
    if needs_default && scenario.target_address.is_none() {
        errors.push(
            "scenario-level target_address is required because at least one connect step \
             does not specify its own target_address — either add target_address to the \
             scenario or add target_address to every connect step"
                .into(),
        );
    }
}

fn collect_connect_steps(steps: &[StepDef], counter: &mut usize, out: &mut Vec<(usize, bool)>) {
    for step in steps {
        let idx = *counter;
        *counter += 1;
        if step.kind == StepKind::Connect {
            let has_own = step.raw_params.get("target_address").is_some();
            out.push((idx, has_own));
        }
        if let Some(body_val) = step.raw_params.get("body") {
            if let Ok(body) = serde_json::from_value::<Vec<StepDef>>(body_val.clone()) {
                collect_connect_steps(&body, counter, out);
            }
        }
        if let Some(branches_val) = step.raw_params.get("branches") {
            if let Ok(branches) = serde_json::from_value::<Vec<Vec<StepDef>>>(branches_val.clone()) {
                for branch in &branches {
                    collect_connect_steps(branch, counter, out);
                }
            }
        }
    }
}

/// Validates the `network` block: unique peer ids, each peer has at least one fixture.
fn validate_network_declaration(scenario: &Scenario, errors: &mut Vec<String>) {
    let Some(ref network) = scenario.network else { return };
    let mut seen: std::collections::HashSet<&str> = std::collections::HashSet::new();
    for peer in &network.peers {
        if !seen.insert(peer.id.as_str()) {
            errors.push(format!("network: duplicate peer id \"{}\"", peer.id));
        }
        if peer.chain_sync_fixture.is_none() && peer.block_fetch_fixture.is_none() {
            errors.push(format!(
                "network: peer \"{}\" has no fixtures — add chain_sync_fixture or block_fetch_fixture",
                peer.id
            ));
        }
    }
}

/// Validates that every `as_peer` reference in the step tree names a declared peer.
/// If there is no network block but a step uses `as_peer`, that is also an error.
fn validate_as_peer_refs(steps: &[StepDef], network: Option<&Network>, errors: &mut Vec<String>) {
    walk_as_peer_refs(steps, network, errors);
}

fn walk_as_peer_refs(steps: &[StepDef], network: Option<&Network>, errors: &mut Vec<String>) {
    for (i, step) in steps.iter().enumerate() {
        let pos = format!("step[{i}] ({})", step.kind.as_str());
        if let Some(Value::String(as_peer_id)) = step.raw_params.get("as_peer") {
            match network {
                None => errors.push(format!(
                    "{pos}: as_peer requires a network declaration in the scenario"
                )),
                Some(net) => {
                    if !net.peers.iter().any(|p| &p.id == as_peer_id) {
                        errors.push(format!(
                            "{pos}: as_peer \"{as_peer_id}\" is not declared in the network block"
                        ));
                    }
                }
            }
        }
        // Recurse into repeat bodies and parallel branches.
        if let Some(body_val) = step.raw_params.get("body") {
            if let Ok(body) = serde_json::from_value::<Vec<StepDef>>(body_val.clone()) {
                walk_as_peer_refs(&body, network, errors);
            }
        }
        if let Some(branches_val) = step.raw_params.get("branches") {
            if let Ok(branches) = serde_json::from_value::<Vec<Vec<StepDef>>>(branches_val.clone()) {
                for branch in &branches {
                    walk_as_peer_refs(branch, network, errors);
                }
            }
        }
    }
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
    if step.raw_params.get("target_address").is_some() && !matches!(step.kind, StepKind::Connect) {
        errors.push(format!("{pos}: target_address is only valid on connect steps"));
    }
    if step.raw_params.get("peer_id").is_some()
        && !matches!(
            step.kind,
            StepKind::Connect | StepKind::AcceptHandshake | StepKind::EmitPeerEvent
        )
    {
        errors.push(format!(
            "{pos}: peer_id is only valid on connect, accept_handshake, and emit_peer_event steps"
        ));
    }

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

        StepKind::Listen | StepKind::AcceptHandshake | StepKind::CloseListener => {}

        StepKind::Parallel => {
            match step.raw_params.get("branches") {
                None => errors.push(format!("{pos}: parallel step requires branches")),
                Some(Value::Array(arr)) if arr.is_empty() =>
                    errors.push(format!("{pos}: parallel step requires at least one branch")),
                Some(Value::Array(arr)) => {
                    for (b, branch_val) in arr.iter().enumerate() {
                        match serde_json::from_value::<Vec<StepDef>>(branch_val.clone()) {
                            Ok(body) => validate_steps(&body, errors, has_chain_sync),
                            Err(e)   => errors.push(format!("{pos}: branches[{b}]: {e}")),
                        }
                    }
                }
                Some(_) => errors.push(format!("{pos}: parallel branches must be an array")),
            }
        }

        StepKind::ServeBlockFetch => {
            let has_fixture   = step.raw_params.get("block_fetch_fixture_path").is_some();
            let has_responses = step.raw_params.get("responses").is_some();
            let has_as_peer   = step.raw_params.get("as_peer").is_some();
            if has_fixture && has_as_peer {
                errors.push(format!(
                    "{pos}: block_fetch_fixture_path and as_peer are mutually exclusive"
                ));
            }
            if !has_fixture && !has_responses && !has_as_peer {
                errors.push(format!(
                    "{pos}: serve_block_fetch requires block_fetch_fixture_path, as_peer, responses, or both"
                ));
            }
        }

        StepKind::EmitPeerEvent => {
            if step.raw_params.get("peer_id").is_none() {
                errors.push(format!("{pos}: emit_peer_event requires peer_id"));
            }
            if step.raw_params.get("event_kind").is_none() {
                errors.push(format!("{pos}: emit_peer_event requires event_kind"));
            }
        }

        StepKind::AdvanceToSlot => {
            if step.raw_params.get("slot").is_none() {
                errors.push(format!("{pos}: advance_to_slot requires slot"));
            }
            // Rewind validation (new_slot <= current) is a runtime check —
            // the current slot is not known at parse time.
        }

        StepKind::TickSlots => {
            let count_val = step.raw_params.get("count");
            let is_ref = matches!(count_val, Some(Value::String(s)) if s.starts_with('$'));
            if is_ref { /* variable reference — can't validate count at parse time */ }
            else {
                match count_val {
                    None => errors.push(format!("{pos}: tick_slots requires count")),
                    Some(v) => {
                        if v.as_u64().map_or(true, |n| n < 1) {
                            errors.push(format!("{pos}: tick_slots: count must be at least 1"));
                        }
                    }
                }
            }
        }

        StepKind::ServeChainSync => {
            let has_fixture   = step.raw_params.get("fixture_path").is_some();
            let has_responses = step.raw_params.get("responses").is_some();
            let has_as_peer   = step.raw_params.get("as_peer").is_some();
            if has_fixture && has_as_peer {
                errors.push(format!(
                    "{pos}: fixture_path and as_peer are mutually exclusive"
                ));
            }
            if !has_fixture && !has_responses && !has_as_peer {
                errors.push(format!(
                    "{pos}: serve_chain_sync requires fixture_path, as_peer, or responses"
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
    fn connect_per_step_target_address_overrides_scenario_level() {
        // All connect steps have their own target_address — scenario-level field may be absent.
        let json = r#"{
            "name":"t","network_magic":1,"trace_output_path":"t.jsonl",
            "steps":[
                {"kind":"connect","target_address":"host-a:3001"},
                {"kind":"handshake"},
                {"kind":"disconnect"}
            ]
        }"#;
        let s: Scenario = serde_json::from_str(json).unwrap();
        assert!(validate(&s).is_ok(), "should accept absent scenario-level target_address when all connects override");
        assert!(s.target_address.is_none());
        assert_eq!(s.steps[0].raw_params["target_address"], "host-a:3001");
    }

    #[test]
    fn connect_without_per_step_address_requires_scenario_level() {
        // A connect step with no per-step target_address and no scenario-level field.
        let json = r#"{
            "name":"t","network_magic":1,"trace_output_path":"t.jsonl",
            "steps":[{"kind":"connect"},{"kind":"disconnect"}]
        }"#;
        let s: Scenario = serde_json::from_str(json).unwrap();
        let err = validate(&s).unwrap_err().to_string();
        assert!(err.contains("target_address"), "error should mention target_address: {err}");
    }

    #[test]
    fn target_address_on_non_connect_step_is_rejected() {
        let json = r#"{
            "name":"t","target_address":"x","network_magic":1,"trace_output_path":"t.jsonl",
            "steps":[
                {"kind":"connect"},
                {"kind":"handshake","target_address":"sneaky:9999"},
                {"kind":"disconnect"}
            ]
        }"#;
        let s: Scenario = serde_json::from_str(json).unwrap();
        let err = validate(&s).unwrap_err().to_string();
        assert!(err.contains("target_address is only valid on connect steps"), "{err}");
    }

    // ── Network declaration tests ─────────────────────────────────────────────

    fn server_with_network(network_json: &str, steps_json: &str) -> String {
        format!(r#"{{
            "name":"t","network_magic":1,"trace_output_path":"t.jsonl",
            "network": {network_json},
            "steps": [{steps_json}]
        }}"#)
    }

    #[test]
    fn network_declaration_parses_and_validates() {
        let json = server_with_network(
            r#"{ "peers": [{ "id": "p1", "chain_sync_fixture": "f.jsonl" }] }"#,
            r#"{ "kind": "listen", "bind_address": "0.0.0.0:9999" },
               { "kind": "accept_handshake" },
               { "kind": "serve_chain_sync", "as_peer": "p1" }"#,
        );
        let s: Scenario = serde_json::from_str(&json).unwrap();
        assert!(validate(&s).is_ok());
        let net = s.network.unwrap();
        assert_eq!(net.peers.len(), 1);
        assert_eq!(net.peers[0].id, "p1");
    }

    #[test]
    fn validate_rejects_duplicate_peer_ids() {
        let json = server_with_network(
            r#"{ "peers": [
                { "id": "same", "chain_sync_fixture": "a.jsonl" },
                { "id": "same", "chain_sync_fixture": "b.jsonl" }
            ] }"#,
            r#"{ "kind": "listen" }"#,
        );
        let s: Scenario = serde_json::from_str(&json).unwrap();
        let err = validate(&s).unwrap_err().to_string();
        assert!(err.contains("duplicate peer id"), "{err}");
    }

    #[test]
    fn validate_rejects_peer_without_fixtures() {
        let json = server_with_network(
            r#"{ "peers": [{ "id": "empty_peer" }] }"#,
            r#"{ "kind": "listen" }"#,
        );
        let s: Scenario = serde_json::from_str(&json).unwrap();
        let err = validate(&s).unwrap_err().to_string();
        assert!(err.contains("no fixtures"), "{err}");
    }

    #[test]
    fn validate_rejects_as_peer_without_network_block() {
        let json = r#"{
            "name":"t","network_magic":1,"trace_output_path":"t.jsonl",
            "steps":[
                { "kind": "listen" },
                { "kind": "accept_handshake" },
                { "kind": "serve_chain_sync", "as_peer": "nobody" }
            ]
        }"#;
        let s: Scenario = serde_json::from_str(json).unwrap();
        let err = validate(&s).unwrap_err().to_string();
        assert!(err.contains("requires a network declaration"), "{err}");
    }

    #[test]
    fn validate_rejects_as_peer_referencing_unknown_peer() {
        let json = server_with_network(
            r#"{ "peers": [{ "id": "known", "chain_sync_fixture": "f.jsonl" }] }"#,
            r#"{ "kind": "listen" },
               { "kind": "accept_handshake" },
               { "kind": "serve_chain_sync", "as_peer": "unknown_peer" }"#,
        );
        let s: Scenario = serde_json::from_str(&json).unwrap();
        let err = validate(&s).unwrap_err().to_string();
        assert!(err.contains("not declared in the network block"), "{err}");
    }

    #[test]
    fn validate_rejects_as_peer_and_fixture_path_together() {
        let json = server_with_network(
            r#"{ "peers": [{ "id": "p1", "chain_sync_fixture": "f.jsonl" }] }"#,
            r#"{ "kind": "listen" },
               { "kind": "accept_handshake" },
               { "kind": "serve_chain_sync", "as_peer": "p1", "fixture_path": "other.jsonl" }"#,
        );
        let s: Scenario = serde_json::from_str(&json).unwrap();
        let err = validate(&s).unwrap_err().to_string();
        assert!(err.contains("mutually exclusive"), "{err}");
    }

    // ── Slot-evolution step validation tests ─────────────────────────────────

    #[test]
    fn advance_to_slot_requires_slot_param() {
        let json = server_with_network(
            r#"{ "peers": [{ "id": "p", "chain_sync_fixture": "f.jsonl" }] }"#,
            r#"{ "kind": "listen" },
               { "kind": "accept_handshake" },
               { "kind": "advance_to_slot" }"#,
        );
        let s: Scenario = serde_json::from_str(&json).unwrap();
        let err = validate(&s).unwrap_err().to_string();
        assert!(err.contains("advance_to_slot requires slot"), "{err}");
    }

    #[test]
    fn tick_slots_requires_count_param() {
        let json = r#"{
            "name":"t","network_magic":1,"trace_output_path":"t.jsonl",
            "network":{"peers":[{"id":"p","chain_sync_fixture":"f.jsonl"}]},
            "steps":[{"kind":"tick_slots"}]
        }"#;
        let s: Scenario = serde_json::from_str(json).unwrap();
        let err = validate(&s).unwrap_err().to_string();
        assert!(err.contains("tick_slots requires count"), "{err}");
    }

    #[test]
    fn tick_slots_rejects_count_zero() {
        let json = r#"{
            "name":"t","network_magic":1,"trace_output_path":"t.jsonl",
            "network":{"peers":[{"id":"p","chain_sync_fixture":"f.jsonl"}]},
            "steps":[{"kind":"tick_slots","count":0}]
        }"#;
        let s: Scenario = serde_json::from_str(json).unwrap();
        let err = validate(&s).unwrap_err().to_string();
        assert!(err.contains("count must be at least 1"), "{err}");
    }

    #[test]
    fn tick_slots_count_one_is_valid() {
        let json = r#"{
            "name":"t","network_magic":1,"trace_output_path":"t.jsonl",
            "network":{"peers":[{"id":"p","chain_sync_fixture":"f.jsonl"}]},
            "steps":[{"kind":"tick_slots","count":1}]
        }"#;
        let s: Scenario = serde_json::from_str(json).unwrap();
        assert!(validate(&s).is_ok());
    }

    #[test]
    fn network_start_slot_parses() {
        let json = r#"{
            "name":"t","network_magic":1,"trace_output_path":"t.jsonl",
            "network":{"start_slot":500,"slot_length_ms":200,"peers":[{"id":"p","chain_sync_fixture":"f.jsonl"}]},
            "steps":[{"kind":"tick_slots","count":1}]
        }"#;
        let s: Scenario = serde_json::from_str(json).unwrap();
        assert!(validate(&s).is_ok());
        let net = s.network.unwrap();
        assert_eq!(net.start_slot, Some(500));
        assert_eq!(net.slot_length_ms, Some(200));
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
