pub mod block_fixture;
pub mod fixture;
pub mod peer_state;
pub mod response_rules;
pub mod runner;
pub mod vars;

use std::path::PathBuf;

use anyhow::Context as _;
use pallas_network::miniprotocols::Point;
use serde::{Deserialize, Serialize};
use serde_json::Value;

// ── Public types ─────────────────────────────────────────────────────────────

/// Rule governing when a peer automatically produces blocks as the scenario's
/// imaginary clock advances. Production fires during `advance_to_slot` and
/// `tick_slots` for all slots in the half-open range (old_slot, new_slot].
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ProductionRule {
    /// No automatic production — explicit opt-out (same as absent).
    None,
    /// Produces at `first_slot`, `first_slot + interval`, `first_slot + 2*interval`, …
    EveryNSlots { first_slot: u64, interval: u64 },
    /// Produces at exactly the listed slots (must be strictly increasing).
    AtSlots { slots: Vec<u64> },
    /// Like `EveryNSlots` until `fork_slot`, then mixes `fork_marker` into the hash
    /// computation so the chain diverges from any peer with a different marker.
    /// Two peers with the same `first_slot`/`interval`/`fork_slot` but different
    /// markers agree on all pre-fork blocks and diverge from `fork_slot` onward.
    ForkedFromSlot {
        first_slot:  u64,
        interval:    u64,
        fork_slot:   u64,
        /// Arbitrary string (non-empty, ≤ 256 bytes) mixed into the hash after
        /// `fork_slot`. A null-byte separator prevents concatenation collisions.
        fork_marker: String,
    },
    /// Like `EveryNSlots` but omits scheduled slots at the given 0-indexed positions.
    /// Block numbers remain sequential; only slot values have gaps.
    SkipsSlots {
        first_slot:   u64,
        interval:     u64,
        /// 0-indexed positions in the full `EveryNSlots` sequence to skip
        /// (must be strictly increasing).
        skip_indices: Vec<usize>,
    },
    /// Like `EveryNSlots` but replaces `prev_hash` with `wrong_hash` from
    /// `break_at_slot` onward, severing hash-chain integrity.
    BrokenPrevHash {
        first_slot:    u64,
        interval:      u64,
        break_at_slot: u64,
        /// Literal hex hash (64 chars = 32 bytes) to substitute as `prev_hash`.
        wrong_hash:    String,
    },
}

impl ProductionRule {
    pub fn kind_str(&self) -> &'static str {
        match self {
            ProductionRule::None               => "none",
            ProductionRule::EveryNSlots { .. } => "every_n_slots",
            ProductionRule::AtSlots { .. }     => "at_slots",
            ProductionRule::ForkedFromSlot { .. } => "forked_from_slot",
            ProductionRule::SkipsSlots { .. }     => "skips_slots",
            ProductionRule::BrokenPrevHash { .. } => "broken_prev_hash",
        }
    }

    /// Returns every slot in the **closed** interval `[from, to]` at which
    /// this rule fires. Caller is responsible for passing (old+1, new) so
    /// the advance range `(old, new]` is respected.
    pub fn slots_in_range(&self, from: u64, to: u64) -> Vec<u64> {
        if from > to { return vec![]; }
        match self {
            ProductionRule::None => vec![],
            ProductionRule::EveryNSlots { first_slot, interval } => {
                every_n_slots_range(*first_slot, *interval, from, to)
            }
            ProductionRule::AtSlots { slots } => {
                slots.iter().copied().filter(|&s| s >= from && s <= to).collect()
            }
            // Fork and broken-prev-hash fire at the same slots as EveryNSlots;
            // only the block content differs.
            ProductionRule::ForkedFromSlot { first_slot, interval, .. }
            | ProductionRule::BrokenPrevHash { first_slot, interval, .. } => {
                every_n_slots_range(*first_slot, *interval, from, to)
            }
            // SkipsSlots fires at EveryNSlots positions minus the skip_indices.
            ProductionRule::SkipsSlots { first_slot, interval, skip_indices } => {
                every_n_slots_range(*first_slot, *interval, from, to)
                    .into_iter()
                    .filter(|&s| {
                        let global_idx = ((s - first_slot) / interval) as usize;
                        !skip_indices.contains(&global_idx)
                    })
                    .collect()
            }
        }
    }
}

/// Compute the ordered list of slots for an `EveryNSlots`-style rule in [from, to].
fn every_n_slots_range(first_slot: u64, interval: u64, from: u64, to: u64) -> Vec<u64> {
    if interval == 0 || from > to { return vec![]; }
    let start = if first_slot >= from {
        first_slot
    } else {
        let steps = (from - first_slot + interval - 1) / interval;
        first_slot + steps * interval
    };
    let mut slots = Vec::new();
    let mut s = start;
    while s <= to {
        slots.push(s);
        s = s.saturating_add(interval);
    }
    slots
}

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
    /// Optional automatic block-production rule. When absent or `{ "kind": "none" }`,
    /// this peer never produces blocks automatically.
    pub production_rule: Option<ProductionRule>,
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
    /// Append a new header (and optionally a block body) to a declared peer's
    /// chain. The new entry's slot must be strictly greater than the current
    /// chain tip slot. Any `serve_chain_sync as_peer:` step that runs after this
    /// will see the extended chain.
    PeerExtendsChain,
    /// Receive `count` Leios notifications (block announcements, EB offers,
    /// vote messages) from the server via LeiosNotify (protocol 18).
    LeiosNotify,
    /// Fetch endorser blocks by point from the server via LeiosFetch (protocol 19).
    /// Params: `points` (array of "slot:hex_hash" strings or a `$varname` reference).
    LeiosFetch,
    /// Serve Leios notifications to the connected client via LeiosNotify (protocol 18).
    /// Params: `notifications` (array of notification actions to send in sequence).
    ServeLeiosNotify,
    /// Serve Leios block fetches to the connected client via LeiosFetch (protocol 19).
    /// Params: `responses` (array of response rules keyed by request type).
    ServeLeiosFetch,
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
            StepKind::AdvanceToSlot    => "advance_to_slot",
            StepKind::TickSlots        => "tick_slots",
            StepKind::PeerExtendsChain => "peer_extends_chain",
            StepKind::LeiosNotify        => "leios_notify",
            StepKind::LeiosFetch         => "leios_fetch",
            StepKind::ServeLeiosNotify   => "serve_leios_notify",
            StepKind::ServeLeiosFetch    => "serve_leios_fetch",
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
                | StepKind::LeiosNotify
                | StepKind::LeiosFetch
        )
    }

    pub fn is_server_side(self) -> bool {
        matches!(
            self,
            StepKind::Listen
                | StepKind::AcceptHandshake
                | StepKind::ServeChainSync
                | StepKind::ServeBlockFetch
                | StepKind::ServeLeiosNotify
                | StepKind::ServeLeiosFetch
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

    // ── peer_extends_chain params ─────────────────────────────────────────────

    /// Block height of the new entry.
    pub block_number: Option<u64>,
    /// 64-char hex-encoded block hash of the new entry.
    pub block_hash: Option<String>,
    /// Hex-encoded header CBOR bytes.
    pub header_cbor: Option<String>,
    /// Era variant byte (default: `DEFAULT_HEADER_VARIANT`).
    pub variant: Option<u8>,
    /// Optional hex-encoded block body. When present, also stored in the peer's
    /// block_store so `serve_block_fetch as_peer` can serve it.
    pub block_body_cbor: Option<String>,
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
            | StepKind::QueryTip
            | StepKind::LeiosNotify
            | StepKind::LeiosFetch => {
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
            StepKind::ServeChainSync | StepKind::ServeBlockFetch
            | StepKind::ServeLeiosNotify | StepKind::ServeLeiosFetch => {
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
            | StepKind::AdvanceToSlot | StepKind::TickSlots
            | StepKind::PeerExtendsChain => {}
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

/// Validates the `network` block: unique peer ids, each peer has at least one fixture,
/// and production_rule fields are well-formed.
fn validate_network_declaration(scenario: &Scenario, errors: &mut Vec<String>) {
    let Some(ref network) = scenario.network else { return };
    let mut seen: std::collections::HashSet<&str> = std::collections::HashSet::new();
    for peer in &network.peers {
        let pos = format!("network.peers[\"{}\"]", peer.id);
        if !seen.insert(peer.id.as_str()) {
            errors.push(format!("network: duplicate peer id \"{}\"", peer.id));
        }
        let has_content = peer.chain_sync_fixture.is_some()
            || peer.block_fetch_fixture.is_some()
            || matches!(&peer.production_rule, Some(r) if !matches!(r, ProductionRule::None));
        if !has_content {
            errors.push(format!(
                "network: peer \"{}\" has no content source — declare \
                 chain_sync_fixture, block_fetch_fixture, or production_rule",
                peer.id
            ));
        }
        // Validate production_rule if present.
        match &peer.production_rule {
            None | Some(ProductionRule::None) => {}
            Some(ProductionRule::EveryNSlots { first_slot, interval }) => {
                if *interval == 0 {
                    errors.push(format!(
                        "{pos}: production_rule interval must be at least 1 (got 0)"
                    ));
                }
                if let Some(start) = network.start_slot {
                    if *first_slot < start {
                        errors.push(format!(
                            "{pos}: production_rule first_slot {first_slot} is before \
                             network start_slot {start} — no blocks would ever be produced \
                             (blocks only fire during time advances past start_slot)"
                        ));
                    }
                }
            }
            Some(ProductionRule::AtSlots { slots }) => {
                if slots.is_empty() {
                    errors.push(format!("{pos}: production_rule at_slots slots list must be non-empty"));
                } else {
                    for i in 1..slots.len() {
                        if slots[i] <= slots[i - 1] {
                            errors.push(format!(
                                "{pos}: production_rule at_slots slots must be strictly increasing \
                                 (slots[{i}]={} is not greater than slots[{}]={})",
                                slots[i], i - 1, slots[i - 1]
                            ));
                            break;
                        }
                    }
                }
            }
            Some(ProductionRule::ForkedFromSlot { first_slot, interval, fork_slot, fork_marker }) => {
                if *interval == 0 {
                    errors.push(format!("{pos}: production_rule interval must be at least 1 (got 0)"));
                }
                if *fork_slot < *first_slot {
                    errors.push(format!(
                        "{pos}: production_rule fork_slot {fork_slot} is before \
                         first_slot {first_slot} — no blocks would be produced before the fork"
                    ));
                }
                if fork_marker.is_empty() {
                    errors.push(format!("{pos}: production_rule fork_marker must be non-empty"));
                } else if fork_marker.len() > 256 {
                    errors.push(format!(
                        "{pos}: production_rule fork_marker must be at most 256 bytes \
                         (got {} bytes)", fork_marker.len()
                    ));
                }
                if let Some(start) = network.start_slot {
                    if *first_slot < start {
                        errors.push(format!(
                            "{pos}: production_rule first_slot {first_slot} is before \
                             network start_slot {start}"
                        ));
                    }
                }
            }
            Some(ProductionRule::SkipsSlots { first_slot, interval, skip_indices }) => {
                if *interval == 0 {
                    errors.push(format!("{pos}: production_rule interval must be at least 1 (got 0)"));
                }
                for i in 1..skip_indices.len() {
                    if skip_indices[i] <= skip_indices[i - 1] {
                        errors.push(format!(
                            "{pos}: production_rule skip_indices must be strictly increasing \
                             (skip_indices[{i}]={} is not greater than skip_indices[{}]={})",
                            skip_indices[i], i - 1, skip_indices[i - 1]
                        ));
                        break;
                    }
                }
                if let Some(start) = network.start_slot {
                    if *first_slot < start {
                        errors.push(format!(
                            "{pos}: production_rule first_slot {first_slot} is before \
                             network start_slot {start}"
                        ));
                    }
                }
            }
            Some(ProductionRule::BrokenPrevHash { first_slot, interval, break_at_slot, wrong_hash }) => {
                if *interval == 0 {
                    errors.push(format!("{pos}: production_rule interval must be at least 1 (got 0)"));
                }
                if *break_at_slot < *first_slot {
                    errors.push(format!(
                        "{pos}: production_rule break_at_slot {break_at_slot} is before \
                         first_slot {first_slot}"
                    ));
                }
                if wrong_hash.len() != 64 || wrong_hash.chars().any(|c| !c.is_ascii_hexdigit()) {
                    errors.push(format!(
                        "{pos}: production_rule wrong_hash must be 64 lowercase hex characters \
                         (32 bytes), got {:?}", wrong_hash
                    ));
                }
                if let Some(start) = network.start_slot {
                    if *first_slot < start {
                        errors.push(format!(
                            "{pos}: production_rule first_slot {first_slot} is before \
                             network start_slot {start}"
                        ));
                    }
                }
            }
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

// When adding a new StepKind, audit these allowlists:
//   1. peer_id allowlist (around line 585)
//   2. target_address allowlist (around line 580)
//   3. validate_connection_names match — needs an explicit arm
//
// Forgetting any of these produces "passes unit tests, fails integration
// tests" failures where the new step kind is rejected at parse time.
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
            StepKind::Connect
                | StepKind::AcceptHandshake
                | StepKind::EmitPeerEvent
                | StepKind::PeerExtendsChain
        )
    {
        errors.push(format!(
            "{pos}: peer_id is only valid on connect, accept_handshake, \
             emit_peer_event, and peer_extends_chain steps"
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

        StepKind::PeerExtendsChain => {
            for req in &["peer_id", "slot", "block_number", "block_hash", "header_cbor"] {
                if step.raw_params.get(req).is_none() {
                    errors.push(format!("{pos}: peer_extends_chain requires {req}"));
                }
            }
            // Validate block_hash is 64 hex chars (32 bytes) when it's a literal.
            if let Some(Value::String(h)) = step.raw_params.get("block_hash") {
                if !h.starts_with('$') && h.len() != 64 {
                    errors.push(format!(
                        "{pos}: peer_extends_chain block_hash must be 64 hex characters (32 bytes), got {}",
                        h.len()
                    ));
                }
            }
            // Monotonic slot check and peer existence are runtime validations.
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

        StepKind::LeiosNotify => {
            // count and await_timeout_secs are optional (defaults applied at runtime).
        }

        StepKind::LeiosFetch => {
            let points_val = step.raw_params.get("points");
            let is_ref = matches!(points_val, Some(Value::String(s)) if s.starts_with('$'));
            if points_val.is_none() {
                errors.push(format!("{pos}: leios_fetch requires points"));
            } else if !is_ref {
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

        StepKind::ServeLeiosNotify | StepKind::ServeLeiosFetch => {
            // notifications/responses are optional — no required params to validate at parse time.
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
    fn peer_with_no_content_sources_is_rejected() {
        // No fixture and no production_rule (or explicit None) — must fail.
        let json = server_with_network(
            r#"{ "peers": [{ "id": "empty_peer" }] }"#,
            r#"{ "kind": "listen" }"#,
        );
        let s: Scenario = serde_json::from_str(&json).unwrap();
        let err = validate(&s).unwrap_err().to_string();
        assert!(err.contains("no content source"), "{err}");
        assert!(err.contains("production_rule"), "{err}");
    }

    #[test]
    fn peer_with_only_production_rule_passes_validation() {
        // production_rule is a valid content source — no fixture required.
        let json = server_with_network(
            r#"{ "start_slot": 99, "peers": [{ "id": "p",
                 "production_rule": { "kind": "every_n_slots",
                                      "first_slot": 100, "interval": 5 } }] }"#,
            r#"{ "kind": "listen" }"#,
        );
        let s: Scenario = serde_json::from_str(&json).unwrap();
        assert!(validate(&s).is_ok(), "production_rule alone must satisfy the content-source requirement");
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

    // ── production_rule validation ────────────────────────────────────────────

    fn network_with_rule(rule_json: &str) -> String {
        format!(r#"{{
            "name":"t","network_magic":1,"trace_output_path":"t.jsonl",
            "network":{{
                "start_slot":99,
                "peers":[{{
                    "id":"p",
                    "chain_sync_fixture":"f.jsonl",
                    "production_rule":{rule_json}
                }}]
            }},
            "steps":[]
        }}"#)
    }

    #[test]
    fn every_n_slots_interval_zero_rejected() {
        let json = network_with_rule(r#"{"kind":"every_n_slots","first_slot":100,"interval":0}"#);
        let s: Scenario = serde_json::from_str(&json).unwrap();
        let err = validate(&s).unwrap_err().to_string();
        assert!(err.contains("interval must be at least 1"), "{err}");
    }

    #[test]
    fn at_slots_empty_list_rejected() {
        let json = network_with_rule(r#"{"kind":"at_slots","slots":[]}"#);
        let s: Scenario = serde_json::from_str(&json).unwrap();
        let err = validate(&s).unwrap_err().to_string();
        assert!(err.contains("non-empty"), "{err}");
    }

    #[test]
    fn at_slots_not_strictly_increasing_rejected() {
        let json = network_with_rule(r#"{"kind":"at_slots","slots":[100,105,100]}"#);
        let s: Scenario = serde_json::from_str(&json).unwrap();
        let err = validate(&s).unwrap_err().to_string();
        assert!(err.contains("strictly increasing"), "{err}");
    }

    #[test]
    fn every_n_slots_first_slot_before_start_slot_rejected() {
        let json = network_with_rule(r#"{"kind":"every_n_slots","first_slot":50,"interval":5}"#);
        let s: Scenario = serde_json::from_str(&json).unwrap();
        let err = validate(&s).unwrap_err().to_string();
        assert!(err.contains("first_slot") && err.contains("start_slot"), "{err}");
    }

    #[test]
    fn forked_from_slot_fork_before_first_rejected() {
        let json = network_with_rule(r#"{"kind":"forked_from_slot","first_slot":100,"interval":5,"fork_slot":90,"fork_marker":"x"}"#);
        let s: Scenario = serde_json::from_str(&json).unwrap();
        let err = validate(&s).unwrap_err().to_string();
        assert!(err.contains("fork_slot") && err.contains("first_slot"), "{err}");
    }

    #[test]
    fn forked_from_slot_empty_marker_rejected() {
        let json = network_with_rule(r#"{"kind":"forked_from_slot","first_slot":100,"interval":5,"fork_slot":110,"fork_marker":""}"#);
        let s: Scenario = serde_json::from_str(&json).unwrap();
        let err = validate(&s).unwrap_err().to_string();
        assert!(err.contains("fork_marker must be non-empty"), "{err}");
    }

    #[test]
    fn skips_slots_non_increasing_indices_rejected() {
        let json = network_with_rule(r#"{"kind":"skips_slots","first_slot":100,"interval":5,"skip_indices":[2,1]}"#);
        let s: Scenario = serde_json::from_str(&json).unwrap();
        let err = validate(&s).unwrap_err().to_string();
        assert!(err.contains("strictly increasing"), "{err}");
    }

    #[test]
    fn broken_prev_hash_bad_hex_rejected() {
        let json = network_with_rule(r#"{"kind":"broken_prev_hash","first_slot":100,"interval":5,"break_at_slot":110,"wrong_hash":"zzzz"}"#);
        let s: Scenario = serde_json::from_str(&json).unwrap();
        let err = validate(&s).unwrap_err().to_string();
        assert!(err.contains("wrong_hash"), "{err}");
    }

    #[test]
    fn broken_prev_hash_break_before_first_rejected() {
        let json = network_with_rule(r#"{"kind":"broken_prev_hash","first_slot":100,"interval":5,"break_at_slot":90,"wrong_hash":"0000000000000000000000000000000000000000000000000000000000000000"}"#);
        let s: Scenario = serde_json::from_str(&json).unwrap();
        let err = validate(&s).unwrap_err().to_string();
        assert!(err.contains("break_at_slot") && err.contains("first_slot"), "{err}");
    }

    #[test]
    fn valid_production_rule_passes_validation() {
        let json = network_with_rule(r#"{"kind":"every_n_slots","first_slot":100,"interval":5}"#);
        let s: Scenario = serde_json::from_str(&json).unwrap();
        assert!(validate(&s).is_ok());
    }

    #[test]
    fn peer_extends_chain_without_peer_id_is_rejected() {
        // peer_id is required on peer_extends_chain; omitting it must be a parse-time error.
        let json = server_with_network(
            r#"{ "peers": [{ "id": "p", "chain_sync_fixture": "f.jsonl" }] }"#,
            r#"{ "kind": "peer_extends_chain",
                 "slot": 100, "block_number": 1,
                 "block_hash": "0000000000000000000000000000000000000000000000000000000000000000",
                 "header_cbor": "8200" }"#,
        );
        let s: Scenario = serde_json::from_str(&json).unwrap();
        let err = validate(&s).unwrap_err().to_string();
        assert!(err.contains("peer_extends_chain requires peer_id"), "{err}");
    }

    #[test]
    fn peer_extends_chain_with_peer_id_passes_validation() {
        // Confirm the allowlist fix: peer_id on peer_extends_chain must not be rejected.
        let json = server_with_network(
            r#"{ "peers": [{ "id": "p", "chain_sync_fixture": "f.jsonl" }] }"#,
            r#"{ "kind": "peer_extends_chain",
                 "peer_id": "p",
                 "slot": 100, "block_number": 1,
                 "block_hash": "0000000000000000000000000000000000000000000000000000000000000000",
                 "header_cbor": "8200" }"#,
        );
        let s: Scenario = serde_json::from_str(&json).unwrap();
        assert!(validate(&s).is_ok(), "peer_extends_chain with peer_id must be valid");
    }

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
