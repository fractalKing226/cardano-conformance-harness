use std::collections::HashMap;
use std::future::Future;
use std::net::ToSocketAddrs;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Context as _;
use futures::future::try_join_all;
use net_core::bearer::tcp::TcpBearer;
use net_core::mux::scheduler::{AnyScheduler, TrafficClass};
use net_core::mux::{CodecRecv, CodecSend, Mux, MuxConfig, ProtocolConfig, RunningMux, MODE_INITIATOR, MODE_RESPONDER};
use net_core::protocols::chainsync as nc_chainsync;
use net_core::protocols::handshake::{self as nc_handshake, n2n};
use net_core::protocols::{Role, Runner};
use net_core::types::Point as NcPoint;
use pallas_network::miniprotocols::chainsync::Tip;
use pallas_network::miniprotocols::Point;
use serde_json::{json, Value};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tracing::info;

use crate::miniprotocols::blockfetch::{run_block_fetch, BLOCK_FETCH_PROTOCOL};
use crate::miniprotocols::chainsync::{run_chain_sync, to_nc_point, CHAIN_SYNC_PROTOCOL};
use crate::miniprotocols::blockfetch_server::execute_block_fetch_script;
use crate::miniprotocols::chainsync_server::execute_response_script;
use crate::miniprotocols::handshake::handshake_on_channels;
use crate::miniprotocols::leios_notify::{run_leios_notify, LEIOS_NOTIFY_PROTOCOL};
use crate::miniprotocols::leios_fetch::{run_leios_fetch, LEIOS_FETCH_PROTOCOL};
use crate::miniprotocols::leios_notify_server::{execute_leios_notify_script, LeiosNotifyAction};
use crate::miniprotocols::leios_fetch_server::{execute_leios_fetch_script, LeiosFetchRule};
use crate::scenario::block_fixture;
use crate::scenario::response_rules::{generate_for_block_fetch, generate_from_fixture, rule_def_to_script};
use crate::miniprotocols::keepalive::{run_keepalive, run_keepalive_server, KEEP_ALIVE_INTERVAL, KEEP_ALIVE_PROTOCOL};
use crate::miniprotocols::txsubmission::{run_tx_submission, TX_SUBMISSION_PROTOCOL};
use crate::scenario::fixture;
use crate::scenario::vars::{point_to_str, substitute_in_value, VarStore};
use crate::scenario::peer_state::{
    apply_production_rules, ChainEntry, PeerState, ProductionEvent, decode_hex as decode_hex_bytes,
};
use crate::scenario::fixture::DEFAULT_HEADER_VARIANT;
use crate::scenario::{BlockFetchPoints, Network, Peer, Scenario, StepDef, StepKind, StepParams};
use crate::trace::{Direction, EventKind, TraceEvent, Tracer};

use super::parse_point;

/// Sentinel error type used so the main run loop can distinguish a step
/// assertion failure (step returned Ok but assertions did not pass) from a
/// genuine step execution error, and emit the correct `scenario_completed`
/// outcome accordingly.
#[derive(Debug)]
struct StepAssertionFailure;
impl std::fmt::Display for StepAssertionFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "step assertion failure")
    }
}
impl std::error::Error for StepAssertionFailure {}

// ── Network peer lookup ───────────────────────────────────────────────────────

/// Emits `ProductionRuleFired` (and `PeerChainExtended` for non-skipped blocks)
/// trace events for each entry in a production batch.
async fn emit_production_events(
    events: &[ProductionEvent],
    tracer: &Tracer,
) -> anyhow::Result<()> {
    for ev in events {
        tracer.emit(TraceEvent::new(
            EventKind::ProductionRuleFired,
            Direction::Internal,
            json!({
                "peer_id":   ev.peer_id,
                "slot":      ev.slot,
                "rule_kind": ev.rule_kind,
                "skipped":   ev.skipped,
            }),
        )).await?;
        if !ev.skipped {
            let mut payload = json!({
                "peer_id":      ev.peer_id,
                "slot":         ev.slot,
                "block_hash":   ev.block_hash_hex,
                "block_number": ev.block_number,
                "source":       "production_rule",
            });
            if let Some(dk) = ev.defect_kind {
                payload["defect_kind"] = dk.into();
            }
            tracer.emit(TraceEvent::new(EventKind::PeerChainExtended, Direction::Internal, payload))
                .await?;
        }
    }
    Ok(())
}

/// Emits a `SlotAdvanced` trace event recording a slot transition.
/// Called by both `advance_to_slot` and `tick_slots` handlers.
async fn emit_slot_advanced(
    from_slot: u64,
    to_slot:   u64,
    reason:    &str,
    tracer:    &Tracer,
) -> anyhow::Result<()> {
    tracer
        .emit(
            TraceEvent::new(
                EventKind::SlotAdvanced,
                Direction::Internal,
                json!({ "from_slot": from_slot, "to_slot": to_slot, "reason": reason }),
            )
            // Pin the event's slot to to_slot. The tracer's auto-stamp would also
            // give to_slot (since the store already happened), but being explicit
            // removes any ambiguity about which slot a SlotAdvanced event describes.
            .with_slot(to_slot),
        )
        .await
}

/// Looks up a peer by id in the scenario's network declaration.
/// Returns a clear error if the network block is absent or the peer is unknown.
fn lookup_peer<'a>(network: &'a Option<Arc<Network>>, peer_id: &str) -> anyhow::Result<&'a Peer> {
    network
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("as_peer: no network declaration in scenario"))?
        .peers
        .iter()
        .find(|p| p.id == peer_id)
        .ok_or_else(|| anyhow::anyhow!("as_peer: peer \"{peer_id}\" not found in network block"))
}

// ── Protocol subscription ─────────────────────────────────────────────────────

/// Returns the set of N2N protocol IDs registered on every client connection.
pub fn subscribed_protocols(_negotiated_version: u64) -> Vec<u16> {
    vec![
        CHAIN_SYNC_PROTOCOL,
        BLOCK_FETCH_PROTOCOL,
        TX_SUBMISSION_PROTOCOL,
        KEEP_ALIVE_PROTOCOL,
        LEIOS_NOTIFY_PROTOCOL,
        LEIOS_FETCH_PROTOCOL,
    ]
}

// ── Runner ────────────────────────────────────────────────────────────────────

pub struct ScenarioRunner {
    scenario: Scenario,
    capture_fixture: Option<std::path::PathBuf>,
    capture_block_fixture: Option<std::path::PathBuf>,
}

impl ScenarioRunner {
    pub fn new(scenario: Scenario) -> Self {
        Self { scenario, capture_fixture: None, capture_block_fixture: None }
    }

    pub fn with_capture_fixture(mut self, path: Option<std::path::PathBuf>) -> Self {
        self.capture_fixture = path;
        self
    }

    pub fn with_capture_block_fixture(mut self, path: Option<std::path::PathBuf>) -> Self {
        self.capture_block_fixture = path;
        self
    }

    pub async fn run(self) -> anyhow::Result<()> {
        let tracer = Tracer::open(&self.scenario.trace_output_path).await?;
        let started_at = Instant::now();

        tracer
            .emit(TraceEvent::new(
                EventKind::ScenarioStarted,
                Direction::Internal,
                json!({
                    "name":             self.scenario.name,
                    "description":      self.scenario.description,
                    "target_address":   self.scenario.target_address.as_deref().unwrap_or(""),
                    "network_magic":    self.scenario.network_magic,
                    "steps":            self.scenario.steps.len(),
                    "expected_outcome": self.scenario.expected_outcome,
                }),
            ))
            .await?;

        // Initialize the slot counter before creating RunnerState so the tracer
        // can stamp ScenarioStarted and NetworkDeclared with the starting slot.
        // current_slot is only initialized when start_slot is explicitly declared.
        // A network block without start_slot means "peers exist but time is not
        // tracked" — no slot filtering, no SlotAdvanced events, advance_to_slot and
        // tick_slots steps will error (they require an explicit starting point).
        // Defaulting to 0 here would silently hide all fixture entries whose slots
        // are non-zero (e.g. a devnet fixture captured at slots 138+).
        let current_slot: Option<Arc<AtomicU64>> = self.scenario.network.as_ref()
            .and_then(|n| n.start_slot)
            .map(|s| Arc::new(AtomicU64::new(s)));
        let tracer = tracer.with_slot_tracker(current_slot.clone());

        let mut state = RunnerState {
            capture_fixture_path:       self.capture_fixture.clone(),
            capture_block_fixture_path: self.capture_block_fixture.clone(),
            network:                    self.scenario.network.as_ref().map(|n| Arc::new(n.clone())),
            current_slot,
            ..Default::default()
        };

        // Emit one NetworkDeclared event so the trace records the imaginary topology.
        if let Some(ref network) = self.scenario.network {
            let peers_json: Vec<serde_json::Value> = network.peers.iter().map(|p| {
                let mut obj = json!({ "id": p.id });
                if let Some(ref f) = p.chain_sync_fixture  { obj["chain_sync_fixture"]  = f.clone().into(); }
                if let Some(ref f) = p.block_fetch_fixture { obj["block_fetch_fixture"] = f.clone().into(); }
                if let Some(ref d) = p.description         { obj["description"]         = d.clone().into(); }
                obj
            }).collect();
            tracer.emit(TraceEvent::new(
                EventKind::NetworkDeclared,
                Direction::Internal,
                json!({ "peers": peers_json }),
            )).await?;

            // Initialize each peer's runtime state from its declared fixtures.
            // Emits PeerStateInitialized for every peer (including empty ones).
            let mut peers_map: HashMap<String, PeerState> = HashMap::new();
            for peer in &network.peers {
                let mut peer_state = PeerState::new();

                if let Some(ref path) = peer.chain_sync_fixture {
                    let chain = fixture::load(std::path::Path::new(path.as_str()))
                        .with_context(|| format!(
                            "peer \"{}\": loading chain_sync_fixture \"{path}\"", peer.id
                        ))?;
                    peer_state = PeerState::from_fixture_chain(&chain);
                }
                peer_state.production_rule = peer.production_rule.clone();

                if let Some(ref path) = peer.block_fetch_fixture {
                    let chain = block_fixture::load(std::path::Path::new(path.as_str()))
                        .with_context(|| format!(
                            "peer \"{}\": loading block_fetch_fixture \"{path}\"", peer.id
                        ))?;
                    peer_state.extend_from_block_fixture_chain(&chain);
                }

                let chain_entries_loaded = peer_state.chain_entries.len();
                let blocks_loaded = peer_state.block_store.len();
                tracer.emit(TraceEvent::new(
                    EventKind::PeerStateInitialized,
                    Direction::Internal,
                    json!({
                        "peer_id":              peer.id,
                        "chain_entries_loaded": chain_entries_loaded,
                        "blocks_loaded":        blocks_loaded,
                    }),
                )).await?;

                peers_map.insert(peer.id.clone(), peer_state);
            }
            *state.peers.lock().unwrap() = peers_map;
        }

        let mut steps_passed: u64 = 0;
        let mut steps_failed: u64 = 0;

        for (idx, step_def) in self.scenario.steps.iter().enumerate() {
            match run_step(
                step_def,
                idx,
                self.scenario.target_address.as_deref().unwrap_or(""),
                self.scenario.network_magic,
                &mut state,
                &tracer,
            )
            .await
            {
                Ok(()) => {
                    steps_passed += 1;
                }
                Err(e) => {
                    steps_failed += 1;
                    cleanup(&mut state, &tracer).await;
                    let outcome = if e.downcast_ref::<StepAssertionFailure>().is_some() {
                        "assertion_failed"
                    } else {
                        "step_error"
                    };
                    emit_completed(
                        &tracer,
                        &self.scenario.name,
                        steps_passed,
                        steps_failed,
                        started_at,
                        outcome,
                    )
                    .await;
                    return Err(e);
                }
            }
        }

        emit_completed(
            &tracer,
            &self.scenario.name,
            steps_passed,
            steps_failed,
            started_at,
            "completed",
        )
        .await;
        Ok(())
    }
}

// ── Connection structs ────────────────────────────────────────────────────────

// ── RAII guard for checked-out connections ────────────────────────────────────

/// Removes a value from a shared HashMap for exclusive use by one step,
/// then reinserts it automatically on drop — whether from normal return,
/// `?` error propagation, or tokio task abort.
///
/// Use `consume()` for steps that intentionally remove the entry (disconnect,
/// close_listener) so that `Drop` does NOT reinsert.
///
/// **Undefined behaviour note.** If another branch calls `consume()` to remove
/// the same name from the HashMap while this guard is still alive, `Drop` will
/// reinsert a connection the scenario considers closed. This mirrors the
/// undefined behaviour of two branches concurrently using the same connection —
/// scenarios must use disjoint connection names across parallel branches.
struct CheckedOut<V> {
    name:  String,
    value: Option<V>,
    map:   Arc<Mutex<HashMap<String, V>>>,
}

impl<V> CheckedOut<V> {
    fn take(map: &Arc<Mutex<HashMap<String, V>>>, name: &str) -> anyhow::Result<Self> {
        let value = map.lock().unwrap()   // lock for <1 μs (HashMap::remove)
            .remove(name)
            .ok_or_else(|| anyhow::anyhow!(
                "connection \"{name}\" not found (missing connect/accept_handshake, or already in use by another branch)"
            ))?;
        Ok(Self { name: name.to_string(), value: Some(value), map: Arc::clone(map) })
    }

    fn get_mut(&mut self) -> &mut V {
        self.value.as_mut().unwrap()
    }

    /// Take ownership without reinsertion — use for disconnect/close_listener.
    #[allow(dead_code)]
    fn consume(mut self) -> V {
        self.value.take().unwrap()
        // Drop sees None → no reinsertion.
    }
}

impl<V> Drop for CheckedOut<V> {
    fn drop(&mut self) {
        if let Some(value) = self.value.take() {
            if let Ok(mut guard) = self.map.lock() {
                guard.insert(self.name.clone(), value);
            }
            // Poisoned mutex means the runtime is already panicking;
            // losing the connection entry here is acceptable.
        }
    }
}

// ── Connection structs ────────────────────────────────────────────────────────

/// State for one outgoing (client-mode) connection, indexed by name.
struct ClientConnection {
    mux: RunningMux,
    hs_channels: Option<(CodecSend, CodecRecv)>,
    cs_channels: Option<(CodecSend, CodecRecv)>,
    bf_channels: Option<(CodecSend, CodecRecv)>,
    /// Stored at connect; consumed at handshake to spawn background loops.
    ka_channels: Option<(CodecSend, CodecRecv)>,
    ts_channels: Option<(CodecSend, CodecRecv)>,
    ln_channels: Option<(CodecSend, CodecRecv)>,
    lf_channels: Option<(CodecSend, CodecRecv)>,
    ka_handle: Option<JoinHandle<()>>,
    ts_handle: Option<JoinHandle<()>>,
    #[allow(dead_code)]
    negotiated_version: Option<u64>,
    /// Points from the most-recent chain_sync on this connection.
    /// Legacy backing for `points: "from_chain_sync"`. Prefer explicit
    /// `output` + `"$varname"` in multi-connection scenarios.
    last_chain_sync_points: Vec<Point>,
    /// Peer identity label for trace attribution (set by the connect step's
    /// optional `peer_id` parameter). Propagated to every wire event emitted
    /// on this connection via `tracer.for_peer_opt(conn.peer_id.clone())`.
    peer_id: Option<String>,
}

/// State for one bound TCP listener, indexed by name.
/// The `TcpListener` is behind an `Arc` so multiple `accept_handshake` steps
/// (from parallel branches) can all call `accept_tcp` on the same listener.
struct ServerListener {
    listener: Arc<tokio::net::TcpListener>,
}

/// State for one accepted (server-mode) connection, indexed by name.
struct ServerConnection {
    mux: RunningMux,
    cs_channels: Option<(CodecSend, CodecRecv)>,
    bf_channels: Option<(CodecSend, CodecRecv)>,
    /// Idle but must stay alive — dropping closes the demuxer channel.
    #[allow(dead_code)]
    ts_channels: Option<(CodecSend, CodecRecv)>,
    ln_channels: Option<(CodecSend, CodecRecv)>,
    lf_channels: Option<(CodecSend, CodecRecv)>,
    ka_handle: Option<JoinHandle<()>>,
    peer_id: Option<String>,
}

// ── Runner state ──────────────────────────────────────────────────────────────

/// All heavy fields are behind `Arc` so that cloning `RunnerState` is O(1)
/// (six reference-count bumps). Parallel branches receive a clone and share
/// the underlying data through the `Arc`s.
#[derive(Clone)]
struct RunnerState {
    /// Outgoing connections. Lock held only for HashMap lookup (<1 μs);
    /// connection is taken out for the duration of a step via `CheckedOut`.
    connections: Arc<Mutex<HashMap<String, ClientConnection>>>,
    /// Bound listeners. `TcpListener` is `Arc`-wrapped inside so multiple
    /// parallel branches can call `accept_tcp` on the same listener.
    listeners: Arc<Mutex<HashMap<String, ServerListener>>>,
    /// Accepted server connections. Same take/reinsert pattern as `connections`.
    server_connections: Arc<Mutex<HashMap<String, ServerConnection>>>,
    /// Flat variable namespace. Lock held only for read/write of individual keys.
    vars: Arc<Mutex<VarStore>>,
    /// Written exactly once per run via compare-exchange.
    fixture_anchor_written:       Arc<AtomicBool>,
    block_fixture_anchor_written: Arc<AtomicBool>,
    // These are set-once at construction and cloned cheaply into spawned tasks.
    capture_fixture_path:         Option<std::path::PathBuf>,
    capture_block_fixture_path:   Option<std::path::PathBuf>,
    /// Imaginary-network declaration from the scenario. Stored as Arc so cloning
    /// RunnerState for parallel branches is O(1).
    network:                      Option<Arc<Network>>,
    /// Current imaginary-network slot. Shared across all parallel branches via Arc.
    /// None when the scenario has no network declaration.
    current_slot:                 Option<Arc<AtomicU64>>,
    /// Runtime state for each declared peer. Populated at scenario start from
    /// peer fixtures; extended at runtime by `peer_extends_chain` steps.
    peers:                        Arc<Mutex<HashMap<String, PeerState>>>,
}

impl Default for RunnerState {
    fn default() -> Self {
        Self {
            connections:             Arc::new(Mutex::new(HashMap::new())),
            listeners:               Arc::new(Mutex::new(HashMap::new())),
            server_connections:      Arc::new(Mutex::new(HashMap::new())),
            vars:                    Arc::new(Mutex::new(VarStore::new())),
            fixture_anchor_written:       Arc::new(AtomicBool::new(false)),
            block_fixture_anchor_written: Arc::new(AtomicBool::new(false)),
            capture_fixture_path:         None,
            capture_block_fixture_path:   None,
            network:                      None,
            current_slot:                 None,
            peers:                        Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

// ── Step execution with bookkeeping ───────────────────────────────────────────

/// Runs one step with full trace bookkeeping: StepStarted/Completed events,
/// variable substitution, assertion evaluation, and VariableReferenced events.
///
/// Returns `Ok(())` on success (step + assertions passed) or `Err` on failure.
/// Does NOT handle scenario-level cleanup on failure — that's the caller's job.
///
/// Returns `Pin<Box<...>>` to allow recursive invocation from `repeat` steps.
fn run_step<'a>(
    step: &'a StepDef,
    step_idx: usize,
    target_address: &'a str,
    network_magic: u64,
    state: &'a mut RunnerState,
    tracer: &'a Tracer,
) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + 'a>> {
    Box::pin(async move {
        // Clear buffer so assertions only see this step's events.
        tracer.drain_buffer().await;

        tracer
            .emit(TraceEvent::new(
                EventKind::StepStarted,
                Direction::Internal,
                json!({ "index": step_idx, "kind": step.kind.as_str() }),
            ))
            .await?;
        tracer.drain_buffer().await; // StepStarted is not part of this step's assertions

        // Variable substitution: resolve all $refs in params before execution.
        let mut resolved = step.raw_params.clone();
        // For repeat steps, exclude the `body` array from substitution.
        // Body steps are substituted per-iteration when they actually execute
        // (inside the repeat handler, which calls run_step for each body step).
        // Substituting the body here would try to resolve variables that haven't
        // been set yet — e.g. a query_tip inside the body sets a variable that a
        // later body step in the same iteration needs.
        if matches!(step.kind, StepKind::Repeat | StepKind::Parallel) {
            if let serde_json::Value::Object(ref mut map) = resolved {
                map.remove("body");
                map.remove("branches");
            }
        }
        // Snapshot vars before substitution — brief lock, released before any I/O.
        let vars_snapshot = state.vars.lock().unwrap().clone();
        let refs_made = substitute_in_value(&mut resolved, &vars_snapshot)
            .with_context(|| format!("step[{step_idx}] ({}): variable substitution failed", step.kind.as_str()))?;

        for (ref_expr, type_name) in &refs_made {
            let _ = tracer
                .emit(TraceEvent::new(
                    EventKind::VariableReferenced,
                    Direction::Internal,
                    json!({ "step_index": step_idx, "reference": ref_expr, "resolved_type": type_name }),
                ))
                .await;
        }

        let step_result = execute_step(step, &resolved, target_address, network_magic, state, tracer).await;

        let step_events = tracer.drain_buffer().await;

        // Evaluate expect clauses.
        let mut assertions_ok = true;
        if let Some(expect) = &step.expect {
            for result in evaluate_assertions(expect, &step_events) {
                if !result.passed {
                    assertions_ok = false;
                }
                let kind = if result.passed {
                    EventKind::AssertionPassed
                } else {
                    EventKind::AssertionFailed
                };
                let _ = tracer
                    .emit(TraceEvent::new(
                        kind,
                        Direction::Internal,
                        json!({
                            "step_index": step_idx,
                            "assertion":  result.name,
                            "message":    result.message,
                        }),
                    ))
                    .await;
            }
        }

        match (step_result, assertions_ok) {
            (Ok(()), true) => {
                tracer
                    .emit(TraceEvent::new(
                        EventKind::StepCompleted,
                        Direction::Internal,
                        json!({ "index": step_idx, "outcome": "ok" }),
                    ))
                    .await?;
                Ok(())
            }
            (Ok(()), false) => {
                tracer
                    .emit(TraceEvent::new(
                        EventKind::StepCompleted,
                        Direction::Internal,
                        json!({ "index": step_idx, "outcome": "assertion_failed" }),
                    ))
                    .await?;
                return Err(anyhow::Error::new(StepAssertionFailure));
            }
            (Err(e), _) => {
                let _ = tracer
                    .emit(TraceEvent::new(
                        EventKind::StepCompleted,
                        Direction::Internal,
                        json!({ "index": step_idx, "outcome": "error", "error": e.to_string() }),
                    ))
                    .await;
                Err(e)
            }
        }
    })
}

// ── Step dispatch ─────────────────────────────────────────────────────────────

/// Executes a single step against resolved params. Does not handle
/// StepStarted/Completed or assertion evaluation — that's run_step's job.
///
/// Returns `Pin<Box<...>>` to allow recursive invocation from `repeat` steps.
fn execute_step<'a>(
    step: &'a StepDef,
    resolved: &'a Value,
    target_address: &'a str,
    network_magic: u64,
    state: &'a mut RunnerState,
    tracer: &'a Tracer,
) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + 'a>> {
    Box::pin(async move {
        let params: StepParams = serde_json::from_value(resolved.clone())
            .with_context(|| format!("step ({}): invalid params after substitution", step.kind.as_str()))?;

        match step.kind {
            StepKind::Connect => {
                let conn_name = step.as_name.as_deref().unwrap_or("default").to_string();
                // Phase 1 of the two-phase connection lifecycle: structural setup.
                // Register every protocol channel with the Mux and spawn it.
                // No mini-protocol traffic is sent here — the Cardano N2N spec
                // requires Handshake to complete before any other protocol can be
                // used. Background workers are started in the Handshake arm below.
                let addr = params.target_address.as_deref().unwrap_or(target_address);
                let socket_addr = addr
                    .to_socket_addrs()
                    .with_context(|| format!("failed to resolve {addr}"))?
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("no address resolved for {addr}"))?;
                let bearer = TcpBearer::connect(socket_addr)
                    .await
                    .with_context(|| format!("failed to connect to {addr}"))?;
                tracer
                    .emit(TraceEvent::new(
                        EventKind::ConnectionOpened,
                        Direction::Internal,
                        json!({ "addr": addr }),
                    ).with_connection(&conn_name))
                    .await?;
                let mut mux = Mux::new(MuxConfig::default(), AnyScheduler::default(), MODE_INITIATOR);
                let (hs_send, hs_recv) = mux.register(&ProtocolConfig {
                    id: net_core::protocols::handshake::PROTOCOL_ID,
                    traffic_class: TrafficClass::Priority,
                    ingress_limit: net_core::protocols::handshake::SIZE_LIMIT,
                    egress_queue_size: 4,
                });
                let (cs_send, cs_recv) = mux.register(&ProtocolConfig {
                    id: CHAIN_SYNC_PROTOCOL,
                    traffic_class: TrafficClass::Default(1),
                    ingress_limit: net_core::protocols::chainsync::INGRESS_LIMIT,
                    egress_queue_size: 16,
                });
                let (bf_send, bf_recv) = mux.register(&ProtocolConfig {
                    id: BLOCK_FETCH_PROTOCOL,
                    traffic_class: TrafficClass::Default(1),
                    ingress_limit: net_core::protocols::blockfetch::INGRESS_LIMIT,
                    egress_queue_size: 16,
                });
                let (ts_send, ts_recv) = mux.register(&ProtocolConfig {
                    id: TX_SUBMISSION_PROTOCOL,
                    traffic_class: TrafficClass::Default(1),
                    ingress_limit: net_core::protocols::txsubmission::INGRESS_LIMIT,
                    egress_queue_size: 16,
                });
                let (ka_send, ka_recv) = mux.register(&ProtocolConfig {
                    id: KEEP_ALIVE_PROTOCOL,
                    traffic_class: TrafficClass::Default(1),
                    ingress_limit: net_core::protocols::keepalive::INGRESS_LIMIT,
                    egress_queue_size: 4,
                });
                let (ln_send, ln_recv) = mux.register(&ProtocolConfig {
                    id: LEIOS_NOTIFY_PROTOCOL,
                    traffic_class: TrafficClass::Default(1),
                    ingress_limit: net_core::protocols::leios_notify::INGRESS_LIMIT,
                    egress_queue_size: 16,
                });
                let (lf_send, lf_recv) = mux.register(&ProtocolConfig {
                    id: LEIOS_FETCH_PROTOCOL,
                    traffic_class: TrafficClass::Default(1),
                    ingress_limit: net_core::protocols::leios_fetch::INGRESS_LIMIT,
                    egress_queue_size: 16,
                });
                let running = mux.run(bearer);
                state.connections.lock().unwrap().insert(conn_name, ClientConnection {
                    mux: running,
                    hs_channels: Some((CodecSend::new(hs_send), CodecRecv::new(hs_recv))),
                    cs_channels: Some((CodecSend::new(cs_send), CodecRecv::new(cs_recv))),
                    bf_channels: Some((CodecSend::new(bf_send), CodecRecv::new(bf_recv))),
                    ts_channels: Some((CodecSend::new(ts_send), CodecRecv::new(ts_recv))),
                    ka_channels: Some((CodecSend::new(ka_send), CodecRecv::new(ka_recv))),
                    ln_channels: Some((CodecSend::new(ln_send), CodecRecv::new(ln_recv))),
                    lf_channels: Some((CodecSend::new(lf_send), CodecRecv::new(lf_recv))),
                    ka_handle: None,
                    ts_handle: None,
                    negotiated_version: None,
                    last_chain_sync_points: Vec::new(),
                    peer_id: params.peer_id.clone(),
                });
                Ok(())
            }

            StepKind::Handshake => {
                let conn_name = step.on_name.as_deref().unwrap_or("default");
                // Phase 2 of the two-phase connection lifecycle: version negotiation
                // and worker activation. Only after handshake_on_channels returns
                // successfully do we know the negotiated version and that the node
                // will accept traffic on other channels. Background tasks are spawned
                // here, never in Connect, so they cannot send messages prematurely.
                let mut co = CheckedOut::take(&state.connections, conn_name)?;
                let conn = co.get_mut();
                let (hs_send, hs_recv) = conn.hs_channels.take()
                    .ok_or_else(|| anyhow::anyhow!("handshake: no channels (already used?)"))?;
                let eff_trc = tracer.for_peer_opt(conn.peer_id.clone());
                let version = handshake_on_channels(hs_send, hs_recv, network_magic, &eff_trc).await?;
                conn.negotiated_version = Some(version);

                let mut spawned_protocols: Vec<&str> = Vec::new();
                if let Some((ka_send, ka_recv)) = conn.ka_channels.take() {
                    conn.ka_handle = Some(tokio::spawn(run_keepalive(ka_send, ka_recv, tracer.clone(), KEEP_ALIVE_INTERVAL)));
                    spawned_protocols.push("keep-alive");
                }
                if let Some((ts_send, ts_recv)) = conn.ts_channels.take() {
                    conn.ts_handle = Some(tokio::spawn(run_tx_submission(ts_send, ts_recv, tracer.clone())));
                    spawned_protocols.push("tx-submission");
                }
                tracer
                    .emit(TraceEvent::new(
                        EventKind::ProtocolWorkersStarted,
                        Direction::Internal,
                        json!({ "protocols": spawned_protocols }),
                    ).with_connection(conn_name))
                    .await?;
                info!(version, conn = conn_name, protocols = ?spawned_protocols, "Handshake complete");
                Ok(())
            }

            StepKind::QueryTip => {
                let conn_name = step.on_name.as_deref().unwrap_or("default");
                // query_tip opens its own temporary TCP connection regardless of the
                // named connection — see fetch_tip's comment for the rationale.
                tracer
                    .emit(TraceEvent::new(EventKind::QueryTipStarted, Direction::Internal, json!({}))
                        .with_connection(conn_name))
                    .await?;
                let tip = fetch_tip(target_address, network_magic, tracer).await?;
                let Tip(tip_point, block_number) = &tip;
                let tip_val = json!({ "point": point_to_str(tip_point), "block_number": block_number });
                tracer
                    .emit(TraceEvent::new(EventKind::QueryTipCompleted, Direction::Internal, json!({ "tip": tip_val }))
                        .with_connection(conn_name))
                    .await?;
                if let Some(output_var) = &step.output {
                    state.vars.lock().unwrap().insert(output_var.clone(), tip_val.clone());
                    tracer.emit(TraceEvent::new(EventKind::VariableSet, Direction::Internal,
                        json!({ "name": output_var, "shape": "tip{point,block_number}" }))).await?;
                    info!(var = output_var.as_str(), "Stored tip");
                }
                Ok(())
            }

            StepKind::ChainSync => {
                let conn_name = step.on_name.as_deref().unwrap_or("default").to_string();
                let mut co = CheckedOut::take(&state.connections, &conn_name)?;
                let conn = co.get_mut();
                let (cs_send, cs_recv) = conn.cs_channels.take()
                    .ok_or_else(|| anyhow::anyhow!("chain_sync: no channels (already used?)"))?;

                let origin = vec!["origin".to_string()];
                let raw_pts = params.intersection_points.as_deref().unwrap_or(origin.as_slice());
                let intersection_points = raw_pts.iter().map(|s| parse_point(s))
                    .collect::<anyhow::Result<Vec<Point>>>()?;
                let count = params.count.unwrap_or(10);
                let await_secs = params.await_timeout_secs.unwrap_or(30);

                let eff_trc = tracer.for_peer_opt(conn.peer_id.clone());
                let summary = run_chain_sync(cs_send, cs_recv, intersection_points, count,
                    Duration::from_secs(await_secs), &eff_trc).await?;

                let n = summary.collected_points.len();
                conn.last_chain_sync_points = summary.collected_points.clone();

                if let Some(output_var) = &step.output {
                    let pts_json: Vec<Value> = summary.collected_points.iter()
                        .map(|p| Value::String(point_to_str(p))).collect();
                    state.vars.lock().unwrap().insert(output_var.clone(), Value::Array(pts_json));
                    tracer.emit(TraceEvent::new(EventKind::VariableSet, Direction::Internal,
                        json!({ "name": output_var, "shape": format!("array[{n}]") }))).await?;
                    info!(var = output_var.as_str(), points = n, "Stored chain-sync points");
                }

                if let Some(ref fixture_path) = state.capture_fixture_path {
                    if !summary.captured_headers.is_empty() {
                        if !state.fixture_anchor_written.load(Ordering::SeqCst) {
                            fixture::write_anchor(fixture_path, &Point::Origin)?;
                            state.fixture_anchor_written.store(true, Ordering::SeqCst);
                        }
                        for h in &summary.captured_headers {
                            let entry = fixture::FixtureEntry {
                                slot: h.slot, block_hash: fixture::encode_hex(&h.block_hash),
                                block_number: h.block_number, cbor_hex: fixture::encode_hex(&h.cbor),
                                variant: h.variant,
                            };
                            fixture::append_entry(fixture_path, &entry)?;
                        }
                        info!(headers = summary.captured_headers.len(), "Wrote fixture entries");
                    }
                }
                info!(headers = summary.headers_received, conn = conn_name, "Chain-sync complete");
                Ok(())
            }

            StepKind::BlockFetch => {
                let conn_name = step.on_name.as_deref().unwrap_or("default").to_string();
                let mut co = CheckedOut::take(&state.connections, &conn_name)?;
                let conn = co.get_mut();
                let (bf_send, bf_recv) = conn.bf_channels.take()
                    .ok_or_else(|| anyhow::anyhow!("block_fetch: no channels (already used?)"))?;

                let points: Vec<Point> =
                    match params.points.as_ref().unwrap_or(&BlockFetchPoints::FromChainSync) {
                        BlockFetchPoints::FromChainSync => {
                            anyhow::ensure!(!conn.last_chain_sync_points.is_empty(),
                                "block_fetch: from_chain_sync but no chain_sync points on connection \"{conn_name}\"");
                            std::mem::take(&mut conn.last_chain_sync_points)
                        }
                        BlockFetchPoints::Explicit(strings) => strings.iter()
                            .map(|s| parse_point(s)).collect::<anyhow::Result<Vec<Point>>>()?,
                    };

                let batch_size = params.batch_size.unwrap_or(1);
                let eff_trc = tracer.for_peer_opt(conn.peer_id.clone());
                let summary = run_block_fetch(bf_send, bf_recv, points.clone(), batch_size, &eff_trc).await?;

                if let Some(ref bfp) = state.capture_block_fixture_path {
                    if !summary.captured_blocks.is_empty() {
                        if !state.block_fixture_anchor_written.load(Ordering::SeqCst) {
                            block_fixture::write_anchor(bfp)?;
                            state.block_fixture_anchor_written.store(true, Ordering::SeqCst);
                        }
                        for cb in &summary.captured_blocks {
                            let entry = block_fixture::BlockFixtureEntry {
                                slot: cb.slot, block_hash: block_fixture::encode_hex(&cb.block_hash),
                                block_cbor_hex: block_fixture::encode_hex(&cb.cbor),
                            };
                            block_fixture::append_entry(bfp, &entry)?;
                        }
                    }
                }
                info!(blocks = summary.blocks_received, conn = conn_name, "Block-fetch complete");
                Ok(())
            }

            StepKind::LeiosNotify => {
                let conn_name = step.on_name.as_deref().unwrap_or("default").to_string();
                let mut co = CheckedOut::take(&state.connections, &conn_name)?;
                let conn = co.get_mut();
                let (ln_send, ln_recv) = conn.ln_channels.take()
                    .ok_or_else(|| anyhow::anyhow!("leios_notify: no channels (already used?)"))?;

                let count = params.count.unwrap_or(5);
                let await_secs = params.await_timeout_secs.unwrap_or(30);
                let eff_trc = tracer.for_peer_opt(conn.peer_id.clone());
                run_leios_notify(ln_send, ln_recv, count, Duration::from_secs(await_secs), &eff_trc).await?;
                info!(count, conn = conn_name, "LeiosNotify complete");
                Ok(())
            }

            StepKind::LeiosFetch => {
                let conn_name = step.on_name.as_deref().unwrap_or("default").to_string();
                let mut co = CheckedOut::take(&state.connections, &conn_name)?;
                let conn = co.get_mut();
                let (lf_send, lf_recv) = conn.lf_channels.take()
                    .ok_or_else(|| anyhow::anyhow!("leios_fetch: no channels (already used?)"))?;

                let points: Vec<NcPoint> =
                    match params.points.as_ref().ok_or_else(|| anyhow::anyhow!("leios_fetch: points is required"))? {
                        BlockFetchPoints::FromChainSync => {
                            anyhow::bail!("leios_fetch: points = \"from_chain_sync\" is not supported; use an explicit array or a $varname reference");
                        }
                        BlockFetchPoints::Explicit(strings) => strings.iter()
                            .map(|s| parse_point(s).map(|p| to_nc_point(&p)))
                            .collect::<anyhow::Result<Vec<NcPoint>>>()?,
                    };

                let eff_trc = tracer.for_peer_opt(conn.peer_id.clone());
                run_leios_fetch(lf_send, lf_recv, points, &eff_trc).await?;
                info!(conn = conn_name, "LeiosFetch complete");
                Ok(())
            }

            StepKind::Repeat => {
                let times_val = resolved
                    .get("times")
                    .ok_or_else(|| anyhow::anyhow!("repeat step requires times"))?;
                let times: u64 = times_val
                    .as_u64()
                    .ok_or_else(|| anyhow::anyhow!("repeat times must be a non-negative integer, got {times_val}"))?;

                // Body steps are parsed from the ORIGINAL raw_params so that
                // each iteration substitutes variables fresh from the current state.
                // Variable resolution happens inside run_step, per-step — a variable
                // set by body step N (e.g. query_tip output: "tip") is immediately
                // visible to body step N+1 (e.g. chain_sync intersection_points: ["$tip.point"])
                // because all body steps share the same &mut RunnerState.vars.
                let body_val = step
                    .raw_params
                    .get("body")
                    .ok_or_else(|| anyhow::anyhow!("repeat step requires body"))?;
                let body: Vec<StepDef> = serde_json::from_value(body_val.clone())
                    .context("invalid repeat body")?;

                for iteration in 0..times {
                    tracer
                        .emit(TraceEvent::new(
                            EventKind::RepeatIterationStarted,
                            Direction::Internal,
                            json!({ "iteration": iteration, "total": times }),
                        ))
                        .await?;

                    for (body_idx, body_step) in body.iter().enumerate() {
                        if let Err(e) = run_step(
                            body_step,
                            body_idx,
                            target_address,
                            network_magic,
                            state,
                            tracer,
                        )
                        .await
                        {
                            tracer
                                .emit(TraceEvent::new(
                                    EventKind::RepeatIterationCompleted,
                                    Direction::Internal,
                                    json!({
                                        "iteration": iteration,
                                        "outcome": "error",
                                        "error": e.to_string(),
                                    }),
                                ))
                                .await?;
                            return Err(e);
                        }
                    }

                    tracer
                        .emit(TraceEvent::new(
                            EventKind::RepeatIterationCompleted,
                            Direction::Internal,
                            json!({ "iteration": iteration, "outcome": "ok" }),
                        ))
                        .await?;
                }
                Ok(())
            }

            StepKind::Disconnect => {
                let conn_name = step.on_name.as_deref().unwrap_or("default").to_string();
                let mut conn = state.connections.lock().unwrap().remove(&conn_name)
                    .ok_or_else(|| anyhow::anyhow!("disconnect: connection \"{conn_name}\" not found"))?;
                conn.ka_channels.take();
                conn.ts_channels.take();
                conn.ln_channels.take();
                conn.lf_channels.take();
                if let Some(h) = conn.ka_handle.take() { h.abort(); }
                if let Some(h) = conn.ts_handle.take() { h.abort(); }
                conn.mux.abort();
                tracer
                    .emit(TraceEvent::new(EventKind::ConnectionClosed, Direction::Internal, json!({}))
                        .with_connection(&conn_name))
                    .await?;
                Ok(())
            }

            StepKind::Sleep => {
                let secs = params.duration_secs.unwrap_or(0);
                info!(secs, "Sleeping");
                tokio::time::sleep(Duration::from_secs(secs)).await;
                Ok(())
            }

            // ── Server-side steps ─────────────────────────────────────────────

            StepKind::Listen => {
                let listener_name = step.as_name.as_deref().unwrap_or("default").to_string();
                let addr = params.bind_address.as_deref().unwrap_or("0.0.0.0:3001");
                let listener = TcpListener::bind(addr)
                    .await
                    .with_context(|| format!("listen: failed to bind {addr}"))?;
                tracer
                    .emit(TraceEvent::new(EventKind::ServerListenStarted, Direction::Internal,
                        json!({ "bind_address": addr })))
                    .await?;
                info!(%addr, listener = listener_name, "Listener started");
                state.listeners.lock().unwrap().insert(listener_name, ServerListener { listener: Arc::new(listener) });
                Ok(())
            }

            StepKind::AcceptHandshake => {
                let listener_name = step.on_name.as_deref().unwrap_or("default");
                let conn_name = step.as_name.as_deref().unwrap_or("default").to_string();

                let listener_arc = {
                    let guard = state.listeners.lock().unwrap();
                    let sl = guard.get(listener_name)
                        .ok_or_else(|| anyhow::anyhow!("accept_handshake: listener \"{listener_name}\" not found"))?;
                    Arc::clone(&sl.listener)
                };
                let (bearer, peer_addr) = TcpBearer::accept(&listener_arc)
                    .await
                    .context("accept_handshake: TcpBearer::accept failed")?;

                tracer
                    .emit(TraceEvent::new(EventKind::ServerBearerAccepted, Direction::Internal,
                        json!({ "peer_address": peer_addr.to_string() }))
                        .with_connection(&conn_name))
                    .await?;

                // Phase 1: register all server-side channels with the mux and spawn it.
                let mut mux = Mux::new(MuxConfig::default(), AnyScheduler::default(), MODE_RESPONDER);
                let (hs_send, hs_recv) = mux.register(&ProtocolConfig {
                    id: net_core::protocols::handshake::PROTOCOL_ID,
                    traffic_class: TrafficClass::Priority,
                    ingress_limit: net_core::protocols::handshake::SIZE_LIMIT,
                    egress_queue_size: 4,
                });
                let (cs_send, cs_recv) = mux.register(&ProtocolConfig {
                    id: CHAIN_SYNC_PROTOCOL,
                    traffic_class: TrafficClass::Default(1),
                    ingress_limit: net_core::protocols::chainsync::INGRESS_LIMIT,
                    egress_queue_size: 16,
                });
                let (bf_send, bf_recv) = mux.register(&ProtocolConfig {
                    id: BLOCK_FETCH_PROTOCOL,
                    traffic_class: TrafficClass::Default(1),
                    ingress_limit: net_core::protocols::blockfetch::INGRESS_LIMIT,
                    egress_queue_size: 16,
                });
                let (ts_send, ts_recv) = mux.register(&ProtocolConfig {
                    id: TX_SUBMISSION_PROTOCOL,
                    traffic_class: TrafficClass::Default(1),
                    ingress_limit: net_core::protocols::txsubmission::INGRESS_LIMIT,
                    egress_queue_size: 16,
                });
                let (ka_send, ka_recv) = mux.register(&ProtocolConfig {
                    id: KEEP_ALIVE_PROTOCOL,
                    traffic_class: TrafficClass::Default(1),
                    ingress_limit: net_core::protocols::keepalive::INGRESS_LIMIT,
                    egress_queue_size: 4,
                });
                let (ln_send, ln_recv) = mux.register(&ProtocolConfig {
                    id: LEIOS_NOTIFY_PROTOCOL,
                    traffic_class: TrafficClass::Default(1),
                    ingress_limit: net_core::protocols::leios_notify::INGRESS_LIMIT,
                    egress_queue_size: 16,
                });
                let (lf_send, lf_recv) = mux.register(&ProtocolConfig {
                    id: LEIOS_FETCH_PROTOCOL,
                    traffic_class: TrafficClass::Default(1),
                    ingress_limit: net_core::protocols::leios_fetch::INGRESS_LIMIT,
                    egress_queue_size: 16,
                });
                let server_mux = mux.run(bearer);

                // Phase 2: complete the handshake as responder.
                let proposed_versions = Arc::new(Mutex::new(Vec::<u64>::new()));
                let pv_capture = Arc::clone(&proposed_versions);
                let server_data = n2n::VersionData {
                    network_magic,
                    initiator_only_diffusion_mode: false,
                    peer_sharing: 0,
                    query: false,
                };
                let hs_result = nc_handshake::run_server(
                    CodecSend::new(hs_send),
                    CodecRecv::new(hs_recv),
                    move |client_table| {
                        *pv_capture.lock().unwrap() = client_table.keys().cloned().collect();
                        n2n::negotiate(client_table, &server_data)
                    },
                ).await;

                let mut proposed_vers = proposed_versions.lock().unwrap().clone();
                proposed_vers.sort();

                tracer
                    .emit(TraceEvent::new(EventKind::HandshakeVersionProposed, Direction::Received,
                        json!({ "versions": proposed_vers, "magic": network_magic }))
                        .with_connection(&conn_name))
                    .await?;

                let version = match hs_result {
                    Ok((v, _)) => v,
                    Err(e) => {
                        server_mux.abort();
                        anyhow::bail!("accept_handshake: handshake failed: {e}");
                    }
                };

                tracer
                    .emit(TraceEvent::new(EventKind::HandshakeVersionAccepted, Direction::Sent,
                        json!({ "version": version }))
                        .with_connection(&conn_name))
                    .await?;
                tracer
                    .emit(TraceEvent::new(EventKind::ServerHandshakeAccepted, Direction::Internal,
                        json!({ "peer_address": peer_addr.to_string(), "negotiated_version": version }))
                        .with_connection(&conn_name))
                    .await?;

                let ka_handle = tokio::spawn(run_keepalive_server(
                    CodecSend::new(ka_send),
                    CodecRecv::new(ka_recv),
                ));
                tracer
                    .emit(TraceEvent::new(EventKind::ProtocolWorkersStarted, Direction::Internal,
                        json!({ "protocols": ["keep-alive"] }))
                        .with_connection(&conn_name))
                    .await?;

                info!(version, %peer_addr, conn = conn_name, "Handshake accepted as server");
                state.server_connections.lock().unwrap().insert(conn_name, ServerConnection {
                    mux: server_mux,
                    cs_channels: Some((CodecSend::new(cs_send), CodecRecv::new(cs_recv))),
                    bf_channels: Some((CodecSend::new(bf_send), CodecRecv::new(bf_recv))),
                    ts_channels: Some((CodecSend::new(ts_send), CodecRecv::new(ts_recv))),
                    ln_channels: Some((CodecSend::new(ln_send), CodecRecv::new(ln_recv))),
                    lf_channels: Some((CodecSend::new(lf_send), CodecRecv::new(lf_recv))),
                    ka_handle: Some(ka_handle),
                    peer_id: params.peer_id.clone(),
                });
                Ok(())
            }

            StepKind::ServeChainSync => {
                let conn_name = step.on_name.as_deref().unwrap_or("default").to_string();
                let mut co = CheckedOut::take(&state.server_connections, &conn_name)?;
                let sc = co.get_mut();
                let (cs_send, mut cs_recv) = sc.cs_channels.take()
                    .ok_or_else(|| anyhow::anyhow!("serve_chain_sync: no chain-sync channel"))?;

                // as_peer: clone the peer's current chain (filtered by slot) from PeerState
                // and override the connection's peer_id. The lock is released before any I/O.
                // as_peer takes precedence over fixture_path (mutually exclusive, validated).
                let fixture_opt: Option<crate::scenario::fixture::FixtureChain> =
                    if let Some(ref as_peer_id) = params.as_peer {
                        let _ = lookup_peer(&state.network, as_peer_id)?;
                        sc.peer_id = Some(as_peer_id.clone());
                        let current_slot_val = state.current_slot
                            .as_ref().map(|a| a.load(Ordering::Relaxed));
                        let chain = {
                            let guard = state.peers.lock().unwrap();
                            let ps = guard.get(as_peer_id.as_str()).ok_or_else(|| {
                                anyhow::anyhow!(
                                    "serve_chain_sync: peer \"{as_peer_id}\" has no state; \
                                     declare it in the network block"
                                )
                            })?;
                            if ps.chain_entries.is_empty() && ps.chain_tip_slot().is_none() {
                                None  // empty chain — auto-generate will produce empty script
                            } else {
                                Some(ps.filtered_fixture_chain(current_slot_val))
                            }
                        };
                        chain
                    } else if let Some(ref path) = params.fixture_path {
                        let chain = fixture::load(std::path::Path::new(path.as_str()))
                            .with_context(|| format!("serve_chain_sync: loading fixture \"{path}\""))?;
                        Some(chain)
                    } else {
                        None
                    };

                let await_secs = params.await_at_tip_secs.unwrap_or(30).min(300);
                let eff_trc = tracer.for_peer_opt(sc.peer_id.clone());

                // Build the response script.
                let script = match step.raw_params.get("responses") {
                    Some(responses_val) => {
                        // Explicit responses list — fixture (if present) only provides
                        // header sources for header_from_fixture references.
                        let defs: Vec<crate::scenario::response_rules::ResponseRuleDef> =
                            serde_json::from_value((*responses_val).clone())
                                .context("serve_chain_sync: parsing responses")?;
                        defs.iter()
                            .enumerate()
                            .map(|(i, d)| {
                                rule_def_to_script(d, fixture_opt.as_ref(), None).with_context(|| {
                                    format!("serve_chain_sync: responses[{i}]")
                                })
                            })
                            .collect::<anyhow::Result<Vec<_>>>()?
                    }
                    None => {
                        // Auto-generate from fixture.
                        let chain = fixture_opt.as_ref().ok_or_else(|| {
                            anyhow::anyhow!("serve_chain_sync: fixture_path or as_peer is required when responses is not set")
                        })?;
                        generate_from_fixture(chain, await_secs)
                    }
                };

                let summary = execute_response_script(
                    &cs_send,
                    &mut cs_recv,
                    &script,
                    fixture_opt.as_ref(),
                    &eff_trc,
                )
                .await?;

                info!(
                    headers_served = summary.headers_served,
                    rules_applied  = summary.rules_applied,
                    duration_ms    = summary.duration_ms,
                    "serve_chain_sync step complete"
                );
                Ok(())
            }

            StepKind::ServeBlockFetch => {
                let conn_name = step.on_name.as_deref().unwrap_or("default").to_string();
                let mut co = CheckedOut::take(&state.server_connections, &conn_name)?;
                let sc = co.get_mut();
                let (bf_send, mut bf_recv) = sc.bf_channels.take()
                    .ok_or_else(|| anyhow::anyhow!("serve_block_fetch: no block-fetch channel"))?;

                // as_peer: clone the peer's block_store from PeerState and override peer_id.
                // Lock is released before any I/O.
                let bf_fixture_opt: Option<crate::scenario::block_fixture::BlockFixtureChain> =
                    if let Some(ref as_peer_id) = params.as_peer {
                        let _ = lookup_peer(&state.network, as_peer_id)?;
                        sc.peer_id = Some(as_peer_id.clone());
                        let chain = {
                            let guard = state.peers.lock().unwrap();
                            let ps = guard.get(as_peer_id.as_str()).ok_or_else(|| {
                                anyhow::anyhow!(
                                    "serve_block_fetch: peer \"{as_peer_id}\" has no state; \
                                     declare it in the network block"
                                )
                            })?;
                            ps.to_block_fixture_chain()
                        };
                        if chain.entries.is_empty() { None } else { Some(chain) }
                    } else if let Some(ref path) = params.block_fetch_fixture_path {
                        let chain = block_fixture::load(std::path::Path::new(path.as_str()))
                            .with_context(|| format!("serve_block_fetch: loading fixture \"{path}\""))?;
                        Some(chain)
                    } else {
                        None
                    };

                let eff_trc = tracer.for_peer_opt(sc.peer_id.clone());

                // Build the response script.
                let script = match step.raw_params.get("responses") {
                    Some(responses_val) => {
                        let defs: Vec<crate::scenario::response_rules::ResponseRuleDef> =
                            serde_json::from_value((*responses_val).clone())
                                .context("serve_block_fetch: parsing responses")?;
                        defs.iter()
                            .enumerate()
                            .map(|(i, d)| {
                                rule_def_to_script(d, None, bf_fixture_opt.as_ref())
                                    .with_context(|| format!("serve_block_fetch: responses[{i}]"))
                            })
                            .collect::<anyhow::Result<Vec<_>>>()?
                    }
                    None => {
                        let no_blocks = params.no_blocks_default.unwrap_or(false);
                        generate_for_block_fetch(no_blocks)
                    }
                };

                let summary = execute_block_fetch_script(
                    &bf_send,
                    &mut bf_recv,
                    &script,
                    bf_fixture_opt.as_ref(),
                    &eff_trc,
                )
                .await?;

                info!(
                    blocks_served = summary.blocks_served,
                    duration_ms   = summary.duration_ms,
                    "serve_block_fetch step complete"
                );
                Ok(())
            }

            StepKind::ServeLeiosNotify => {
                let conn_name = step.on_name.as_deref().unwrap_or("default").to_string();
                let mut co = CheckedOut::take(&state.server_connections, &conn_name)?;
                let sc = co.get_mut();
                let (ln_send, ln_recv) = sc.ln_channels.take()
                    .ok_or_else(|| anyhow::anyhow!("serve_leios_notify: no leios-notify channel"))?;
                let actions: Vec<LeiosNotifyAction> = match step.raw_params.get("notifications") {
                    Some(v) => serde_json::from_value((*v).clone())
                        .context("serve_leios_notify: parsing notifications")?,
                    None => vec![],
                };
                let eff_trc = tracer.for_peer_opt(sc.peer_id.clone());
                let summary = execute_leios_notify_script(ln_send, ln_recv, actions, &eff_trc).await?;
                info!(notifications_sent = summary.notifications_sent, "serve_leios_notify complete");
                Ok(())
            }

            StepKind::ServeLeiosFetch => {
                let conn_name = step.on_name.as_deref().unwrap_or("default").to_string();
                let mut co = CheckedOut::take(&state.server_connections, &conn_name)?;
                let sc = co.get_mut();
                let (lf_send, lf_recv) = sc.lf_channels.take()
                    .ok_or_else(|| anyhow::anyhow!("serve_leios_fetch: no leios-fetch channel"))?;
                let rules: Vec<LeiosFetchRule> = match step.raw_params.get("responses") {
                    Some(v) => serde_json::from_value((*v).clone())
                        .context("serve_leios_fetch: parsing responses")?,
                    None => vec![],
                };
                let eff_trc = tracer.for_peer_opt(sc.peer_id.clone());
                let summary = execute_leios_fetch_script(lf_send, lf_recv, rules, &eff_trc).await?;
                info!(blocks_served = summary.blocks_served, "serve_leios_fetch complete");
                Ok(())
            }

            StepKind::CloseListener => {
                let listener_name = step.on_name.as_deref().unwrap_or("default").to_string();
                state.listeners.lock().unwrap().remove(&listener_name);
                // Server connections created via this listener remain open until
                // explicitly disconnected or until cleanup runs.
                tracer
                    .emit(TraceEvent::new(EventKind::ServerListenStopped, Direction::Internal, json!({})))
                    .await?;
                info!(listener = listener_name, "Listener stopped");
                Ok(())
            }

            StepKind::EmitPeerEvent => {
                let peer_id = params.peer_id.clone()
                    .ok_or_else(|| anyhow::anyhow!("emit_peer_event: peer_id is required"))?;
                let event_kind_str = params.event_kind.as_deref()
                    .ok_or_else(|| anyhow::anyhow!("emit_peer_event: event_kind is required"))?;
                let payload = resolved.get("payload").cloned().unwrap_or(json!({}));

                let kind = match event_kind_str {
                    "peer_produced_block" => EventKind::PeerProducedBlock,
                    "peer_cast_vote"      => EventKind::PeerCastVote,
                    "peer_forked_chain"   => EventKind::PeerForkedChain,
                    "peer_joined_network" => EventKind::PeerJoinedNetwork,
                    "peer_left_network"   => EventKind::PeerLeftNetwork,
                    other => {
                        tracing::warn!(kind = other, "unknown peer event_kind; emitting as peer_network_event");
                        EventKind::PeerNetworkEvent
                    }
                };

                tracer
                    .emit(TraceEvent::new(kind, Direction::Internal, payload).with_peer_id(peer_id))
                    .await?;
                Ok(())
            }

            StepKind::PeerExtendsChain => {
                let peer_id = params.peer_id.as_deref()
                    .ok_or_else(|| anyhow::anyhow!("peer_extends_chain: peer_id is required"))?;
                let slot = params.slot
                    .ok_or_else(|| anyhow::anyhow!("peer_extends_chain: slot is required"))?;
                let block_number = params.block_number
                    .ok_or_else(|| anyhow::anyhow!("peer_extends_chain: block_number is required"))?;
                let block_hash_hex = params.block_hash.as_deref()
                    .ok_or_else(|| anyhow::anyhow!("peer_extends_chain: block_hash is required"))?;
                let header_cbor_hex = params.header_cbor.as_deref()
                    .ok_or_else(|| anyhow::anyhow!("peer_extends_chain: header_cbor is required"))?;

                // Decode outside the lock — decoding is not instant and the lock should be brief.
                let block_hash   = decode_hex_bytes(block_hash_hex).context("peer_extends_chain: block_hash")?;
                let header_cbor  = decode_hex_bytes(header_cbor_hex).context("peer_extends_chain: header_cbor")?;
                let variant      = params.variant.unwrap_or(DEFAULT_HEADER_VARIANT);
                let body_opt: Option<Vec<u8>> = params.block_body_cbor.as_deref()
                    .map(|hex| decode_hex_bytes(hex).context("peer_extends_chain: block_body_cbor"))
                    .transpose()?;

                // Brief lock: validate monotonic order and push the new entry.
                {
                    let mut guard = state.peers.lock().unwrap();
                    let ps = guard.get_mut(peer_id).ok_or_else(|| {
                        anyhow::anyhow!(
                            "peer_extends_chain: peer \"{peer_id}\" not found — \
                             declare it in the network block"
                        )
                    })?;

                    // Monotonic slot check. Accept any slot when chain is empty;
                    // a misleading "tip slot 0" message would be confusing.
                    if let Some(tip_slot) = ps.chain_tip_slot() {
                        if slot <= tip_slot {
                            anyhow::bail!(
                                "peer_extends_chain: slot {slot} is not greater than \
                                 current chain tip slot {tip_slot}"
                            );
                        }
                    }

                    ps.chain_entries.push(ChainEntry {
                        slot,
                        block_hash: block_hash.clone(),
                        block_number,
                        header_cbor,
                        variant,
                    });
                    if let Some(body) = body_opt {
                        ps.block_store.insert((slot, block_hash.clone()), body);
                    }
                }

                tracer.emit(TraceEvent::new(
                    EventKind::PeerChainExtended,
                    Direction::Internal,
                    json!({
                        "peer_id":      peer_id,
                        "slot":         slot,
                        "block_hash":   block_hash_hex,
                        "block_number": block_number,
                        "source":       "explicit",
                    }),
                )).await?;
                Ok(())
            }

            StepKind::AdvanceToSlot => {
                let new_slot = params.slot
                    .ok_or_else(|| anyhow::anyhow!("advance_to_slot: slot parameter is required"))?;
                let slot_arc = state.current_slot.as_ref()
                    .ok_or_else(|| anyhow::anyhow!(
                        "advance_to_slot: no network declaration — current_slot is not initialized"
                    ))?;
                // Relaxed: slot is a logical counter, not a memory-ordering barrier.
                // The SlotAdvanced trace event is the authoritative record of changes.
                let current = slot_arc.load(Ordering::Relaxed);
                if new_slot <= current {
                    anyhow::bail!(
                        "advance_to_slot: target slot {new_slot} is not greater than \
                         current slot {current}"
                    );
                }
                slot_arc.store(new_slot, Ordering::Relaxed); // Relaxed: see above
                // Apply production rules for (current, new_slot] before emitting events.
                let prod_events = {
                    let mut guard = state.peers.lock().unwrap();
                    apply_production_rules(&mut guard, current, new_slot)
                };
                emit_slot_advanced(current, new_slot, "advance_to_slot", tracer).await?;
                emit_production_events(&prod_events, tracer).await?;
                Ok(())
            }

            StepKind::TickSlots => {
                let count = params.count
                    .ok_or_else(|| anyhow::anyhow!("tick_slots: count parameter is required"))?;
                let slot_arc = state.current_slot.as_ref()
                    .ok_or_else(|| anyhow::anyhow!(
                        "tick_slots: no network declaration — current_slot is not initialized"
                    ))?;
                // fetch_add with Relaxed: atomic increment of a monotonic counter.
                // Returns the value before the addition; new value = old + count.
                let old_slot = slot_arc.fetch_add(count, Ordering::Relaxed);
                let new_slot = old_slot + count;
                let prod_events = {
                    let mut guard = state.peers.lock().unwrap();
                    apply_production_rules(&mut guard, old_slot, new_slot)
                };
                emit_slot_advanced(old_slot, new_slot, "tick_slots", tracer).await?;
                emit_production_events(&prod_events, tracer).await?;
                Ok(())
            }

            StepKind::Parallel => {
                let branches_val = step
                    .raw_params
                    .get("branches")
                    .ok_or_else(|| anyhow::anyhow!("parallel step requires branches"))?;
                let branches: Vec<Vec<StepDef>> = serde_json::from_value(branches_val.clone())
                    .context("parallel: invalid branches")?;

                let branch_count = branches.len();

                tracer
                    .emit(TraceEvent::new(
                        EventKind::ParallelStarted,
                        Direction::Internal,
                        json!({ "branch_count": branch_count }),
                    ))
                    .await?;

                // Per-branch flags written by the futures before they return or are dropped.
                // After try_join_all resolves we inspect these to emit branch_aborted for
                // any branch that was still in-flight when the failing branch returned Err.
                let completed: Vec<Arc<AtomicBool>> = (0..branch_count)
                    .map(|_| Arc::new(AtomicBool::new(false)))
                    .collect();
                let failed: Vec<Arc<AtomicBool>> = (0..branch_count)
                    .map(|_| Arc::new(AtomicBool::new(false)))
                    .collect();
                // Last step index each branch reached (set before each run_step call).
                let last_step: Vec<Arc<AtomicUsize>> = (0..branch_count)
                    .map(|_| Arc::new(AtomicUsize::new(0)))
                    .collect();

                let branch_futs: Vec<_> = branches
                    .into_iter()
                    .enumerate()
                    .map(|(bi, branch_steps)| {
                        let mut branch_state = state.clone();
                        let branch_tracer = tracer.clone();
                        let addr = target_address.to_string();
                        let completed_flag = Arc::clone(&completed[bi]);
                        let failed_flag   = Arc::clone(&failed[bi]);
                        let last_step_flag = Arc::clone(&last_step[bi]);
                        async move {
                            branch_tracer
                                .emit(TraceEvent::new(
                                    EventKind::ParallelBranchStarted,
                                    Direction::Internal,
                                    json!({ "branch": bi }),
                                ))
                                .await?;
                            for (si, step) in branch_steps.iter().enumerate() {
                                last_step_flag.store(si, Ordering::Relaxed);
                                if let Err(e) = run_step(
                                    step,
                                    si,
                                    &addr,
                                    network_magic,
                                    &mut branch_state,
                                    &branch_tracer,
                                )
                                .await
                                {
                                    failed_flag.store(true, Ordering::Relaxed);
                                    let _ = branch_tracer
                                        .emit(TraceEvent::new(
                                            EventKind::ParallelBranchFailed,
                                            Direction::Internal,
                                            json!({ "branch": bi, "step": si, "error": e.to_string() }),
                                        ))
                                        .await;
                                    return Err(e);
                                }
                            }
                            completed_flag.store(true, Ordering::Relaxed);
                            branch_tracer
                                .emit(TraceEvent::new(
                                    EventKind::ParallelBranchCompleted,
                                    Direction::Internal,
                                    json!({ "branch": bi }),
                                ))
                                .await?;
                            Ok::<(), anyhow::Error>(())
                        }
                    })
                    .collect();

                let result: anyhow::Result<Vec<()>> = try_join_all(branch_futs).await;

                // try_join_all drops all remaining futures on the first Err.
                // Those futures never wrote completed=true or failed=true, so we
                // emit branch_aborted from the parent task (which is still running)
                // to give the trace a clean completion event for every branch.
                if result.is_err() {
                    for bi in 0..branch_count {
                        if !completed[bi].load(Ordering::Relaxed)
                            && !failed[bi].load(Ordering::Relaxed)
                        {
                            let last = last_step[bi].load(Ordering::Relaxed);
                            let _ = tracer
                                .emit(TraceEvent::new(
                                    EventKind::ParallelBranchAborted,
                                    Direction::Internal,
                                    json!({ "branch": bi, "last_step": last }),
                                ))
                                .await;
                        }
                    }
                }

                tracer
                    .emit(TraceEvent::new(
                        EventKind::ParallelCompleted,
                        Direction::Internal,
                        json!({ "outcome": if result.is_ok() { "ok" } else { "error" } }),
                    ))
                    .await?;

                result.map(|_| ())
            }
        }
    })
}

// ── query_tip implementation ──────────────────────────────────────────────────

/// Opens a temporary TCP connection to `target_address`, performs a handshake,
/// then does a minimal Chain-Sync round-trip to obtain the current chain tip.
/// Returns `(point, block_number)`.
async fn fetch_tip(target_address: &str, network_magic: u64, tracer: &Tracer) -> anyhow::Result<Tip> {
    use net_core::protocols::chainsync::ChainSync;

    let socket_addr = target_address
        .to_socket_addrs()
        .with_context(|| format!("query_tip: failed to resolve {target_address}"))?
        .next()
        .ok_or_else(|| anyhow::anyhow!("query_tip: no address for {target_address}"))?;

    let bearer = TcpBearer::connect(socket_addr)
        .await
        .with_context(|| format!("query_tip: failed to connect to {target_address}"))?;

    let mut mux = Mux::new(MuxConfig::default(), AnyScheduler::default(), MODE_INITIATOR);
    let (hs_send, hs_recv) = mux.register(&ProtocolConfig {
        id: net_core::protocols::handshake::PROTOCOL_ID,
        traffic_class: TrafficClass::Priority,
        ingress_limit: net_core::protocols::handshake::SIZE_LIMIT,
        egress_queue_size: 4,
    });
    let (cs_send, cs_recv) = mux.register(&ProtocolConfig {
        id: CHAIN_SYNC_PROTOCOL,
        traffic_class: TrafficClass::Default(1),
        ingress_limit: net_core::protocols::chainsync::INGRESS_LIMIT,
        egress_queue_size: 16,
    });
    let running = mux.run(bearer);

    handshake_on_channels(
        CodecSend::new(hs_send),
        CodecRecv::new(hs_recv),
        network_magic,
        tracer,
    )
    .await
    .context("query_tip: handshake failed")?;

    let mut runner = Runner::<ChainSync>::new(
        Role::Client,
        CodecSend::new(cs_send),
        CodecRecv::new(cs_recv),
    );

    // FindIntersect at Origin — server always responds with IntersectFound(Origin, tip).
    let tip = match nc_chainsync::find_intersection(&mut runner, vec![NcPoint::Origin]).await {
        Ok(Some((_, nc_tip))) => {
            let nc_point = &nc_tip.point;
            let pallas_point = match nc_point {
                NcPoint::Origin => Point::Origin,
                NcPoint::Specific { slot, hash } => Point::Specific(*slot, hash.to_vec()),
            };
            Tip(pallas_point, nc_tip.block_no)
        }
        Ok(None) => Tip(Point::Origin, 0),
        Err(e) => return Err(anyhow::anyhow!("query_tip: find_intersect failed: {e}")),
    };

    nc_chainsync::done(&mut runner).await.ok();
    running.abort();

    Ok(tip)
}

// ── Shared utilities ──────────────────────────────────────────────────────────

/// Close all open connections and listeners. Called on error/assertion-failure
/// paths before returning from `run`.
async fn cleanup(state: &mut RunnerState, tracer: &Tracer) {
    // Drain under a brief lock, then release before async work.
    let server_conns: Vec<(String, ServerConnection)> =
        state.server_connections.lock().unwrap().drain().collect();
    for (name, mut sc) in server_conns {
        if let Some(h) = sc.ka_handle.take() { h.abort(); }
        sc.mux.abort();
        let _ = tracer.emit(TraceEvent::new(EventKind::ConnectionClosed, Direction::Internal,
            json!({ "reason": "scenario_aborted" })).with_connection(&name)).await;
    }
    // Drop all listeners.
    state.listeners.lock().unwrap().clear();
    // Close all outgoing connections.
    let conns: Vec<(String, ClientConnection)> =
        state.connections.lock().unwrap().drain().collect();
    for (name, mut cc) in conns {
        cc.ka_channels.take();
        cc.ts_channels.take();
        cc.ln_channels.take();
        cc.lf_channels.take();
        if let Some(h) = cc.ka_handle.take() { h.abort(); }
        if let Some(h) = cc.ts_handle.take() { h.abort(); }
        cc.mux.abort();
        let _ = tracer.emit(TraceEvent::new(EventKind::ConnectionClosed, Direction::Internal,
            json!({ "reason": "scenario_aborted" })).with_connection(&name)).await;
    }
}

async fn emit_completed(
    tracer: &Tracer,
    name: &str,
    steps_passed: u64,
    steps_failed: u64,
    started_at: Instant,
    outcome: &str,
) {
    let _ = tracer
        .emit(TraceEvent::new(
            EventKind::ScenarioCompleted,
            Direction::Internal,
            json!({
                "name":         name,
                "steps_passed": steps_passed,
                "steps_failed": steps_failed,
                "duration_ms":  started_at.elapsed().as_millis() as u64,
                "outcome":      outcome,
            }),
        ))
        .await;
}

// ── Assertion evaluator ───────────────────────────────────────────────────────

pub struct AssertionResult {
    pub name: String,
    pub passed: bool,
    pub message: String,
}

pub fn evaluate_assertions(
    assertions: &crate::scenario::Assertions,
    events: &[Value],
) -> Vec<AssertionResult> {
    let mut results = Vec::new();

    if let Some(min) = assertions.min_events {
        let passed = events.len() >= min;
        results.push(AssertionResult {
            name: format!("min_events >= {min}"),
            passed,
            message: format!("got {} events, required >= {min}", events.len()),
        });
    }

    if let Some(required_kinds) = &assertions.must_contain_kind {
        for kind in required_kinds {
            let found = events.iter().any(|e| e["kind"] == kind.as_str());
            results.push(AssertionResult {
                name: format!("must_contain_kind:{kind}"),
                passed: found,
                message: if found {
                    format!("found event with kind \"{kind}\"")
                } else {
                    format!("no event with kind \"{kind}\" was found")
                },
            });
        }
    }

    if let Some(forbidden_kinds) = &assertions.must_not_contain_kind {
        for kind in forbidden_kinds {
            let found = events.iter().any(|e| e["kind"] == kind.as_str());
            results.push(AssertionResult {
                name: format!("must_not_contain_kind:{kind}"),
                passed: !found,
                message: if found {
                    format!("unexpected event with kind \"{kind}\" was found")
                } else {
                    format!("no event with kind \"{kind}\" (correct)")
                },
            });
        }
    }

    results
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scenario::{Network, Peer};
    use crate::scenario::Assertions;

    fn events(kinds: &[&str]) -> Vec<Value> {
        kinds.iter().map(|k| json!({ "kind": k })).collect()
    }

    fn assert_all_pass(results: &[AssertionResult]) {
        for r in results {
            assert!(r.passed, "assertion failed: {} — {}", r.name, r.message);
        }
    }

    fn assert_any_fail(results: &[AssertionResult]) {
        assert!(
            results.iter().any(|r| !r.passed),
            "expected at least one failed assertion, but all passed"
        );
    }

    #[test]
    fn empty_assertions_always_pass() {
        let a = Assertions::default();
        assert!(evaluate_assertions(&a, &events(&["foo"])).is_empty());
    }

    #[test]
    fn min_events_passes() {
        let a = Assertions { min_events: Some(2), ..Default::default() };
        assert_all_pass(&evaluate_assertions(&a, &events(&["a", "b", "c"])));
    }

    #[test]
    fn min_events_fails() {
        let a = Assertions { min_events: Some(5), ..Default::default() };
        assert_any_fail(&evaluate_assertions(&a, &events(&["a", "b"])));
    }

    #[test]
    fn must_contain_kind_passes() {
        let a = Assertions {
            must_contain_kind: Some(vec!["chain_sync_roll_forward".into()]),
            ..Default::default()
        };
        assert_all_pass(&evaluate_assertions(
            &a,
            &events(&["chain_sync_started", "chain_sync_roll_forward"]),
        ));
    }

    #[test]
    fn must_contain_kind_fails() {
        let a = Assertions {
            must_contain_kind: Some(vec!["chain_sync_roll_forward".into()]),
            ..Default::default()
        };
        assert_any_fail(&evaluate_assertions(&a, &events(&["chain_sync_started"])));
    }

    #[test]
    fn must_not_contain_kind_passes() {
        let a = Assertions {
            must_not_contain_kind: Some(vec!["error".into()]),
            ..Default::default()
        };
        assert_all_pass(&evaluate_assertions(&a, &events(&["handshake_completed"])));
    }

    #[test]
    fn must_not_contain_kind_fails() {
        let a = Assertions {
            must_not_contain_kind: Some(vec!["error".into()]),
            ..Default::default()
        };
        assert_any_fail(&evaluate_assertions(
            &a,
            &events(&["error", "handshake_completed"]),
        ));
    }

    #[test]
    fn multiple_assertions_all_pass() {
        let a = Assertions {
            min_events: Some(2),
            must_contain_kind: Some(vec!["handshake_completed".into()]),
            must_not_contain_kind: Some(vec!["error".into()]),
        };
        assert_all_pass(&evaluate_assertions(
            &a,
            &events(&["handshake_started", "handshake_completed"]),
        ));
    }

    #[test]
    fn multiple_assertions_partial_fail() {
        let a = Assertions {
            min_events: Some(10),
            must_contain_kind: Some(vec!["handshake_completed".into()]),
            must_not_contain_kind: None,
        };
        let results = evaluate_assertions(&a, &events(&["handshake_completed"]));
        assert!(results.iter().any(|r| r.passed));
        assert!(results.iter().any(|r| !r.passed));
    }

    #[test]
    fn subscribed_protocols_contains_full_n2n_suite() {
        for &version in &[7u64, 11, 13, 14] {
            let ps = subscribed_protocols(version);
            assert!(ps.contains(&CHAIN_SYNC_PROTOCOL),   "v{version}: missing chain-sync");
            assert!(ps.contains(&BLOCK_FETCH_PROTOCOL),  "v{version}: missing block-fetch");
            assert!(ps.contains(&TX_SUBMISSION_PROTOCOL), "v{version}: missing tx-submission");
            assert!(ps.contains(&KEEP_ALIVE_PROTOCOL),    "v{version}: missing keep-alive");
        }
    }

    /// Background workers must not exist until after handshake. This encodes
    /// the lifecycle invariant: Connect is structural (channels only), Handshake
    /// is behavioral (spawns workers). See the Connect and Handshake step arms
    /// in execute_step for the authoritative comments explaining why.
    #[test]
    fn runner_state_starts_with_no_workers() {
        // With the HashMap-based state, an empty RunnerState has no connections at all.
        // Workers are stored inside ClientConnection entries which don't exist until connect.
        let state = RunnerState::default();
        assert!(state.connections.lock().unwrap().is_empty(), "no connections before connect");
        assert!(state.listeners.lock().unwrap().is_empty(),   "no listeners before listen");
        assert!(state.server_connections.lock().unwrap().is_empty(), "no server connections before accept_handshake");
    }

    #[test]
    fn network_without_start_slot_does_not_activate_slot_tracking() {
        // Pins the invariant: declaring a network for peer vocabulary but omitting
        // start_slot must NOT activate slot tracking. The runner's initialization is:
        //
        //   scenario.network.as_ref().and_then(|n| n.start_slot).map(Arc::new(AtomicU64::new(s)))
        //
        // The original bug used `.map(|n| Arc::new(AtomicU64::new(n.start_slot.unwrap_or(0))))`,
        // which produced Some(0) for any network-declaring scenario and silently hid all
        // fixture entries above slot 0 (e.g. devnet entries at slots 138+).
        //
        // This test replicates the initialization formula directly. A regression to
        // unwrap_or(0) would produce Some(0) here and the assertion would fail.
        let network: Option<Network> = Some(Network {
            peers: vec![Peer {
                id: "p".to_string(),
                chain_sync_fixture: Some("fixtures/devnet_genesis.jsonl".into()),
                block_fetch_fixture: None,
                description: None,
                production_rule: None,
            }],
            start_slot: None,     // explicitly absent
            slot_length_ms: None,
        });

        let current_slot: Option<Arc<AtomicU64>> = network
            .as_ref()
            .and_then(|n| n.start_slot)           // None when start_slot absent
            .map(|s| Arc::new(AtomicU64::new(s))); // never reached

        assert!(
            current_slot.is_none(),
            "network without start_slot must yield current_slot = None; \
             got Some(_) — regression: unwrap_or(0) would activate slot tracking \
             at slot 0 and hide all fixture entries with slot > 0"
        );
    }

    /// After Connect, channels are allocated but workers are not yet running.
    /// After Handshake, workers are running and channels are consumed.
    ///
    /// Run with: cargo test -p cardano-conformance-harness scenario::runner::tests::background_workers_spawned_after_handshake_not_before -- --ignored
    #[tokio::test]
    #[ignore = "requires devnet: docker compose up"]
    async fn background_workers_spawned_after_handshake_not_before() {
        use crate::scenario::StepDef;
        use tempfile::NamedTempFile;

        let tmp = NamedTempFile::new().unwrap();
        let tracer = crate::trace::Tracer::open(tmp.path()).await.unwrap();
        let mut state = RunnerState::default();

        let connect = StepDef {
            kind: StepKind::Connect,
            raw_params: json!({}),
            output: None,
            as_name: None,
            on_name: None,
            expect: None,
        };
        execute_step(&connect, &json!({}), "localhost:3001", crate::DEVNET_MAGIC, &mut state, &tracer)
            .await
            .expect("connect should succeed");

        {
            let guard = state.connections.lock().unwrap();
            let conn = guard.get("default").expect("default connection");
            assert!(conn.ka_handle.is_none(),   "ka_handle must be None after Connect");
            assert!(conn.ts_handle.is_none(),   "ts_handle must be None after Connect");
            assert!(conn.ka_channels.is_some(), "ka_channels must be Some after Connect");
            assert!(conn.ts_channels.is_some(), "ts_channels must be Some after Connect");
        }

        let handshake = StepDef {
            kind: StepKind::Handshake,
            raw_params: json!({}),
            output: None,
            as_name: None,
            on_name: None,
            expect: None,
        };
        execute_step(&handshake, &json!({}), "localhost:3001", crate::DEVNET_MAGIC, &mut state, &tracer)
            .await
            .expect("handshake should succeed");

        {
            let guard = state.connections.lock().unwrap();
            let conn = guard.get("default").expect("default connection");
            assert!(conn.ka_handle.is_some(),   "ka_handle must be Some after Handshake");
            assert!(conn.ts_handle.is_some(),   "ts_handle must be Some after Handshake");
            assert!(conn.ka_channels.is_none(), "ka_channels must be None after Handshake (consumed)");
            assert!(conn.ts_channels.is_none(), "ts_channels must be None after Handshake (consumed)");
        }

        // Cleanup.
        let mut conn = state.connections.lock().unwrap().remove("default").unwrap();
        if let Some(h) = conn.ka_handle.take() { h.abort(); }
        if let Some(h) = conn.ts_handle.take() { h.abort(); }
        conn.mux.abort();
    }

    // ── Slot-evolution runtime tests ──────────────────────────────────────────

    async fn make_slot_state(start: u64) -> (RunnerState, crate::trace::Tracer, tempfile::NamedTempFile) {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let slot_arc = Arc::new(AtomicU64::new(start));
        let tracer = crate::trace::Tracer::open(tmp.path()).await.unwrap()
            .with_slot_tracker(Some(Arc::clone(&slot_arc)));
        let state = RunnerState { current_slot: Some(slot_arc), ..Default::default() };
        (state, tracer, tmp)
    }

    fn slot_step(kind: crate::scenario::StepKind, params: Value) -> crate::scenario::StepDef {
        crate::scenario::StepDef {
            kind,
            raw_params: params,
            output: None, as_name: None, on_name: None, expect: None,
        }
    }

    #[tokio::test]
    async fn advance_to_slot_stores_new_value() {
        let (mut state, tracer, _tmp) = make_slot_state(100).await;
        let step = slot_step(StepKind::AdvanceToSlot, json!({ "slot": 200 }));
        execute_step(&step, &json!({ "slot": 200 }), "", 42, &mut state, &tracer)
            .await.expect("advance_to_slot 100→200 should succeed");
        let current = state.current_slot.as_ref().unwrap().load(Ordering::Relaxed);
        assert_eq!(current, 200);
    }

    #[tokio::test]
    async fn advance_to_slot_rejects_same_slot() {
        let (mut state, tracer, _tmp) = make_slot_state(100).await;
        let step = slot_step(StepKind::AdvanceToSlot, json!({ "slot": 100 }));
        let err = execute_step(&step, &json!({ "slot": 100 }), "", 42, &mut state, &tracer)
            .await.unwrap_err().to_string();
        assert!(err.contains("not greater than current slot"), "{err}");
        // Slot must be unchanged.
        assert_eq!(state.current_slot.as_ref().unwrap().load(Ordering::Relaxed), 100);
    }

    #[tokio::test]
    async fn advance_to_slot_rejects_rewind() {
        let (mut state, tracer, _tmp) = make_slot_state(200).await;
        let step = slot_step(StepKind::AdvanceToSlot, json!({ "slot": 100 }));
        let err = execute_step(&step, &json!({ "slot": 100 }), "", 42, &mut state, &tracer)
            .await.unwrap_err().to_string();
        assert!(err.contains("not greater than current slot"), "{err}");
    }

    #[tokio::test]
    async fn tick_slots_advances_by_count() {
        let (mut state, tracer, _tmp) = make_slot_state(50).await;
        let step = slot_step(StepKind::TickSlots, json!({ "count": 25 }));
        execute_step(&step, &json!({ "count": 25 }), "", 42, &mut state, &tracer)
            .await.expect("tick_slots 50+25 should succeed");
        assert_eq!(state.current_slot.as_ref().unwrap().load(Ordering::Relaxed), 75);
    }

    #[tokio::test]
    async fn tick_slots_count_one_works() {
        let (mut state, tracer, _tmp) = make_slot_state(0).await;
        let step = slot_step(StepKind::TickSlots, json!({ "count": 1 }));
        execute_step(&step, &json!({ "count": 1 }), "", 42, &mut state, &tracer)
            .await.expect("tick_slots count=1 should succeed");
        assert_eq!(state.current_slot.as_ref().unwrap().load(Ordering::Relaxed), 1);
    }

    // ── PeerExtendsChain runtime tests ───────────────────────────────────────

    async fn run_peer_extends_chain(
        state: &mut RunnerState,
        tracer: &crate::trace::Tracer,
        peer_id: &str,
        slot: u64,
        block_number: u64,
        block_hash_hex: &str,
        header_cbor_hex: &str,
    ) -> anyhow::Result<()> {
        let params_json = json!({
            "peer_id": peer_id,
            "slot": slot,
            "block_number": block_number,
            "block_hash": block_hash_hex,
            "header_cbor": header_cbor_hex,
        });
        let step = slot_step(StepKind::PeerExtendsChain, params_json.clone());
        execute_step(&step, &params_json, "", 42, state, tracer).await
    }

    fn state_with_peer(peer_id: &str) -> RunnerState {
        let mut ps = PeerState::new();
        ps.chain_entries.push(crate::scenario::peer_state::ChainEntry {
            slot: 10, block_hash: vec![0x01; 32], block_number: 1,
            header_cbor: vec![0xaa], variant: 6,
        });
        let mut peers = HashMap::new();
        peers.insert(peer_id.to_string(), ps);
        RunnerState {
            peers: Arc::new(Mutex::new(peers)),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn peer_extends_chain_appends_new_entry() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let tracer = crate::trace::Tracer::open(tmp.path()).await.unwrap();
        let mut state = state_with_peer("p");
        run_peer_extends_chain(&mut state, &tracer, "p", 20, 2, &"aa".repeat(32), "8200")
            .await.expect("valid extension should succeed");
        let guard = state.peers.lock().unwrap();
        let ps = guard.get("p").unwrap();
        assert_eq!(ps.chain_entries.len(), 2);
        assert_eq!(ps.chain_entries[1].slot, 20);
    }

    #[tokio::test]
    async fn peer_extends_chain_rejects_same_slot() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let tracer = crate::trace::Tracer::open(tmp.path()).await.unwrap();
        let mut state = state_with_peer("p");
        let err = run_peer_extends_chain(&mut state, &tracer, "p", 10, 2, &"bb".repeat(32), "8200")
            .await.unwrap_err().to_string();
        assert!(err.contains("not greater than current chain tip slot"), "{err}");
    }

    #[tokio::test]
    async fn peer_extends_chain_rejects_rewind() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let tracer = crate::trace::Tracer::open(tmp.path()).await.unwrap();
        let mut state = state_with_peer("p");
        let err = run_peer_extends_chain(&mut state, &tracer, "p", 5, 2, &"cc".repeat(32), "8200")
            .await.unwrap_err().to_string();
        assert!(err.contains("not greater than current chain tip slot"), "{err}");
    }

    #[tokio::test]
    async fn peer_extends_chain_accepts_any_slot_when_empty() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let tracer = crate::trace::Tracer::open(tmp.path()).await.unwrap();
        let mut peers = HashMap::new();
        peers.insert("p".to_string(), PeerState::new());  // empty chain
        let mut state = RunnerState {
            peers: Arc::new(Mutex::new(peers)),
            ..Default::default()
        };
        run_peer_extends_chain(&mut state, &tracer, "p", 1, 1, &"dd".repeat(32), "8200")
            .await.expect("empty chain should accept any slot");
        assert_eq!(state.peers.lock().unwrap().get("p").unwrap().chain_entries.len(), 1);
    }

    #[test]
    fn peer_state_slot_filter_unit() {
        // Constructed synthetic PeerState — no fixture file dependency.
        // 10 entries at slots 100, 110, 120, ..., 190.
        let mut ps = PeerState::new();
        for (i, slot) in (100u64..=190).step_by(10).enumerate() {
            ps.chain_entries.push(crate::scenario::peer_state::ChainEntry {
                slot,
                block_hash:   vec![slot as u8; 32],
                block_number: i as u64,
                header_cbor:  vec![0x82, 0x82, i as u8, 0x18, slot as u8, 0xf6],
                variant:      6,
            });
        }
        assert_eq!(ps.chain_entries.len(), 10);

        // Visible at slot 145: 100, 110, 120, 130, 140 = 5 entries.
        let visible = ps.filtered_fixture_chain(Some(145));
        assert_eq!(visible.entries.len(), 5, "exactly 5 entries have slot <= 145");
        assert_eq!(visible.entries.last().unwrap().slot, 140);

        // Visible at slot 200: all 10.
        assert_eq!(ps.filtered_fixture_chain(Some(200)).entries.len(), 10);

        // No filter: all 10.
        assert_eq!(ps.filtered_fixture_chain(None).entries.len(), 10);
    }

    #[tokio::test]
    async fn slot_appears_on_trace_events_when_network_declared() {
        let (mut state, tracer, tmp) = make_slot_state(42).await;
        // Emit any step that produces a trace event — tick_slots emits SlotAdvanced.
        let step = slot_step(StepKind::TickSlots, json!({ "count": 8 }));
        execute_step(&step, &json!({ "count": 8 }), "", 1, &mut state, &tracer)
            .await.unwrap();
        let events: Vec<Value> = std::fs::read_to_string(tmp.path()).unwrap()
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        let slot_advanced = events.iter().find(|e| e["kind"] == "slot_advanced").unwrap();
        assert_eq!(slot_advanced["slot"], 50, "SlotAdvanced event should carry to_slot=50");
        assert_eq!(slot_advanced["payload"]["from_slot"], 42);
        assert_eq!(slot_advanced["payload"]["to_slot"], 50);
        assert_eq!(slot_advanced["payload"]["reason"], "tick_slots");
    }
}
