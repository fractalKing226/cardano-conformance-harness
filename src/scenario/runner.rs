use std::future::Future;
use std::pin::Pin;
use std::time::{Duration, Instant};

use anyhow::Context as _;
use pallas_network::miniprotocols::chainsync::{N2NClient, Tip};
use pallas_network::miniprotocols::handshake::n2n::VersionTable;
use pallas_network::miniprotocols::handshake::{RefuseReason, Server as HandshakeServer};
use pallas_network::miniprotocols::{Point, PROTOCOL_N2N_HANDSHAKE, PROTOCOL_N2N_KEEP_ALIVE as PROTOCOL_N2N_KEEP_ALIVE_SERVER};
use pallas_network::multiplexer::{AgentChannel, Bearer, Plexer, RunningPlexer};
use serde_json::{json, Value};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tracing::info;

use crate::miniprotocols::blockfetch::{run_block_fetch, BLOCK_FETCH_PROTOCOL};
use crate::miniprotocols::chainsync::{run_chain_sync, CHAIN_SYNC_PROTOCOL};
use crate::miniprotocols::blockfetch_server::execute_block_fetch_script;
use crate::miniprotocols::chainsync_server::execute_response_script;
use crate::miniprotocols::handshake::handshake_on_channel;
use crate::scenario::block_fixture;
use crate::scenario::response_rules::{generate_for_block_fetch, generate_from_fixture, rule_def_to_script};
use crate::miniprotocols::keepalive::{run_keepalive, run_keepalive_server, KEEP_ALIVE_INTERVAL, KEEP_ALIVE_PROTOCOL};
use crate::miniprotocols::txsubmission::{run_tx_submission, TX_SUBMISSION_PROTOCOL};
use crate::scenario::fixture;
use crate::scenario::vars::{point_to_str, substitute_in_value, VarStore};
use crate::scenario::{BlockFetchPoints, Scenario, StepDef, StepKind, StepParams};
use crate::trace::{Direction, EventKind, TraceEvent, Tracer};

use super::parse_point;

// ── Protocol subscription ─────────────────────────────────────────────────────

/// Returns the set of N2N protocol IDs to subscribe on every connection.
///
/// When Pallas adds `PROTOCOL_N2N_PEER_SHARING` (not present in 0.36.0),
/// add: `if negotiated_version >= PEER_SHARING_MIN_VERSION { ... }`
pub fn subscribed_protocols(_negotiated_version: u64) -> Vec<u16> {
    vec![
        CHAIN_SYNC_PROTOCOL,
        BLOCK_FETCH_PROTOCOL,
        TX_SUBMISSION_PROTOCOL,
        KEEP_ALIVE_PROTOCOL,
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
                    "target_address":   self.scenario.target_address,
                    "network_magic":    self.scenario.network_magic,
                    "steps":            self.scenario.steps.len(),
                    "expected_outcome": self.scenario.expected_outcome,
                }),
            ))
            .await?;

        let mut state = RunnerState {
            capture_fixture:           self.capture_fixture.clone(),
            capture_block_fixture:     self.capture_block_fixture.clone(),
            ..Default::default()
        };
        let mut steps_passed: u64 = 0;
        let mut steps_failed: u64 = 0;

        for (idx, step_def) in self.scenario.steps.iter().enumerate() {
            match run_step(
                step_def,
                idx,
                &self.scenario.target_address,
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
                    emit_completed(
                        &tracer,
                        &self.scenario.name,
                        steps_passed,
                        steps_failed,
                        started_at,
                        "step_error",
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

/// State for one outgoing (client-mode) connection, indexed by name.
struct ClientConnection {
    plexer: RunningPlexer,
    // Protocol channels — subscribed at connect, consumed by protocol steps.
    hs_channel: Option<AgentChannel>,
    cs_channel: Option<AgentChannel>,
    bf_channel: Option<AgentChannel>,
    /// Stored at connect; consumed at handshake to spawn background loops.
    ka_channel: Option<AgentChannel>,
    ts_channel: Option<AgentChannel>,
    // Background workers, spawned after handshake completes.
    ka_handle: Option<JoinHandle<()>>,
    ts_handle: Option<JoinHandle<()>>,
    #[allow(dead_code)]
    negotiated_version: Option<u64>,
    /// Points collected by the most-recent chain_sync on this connection.
    /// Legacy backing for `points: "from_chain_sync"`. Prefer explicit
    /// `output` + `"$varname"` in new multi-connection scenarios.
    last_chain_sync_points: Vec<Point>,
}

/// State for one bound TCP listener, indexed by name.
struct ServerListener {
    listener: TcpListener,
}

/// State for one accepted (server-mode) connection, indexed by name.
struct ServerConnection {
    plexer: RunningPlexer,
    cs_channel: Option<AgentChannel>,
    bf_channel: Option<AgentChannel>,
    /// Idle but must stay alive — see comment in accept_handshake handler.
    ts_channel: Option<AgentChannel>,
    ka_handle: Option<JoinHandle<()>>,
}

// ── Runner state ──────────────────────────────────────────────────────────────

#[derive(Default)]
struct RunnerState {
    /// Outgoing connections, keyed by `as` name (default: `"default"`).
    connections: std::collections::HashMap<String, ClientConnection>,
    /// Bound TCP listeners, keyed by `as` name.
    listeners: std::collections::HashMap<String, ServerListener>,
    /// Accepted server connections, keyed by `as` name.
    server_connections: std::collections::HashMap<String, ServerConnection>,
    /// Flat variable namespace shared across all connections.
    vars: VarStore,
    /// Path to write fixture entries to (from `--capture-fixture` CLI flag).
    capture_fixture: Option<std::path::PathBuf>,
    fixture_anchor_written: bool,
    /// Path for --capture-block-fixture output.
    capture_block_fixture: Option<std::path::PathBuf>,
    block_fixture_anchor_written: bool,
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
        if step.kind == StepKind::Repeat {
            if let serde_json::Value::Object(ref mut map) = resolved {
                map.remove("body");
            }
        }
        let refs_made = substitute_in_value(&mut resolved, &state.vars)
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
                anyhow::bail!(
                    "step {step_idx} ({}) failed assertions",
                    step.kind.as_str()
                )
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
                // Subscribe every protocol channel and spawn the multiplexer.
                // No mini-protocol traffic is sent here — the Cardano N2N spec
                // requires Handshake to complete before any other protocol can be
                // used. Background workers are started in the Handshake arm below.
                let bearer = Bearer::connect_tcp(target_address)
                    .await
                    .with_context(|| format!("failed to connect to {target_address}"))?;
                tracer
                    .emit(TraceEvent::new(
                        EventKind::ConnectionOpened,
                        Direction::Internal,
                        json!({ "addr": target_address }),
                    ).with_connection(&conn_name))
                    .await?;
                let mut plexer = Plexer::new(bearer);
                let hs_ch = plexer.subscribe_client(PROTOCOL_N2N_HANDSHAKE);
                let cs_ch = plexer.subscribe_client(CHAIN_SYNC_PROTOCOL);
                let bf_ch = plexer.subscribe_client(BLOCK_FETCH_PROTOCOL);
                let ts_ch = plexer.subscribe_client(TX_SUBMISSION_PROTOCOL);
                let ka_ch = plexer.subscribe_client(KEEP_ALIVE_PROTOCOL);
                let plexer = plexer.spawn();
                state.connections.insert(conn_name, ClientConnection {
                    plexer,
                    hs_channel: Some(hs_ch),
                    cs_channel: Some(cs_ch),
                    bf_channel: Some(bf_ch),
                    ts_channel: Some(ts_ch),
                    ka_channel: Some(ka_ch),
                    ka_handle: None,
                    ts_handle: None,
                    negotiated_version: None,
                    last_chain_sync_points: Vec::new(),
                });
                Ok(())
            }

            StepKind::Handshake => {
                let conn_name = step.on_name.as_deref().unwrap_or("default");
                // Phase 2 of the two-phase connection lifecycle: version negotiation
                // and worker activation. Only after handshake_on_channel returns
                // successfully do we know the negotiated version and that the node
                // will accept traffic on other channels. Background tasks are spawned
                // here, never in Connect, so they cannot send messages prematurely.
                let conn = state.connections.get_mut(conn_name)
                    .ok_or_else(|| anyhow::anyhow!("handshake: connection \"{conn_name}\" not found"))?;
                let channel = conn.hs_channel.take()
                    .ok_or_else(|| anyhow::anyhow!("handshake: no channel (already used?)"))?;
                let version = handshake_on_channel(channel, network_magic, tracer).await?;
                conn.negotiated_version = Some(version);

                let mut spawned_protocols: Vec<&str> = Vec::new();
                if let Some(ka_channel) = conn.ka_channel.take() {
                    let ka_client = pallas_network::miniprotocols::keepalive::Client::new(ka_channel);
                    conn.ka_handle = Some(tokio::spawn(run_keepalive(ka_client, tracer.clone(), KEEP_ALIVE_INTERVAL)));
                    spawned_protocols.push("keep-alive");
                }
                if let Some(ts_channel) = conn.ts_channel.take() {
                    conn.ts_handle = Some(tokio::spawn(run_tx_submission(ts_channel, tracer.clone())));
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
                    state.vars.insert(output_var.clone(), tip_val.clone());
                    tracer.emit(TraceEvent::new(EventKind::VariableSet, Direction::Internal,
                        json!({ "name": output_var, "shape": "tip{point,block_number}" }))).await?;
                    info!(var = output_var.as_str(), "Stored tip");
                }
                Ok(())
            }

            StepKind::ChainSync => {
                let conn_name = step.on_name.as_deref().unwrap_or("default").to_string();
                let conn = state.connections.get_mut(&conn_name)
                    .ok_or_else(|| anyhow::anyhow!("chain_sync: connection \"{conn_name}\" not found"))?;
                let channel = conn.cs_channel.take()
                    .ok_or_else(|| anyhow::anyhow!("chain_sync: no channel (already used?)"))?;

                let origin = vec!["origin".to_string()];
                let raw_pts = params.intersection_points.as_deref().unwrap_or(origin.as_slice());
                let intersection_points = raw_pts.iter().map(|s| parse_point(s))
                    .collect::<anyhow::Result<Vec<Point>>>()?;
                let count = params.count.unwrap_or(10);
                let await_secs = params.await_timeout_secs.unwrap_or(30);

                let summary = run_chain_sync(channel, intersection_points, count,
                    Duration::from_secs(await_secs), tracer).await?;

                let n = summary.collected_points.len();
                conn.last_chain_sync_points = summary.collected_points.clone();

                if let Some(output_var) = &step.output {
                    let pts_json: Vec<Value> = summary.collected_points.iter()
                        .map(|p| Value::String(point_to_str(p))).collect();
                    state.vars.insert(output_var.clone(), Value::Array(pts_json));
                    tracer.emit(TraceEvent::new(EventKind::VariableSet, Direction::Internal,
                        json!({ "name": output_var, "shape": format!("array[{n}]") }))).await?;
                    info!(var = output_var.as_str(), points = n, "Stored chain-sync points");
                }

                if let Some(ref fixture_path) = state.capture_fixture {
                    if !summary.captured_headers.is_empty() {
                        if !state.fixture_anchor_written {
                            fixture::write_anchor(fixture_path, &Point::Origin)?;
                            state.fixture_anchor_written = true;
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
                let conn = state.connections.get_mut(&conn_name)
                    .ok_or_else(|| anyhow::anyhow!("block_fetch: connection \"{conn_name}\" not found"))?;
                let channel = conn.bf_channel.take()
                    .ok_or_else(|| anyhow::anyhow!("block_fetch: no channel (already used?)"))?;

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
                let summary = run_block_fetch(channel, points.clone(), batch_size, tracer).await?;

                if let Some(ref bfp) = state.capture_block_fixture {
                    if !summary.captured_blocks.is_empty() {
                        if !state.block_fixture_anchor_written {
                            block_fixture::write_anchor(bfp)?;
                            state.block_fixture_anchor_written = true;
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
                let mut conn = state.connections.remove(&conn_name)
                    .ok_or_else(|| anyhow::anyhow!("disconnect: connection \"{conn_name}\" not found"))?;
                conn.ka_channel.take();
                conn.ts_channel.take();
                if let Some(h) = conn.ka_handle.take() { h.abort(); }
                if let Some(h) = conn.ts_handle.take() { h.abort(); }
                conn.plexer.abort().await;
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
                state.listeners.insert(listener_name, ServerListener { listener });
                Ok(())
            }

            StepKind::AcceptHandshake => {
                let listener_name = step.on_name.as_deref().unwrap_or("default");
                let conn_name = step.as_name.as_deref().unwrap_or("default").to_string();

                let sl = state.listeners.get(&listener_name.to_string())
                    .ok_or_else(|| anyhow::anyhow!("accept_handshake: listener \"{listener_name}\" not found"))?;

                let (bearer, peer_addr) = Bearer::accept_tcp(&sl.listener)
                    .await
                    .context("accept_handshake: Bearer::accept_tcp failed")?;

                tracer
                    .emit(TraceEvent::new(EventKind::ServerBearerAccepted, Direction::Internal,
                        json!({ "peer_address": peer_addr.to_string() }))
                        .with_connection(&conn_name))
                    .await?;

                // Phase 1: subscribe all server-side channels, spawn plexer.
                // We subscribe manually (not via PeerServer) so we retain the raw
                // AgentChannel for chain-sync, which is needed for execute_response_script.
                let mut plexer = Plexer::new(bearer);
                let hs_channel = plexer.subscribe_server(PROTOCOL_N2N_HANDSHAKE);
                let cs_channel = plexer.subscribe_server(CHAIN_SYNC_PROTOCOL);
                let ka_channel = plexer.subscribe_server(PROTOCOL_N2N_KEEP_ALIVE_SERVER);
                // Subscribe TX_SUBMISSION so the demuxer can route frames from the peer.
                // Must be stored in state — if dropped, the demuxer task exits, killing all channels.
                let ts_server_channel = plexer.subscribe_server(TX_SUBMISSION_PROTOCOL);
                let bf_server_channel = plexer.subscribe_server(BLOCK_FETCH_PROTOCOL);
                let server_plexer = plexer.spawn();

                // Phase 2: complete the handshake as responder.
                let mut hs = HandshakeServer::<pallas_network::miniprotocols::handshake::n2n::VersionData>::new(hs_channel);
                let client_table = hs.receive_proposed_versions().await
                    .context("accept_handshake: receive_proposed_versions failed")?;
                let mut proposed_versions: Vec<u64> = client_table.values.keys().cloned().collect();
                proposed_versions.sort();

                tracer
                    .emit(TraceEvent::new(EventKind::HandshakeVersionProposed, Direction::Received,
                        json!({ "versions": proposed_versions, "magic": network_magic }))
                        .with_connection(&conn_name))
                    .await?;

                let server_supported: Vec<u64> = (7u64..=14).collect();
                let mut client_pairs: Vec<(u64, _)> = client_table.values.into_iter().collect();
                client_pairs.sort_by(|a, b| b.0.cmp(&a.0));
                let (version, client_data): (u64, pallas_network::miniprotocols::handshake::n2n::VersionData) =
                    match client_pairs.into_iter().find(|(v, _)| server_supported.contains(v)) {
                        Some(pair) => pair,
                        None => {
                            let _ = hs.refuse(RefuseReason::VersionMismatch(server_supported)).await;
                            anyhow::bail!("accept_handshake: no common version (proposed: {proposed_versions:?})");
                        }
                    };

                hs.accept_version(version, client_data.clone()).await
                    .context("accept_handshake: accept_version failed")?;

                tracer
                    .emit(TraceEvent::new(EventKind::HandshakeVersionAccepted, Direction::Sent,
                        json!({ "version": version, "peer_data": format!("{client_data:?}") }))
                        .with_connection(&conn_name))
                    .await?;
                tracer
                    .emit(TraceEvent::new(EventKind::ServerHandshakeAccepted, Direction::Internal,
                        json!({ "peer_address": peer_addr.to_string(), "negotiated_version": version }))
                        .with_connection(&conn_name))
                    .await?;

                let ka_server = pallas_network::miniprotocols::keepalive::Server::new(ka_channel);
                let ka_handle = tokio::spawn(run_keepalive_server(ka_server));
                tracer
                    .emit(TraceEvent::new(EventKind::ProtocolWorkersStarted, Direction::Internal,
                        json!({ "protocols": ["keep-alive"] }))
                        .with_connection(&conn_name))
                    .await?;

                info!(version, %peer_addr, conn = conn_name, "Handshake accepted as server");
                state.server_connections.insert(conn_name, ServerConnection {
                    plexer: server_plexer,
                    cs_channel: Some(cs_channel),
                    bf_channel: Some(bf_server_channel),
                    ts_channel: Some(ts_server_channel),
                    ka_handle: Some(ka_handle),
                });
                Ok(())
            }

            StepKind::ServeChainSync => {
                let conn_name = step.on_name.as_deref().unwrap_or("default").to_string();
                let sc = state.server_connections.get_mut(&conn_name)
                    .ok_or_else(|| anyhow::anyhow!("serve_chain_sync: server connection \"{conn_name}\" not found"))?;
                let mut cs_channel = sc.cs_channel.take()
                    .ok_or_else(|| anyhow::anyhow!("serve_chain_sync: no chain-sync channel"))?;

                let await_secs = params.await_at_tip_secs.unwrap_or(30).min(300);

                // Load fixture if fixture_path is set (used for auto-generation OR
                // as a header source for header_from_fixture in explicit responses).
                let fixture_opt = if let Some(path) = &params.fixture_path {
                    let chain = fixture::load(std::path::Path::new(path.as_str()))
                        .with_context(|| format!("serve_chain_sync: loading fixture \"{path}\""))?;
                    Some(chain)
                } else {
                    None
                };

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
                        // Auto-generate from fixture (fixture_path must be set;
                        // validation guarantees at least one of the two is present).
                        let chain = fixture_opt.as_ref().ok_or_else(|| {
                            anyhow::anyhow!("serve_chain_sync: fixture_path is required when responses is not set")
                        })?;
                        generate_from_fixture(chain, await_secs)
                    }
                };

                let summary = execute_response_script(
                    &mut cs_channel,
                    &script,
                    fixture_opt.as_ref(),
                    tracer,
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
                let sc = state.server_connections.get_mut(&conn_name)
                    .ok_or_else(|| anyhow::anyhow!("serve_block_fetch: server connection \"{conn_name}\" not found"))?;
                let mut bf_channel = sc.bf_channel.take()
                    .ok_or_else(|| anyhow::anyhow!("serve_block_fetch: no block-fetch channel"))?;

                // Load BF fixture if provided.
                let bf_fixture_opt = if let Some(path) = &params.block_fetch_fixture_path {
                    let chain = block_fixture::load(std::path::Path::new(path.as_str()))
                        .with_context(|| format!("serve_block_fetch: loading fixture \"{path}\""))?;
                    Some(chain)
                } else {
                    None
                };

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
                    &mut bf_channel,
                    &script,
                    bf_fixture_opt.as_ref(),
                    tracer,
                )
                .await?;

                info!(
                    blocks_served = summary.blocks_served,
                    duration_ms   = summary.duration_ms,
                    "serve_block_fetch step complete"
                );
                Ok(())
            }

            StepKind::CloseListener => {
                let listener_name = step.on_name.as_deref().unwrap_or("default").to_string();
                state.listeners.remove(&listener_name);
                // Server connections created via this listener remain open until
                // explicitly disconnected or until cleanup runs.
                tracer
                    .emit(TraceEvent::new(EventKind::ServerListenStopped, Direction::Internal, json!({})))
                    .await?;
                info!(listener = listener_name, "Listener stopped");
                Ok(())
            }
        }
    })
}

// ── query_tip implementation ──────────────────────────────────────────────────

/// Opens a temporary TCP connection to `target_address`, performs a handshake,
/// then does a minimal Chain-Sync round-trip to obtain the current chain tip.
/// All wire events are logged to the shared tracer.
async fn fetch_tip(target_address: &str, network_magic: u64, tracer: &Tracer) -> anyhow::Result<Tip> {
    let bearer = Bearer::connect_tcp(target_address)
        .await
        .with_context(|| format!("query_tip: failed to connect to {target_address}"))?;

    let mut plexer = Plexer::new(bearer);
    let hs_channel = plexer.subscribe_client(PROTOCOL_N2N_HANDSHAKE);
    let cs_channel = plexer.subscribe_client(CHAIN_SYNC_PROTOCOL);
    let plexer_handle = plexer.spawn();

    handshake_on_channel(hs_channel, network_magic, tracer)
        .await
        .context("query_tip: handshake failed")?;

    let mut cs = N2NClient::new(cs_channel);

    // FindIntersect at origin — the response includes the current tip directly.
    let (_, tip) = cs
        .find_intersect(vec![Point::Origin])
        .await
        .context("query_tip: find_intersect failed")?;

    cs.send_done().await.ok(); // best-effort clean close
    plexer_handle.abort().await;

    Ok(tip)
}

// ── Shared utilities ──────────────────────────────────────────────────────────

/// Close all open connections and listeners. Called on error/assertion-failure
/// paths before returning from `run`.
async fn cleanup(state: &mut RunnerState, tracer: &Tracer) {
    // Close all accepted server connections.
    for (name, mut sc) in state.server_connections.drain() {
        if let Some(h) = sc.ka_handle.take() { h.abort(); }
        sc.plexer.abort().await;
        let _ = tracer.emit(TraceEvent::new(EventKind::ConnectionClosed, Direction::Internal,
            json!({ "reason": "scenario_aborted" })).with_connection(&name)).await;
    }
    // Drop all listeners.
    state.listeners.clear();
    // Close all outgoing connections.
    for (name, mut cc) in state.connections.drain() {
        cc.ka_channel.take();
        cc.ts_channel.take();
        if let Some(h) = cc.ka_handle.take() { h.abort(); }
        if let Some(h) = cc.ts_handle.take() { h.abort(); }
        cc.plexer.abort().await;
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
        use pallas_network::miniprotocols::{
            PROTOCOL_N2N_KEEP_ALIVE, PROTOCOL_N2N_TX_SUBMISSION,
        };
        for &version in &[7u64, 11, 13, 14] {
            let ps = subscribed_protocols(version);
            assert!(ps.contains(&CHAIN_SYNC_PROTOCOL),        "v{version}: missing chain-sync");
            assert!(ps.contains(&BLOCK_FETCH_PROTOCOL),       "v{version}: missing block-fetch");
            assert!(ps.contains(&PROTOCOL_N2N_TX_SUBMISSION), "v{version}: missing tx-submission");
            assert!(ps.contains(&PROTOCOL_N2N_KEEP_ALIVE),    "v{version}: missing keep-alive");
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
        assert!(state.connections.is_empty(), "no connections before connect");
        assert!(state.listeners.is_empty(),   "no listeners before listen");
        assert!(state.server_connections.is_empty(), "no server connections before accept_handshake");
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

        let conn = state.connections.get("default").expect("default connection");
        assert!(conn.ka_handle.is_none(),  "ka_handle must be None after Connect");
        assert!(conn.ts_handle.is_none(),  "ts_handle must be None after Connect");
        assert!(conn.ka_channel.is_some(), "ka_channel must be Some after Connect");
        assert!(conn.ts_channel.is_some(), "ts_channel must be Some after Connect");

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

        let conn = state.connections.get("default").expect("default connection");
        assert!(conn.ka_handle.is_some(),  "ka_handle must be Some after Handshake");
        assert!(conn.ts_handle.is_some(),  "ts_handle must be Some after Handshake");
        assert!(conn.ka_channel.is_none(), "ka_channel must be None after Handshake (consumed)");
        assert!(conn.ts_channel.is_none(), "ts_channel must be None after Handshake (consumed)");

        // Cleanup.
        let mut conn = state.connections.remove("default").unwrap();
        if let Some(h) = conn.ka_handle.take() { h.abort(); }
        if let Some(h) = conn.ts_handle.take() { h.abort(); }
        conn.plexer.abort().await;
    }
}
