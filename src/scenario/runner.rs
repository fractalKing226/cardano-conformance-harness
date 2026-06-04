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
use crate::miniprotocols::chainsync_server::execute_response_script;
use crate::miniprotocols::handshake::handshake_on_channel;
use crate::scenario::response_rules::{generate_from_fixture, rule_def_to_script};
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
    /// If set, every RollForward header is appended to this file as a fixture entry.
    capture_fixture: Option<std::path::PathBuf>,
}

impl ScenarioRunner {
    pub fn new(scenario: Scenario) -> Self {
        Self { scenario, capture_fixture: None }
    }

    pub fn with_capture_fixture(mut self, path: Option<std::path::PathBuf>) -> Self {
        self.capture_fixture = path;
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
            capture_fixture: self.capture_fixture.clone(),
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

// ── Runner state ──────────────────────────────────────────────────────────────

#[derive(Default)]
struct RunnerState {
    plexer_handle: Option<RunningPlexer>,
    ka_handle: Option<JoinHandle<()>>,
    ts_handle: Option<JoinHandle<()>>,
    hs_channel: Option<AgentChannel>,
    cs_channel: Option<AgentChannel>,
    bf_channel: Option<AgentChannel>,
    /// Stored during connect; background task is spawned after handshake.
    ka_channel: Option<AgentChannel>,
    /// Stored during connect; background task is spawned after handshake.
    ts_channel: Option<AgentChannel>,
    #[allow(dead_code)]
    negotiated_version: Option<u64>,
    last_chain_sync_points: Vec<Point>,
    /// Flat variable namespace populated by steps with `output` fields.
    vars: VarStore,

    // ── Server-side state ─────────────────────────────────────────────────────
    /// TCP listener bound by the `listen` step.
    listener: Option<TcpListener>,
    /// Plexer for the accepted server connection.
    server_plexer_handle: Option<RunningPlexer>,
    /// Raw Chain-Sync channel for the accepted connection (consumed by serve_chain_sync).
    server_cs_channel: Option<AgentChannel>,
    /// TX_SUBMISSION channel — idle but must stay alive so the demuxer doesn't
    /// exit when the client sends MsgInit on that channel.
    server_ts_channel: Option<AgentChannel>,
    /// Server-side keep-alive background task handle.
    server_ka_handle: Option<JoinHandle<()>>,
    /// Path to write fixture entries to (from `--capture-fixture` CLI flag).
    capture_fixture: Option<std::path::PathBuf>,
    /// True once the anchor line has been written to the fixture file this run.
    /// Avoids re-writing the anchor if multiple chain_sync steps capture to the same file.
    fixture_anchor_written: bool,
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
                    ))
                    .await?;
                let mut plexer = Plexer::new(bearer);
                state.hs_channel = Some(plexer.subscribe_client(PROTOCOL_N2N_HANDSHAKE));
                state.cs_channel = Some(plexer.subscribe_client(CHAIN_SYNC_PROTOCOL));
                state.bf_channel = Some(plexer.subscribe_client(BLOCK_FETCH_PROTOCOL));
                state.ts_channel = Some(plexer.subscribe_client(TX_SUBMISSION_PROTOCOL));
                state.ka_channel = Some(plexer.subscribe_client(KEEP_ALIVE_PROTOCOL));
                state.plexer_handle = Some(plexer.spawn());
                Ok(())
            }

            StepKind::Handshake => {
                // Phase 2 of the two-phase connection lifecycle: version negotiation
                // and worker activation. Only after handshake_on_channel returns
                // successfully do we know the negotiated version and that the node
                // will accept traffic on other channels. Background tasks are spawned
                // here, never in Connect, so they cannot send messages prematurely.
                let channel = state
                    .hs_channel
                    .take()
                    .ok_or_else(|| anyhow::anyhow!("handshake step: no channel (missing connect?)"))?;
                let version = handshake_on_channel(channel, network_magic, tracer).await?;
                state.negotiated_version = Some(version);

                let mut spawned_protocols: Vec<&str> = Vec::new();

                if let Some(ka_channel) = state.ka_channel.take() {
                    let ka_client = pallas_network::miniprotocols::keepalive::Client::new(ka_channel);
                    state.ka_handle = Some(tokio::spawn(run_keepalive(
                        ka_client,
                        tracer.clone(),
                        KEEP_ALIVE_INTERVAL,
                    )));
                    spawned_protocols.push("keep-alive");
                }
                if let Some(ts_channel) = state.ts_channel.take() {
                    state.ts_handle = Some(tokio::spawn(run_tx_submission(ts_channel, tracer.clone())));
                    spawned_protocols.push("tx-submission");
                }

                tracer
                    .emit(TraceEvent::new(
                        EventKind::ProtocolWorkersStarted,
                        Direction::Internal,
                        json!({ "protocols": spawned_protocols }),
                    ))
                    .await?;

                info!(version, protocols = ?spawned_protocols, "Handshake complete, workers started");
                Ok(())
            }

            StepKind::QueryTip => {
                // This opens a separate TCP connection because the main connection's
                // Chain-Sync channel is currently consumed by chain_sync steps. A
                // future refactor to support sequential sessions on a single channel
                // would let query_tip share the main connection. The current design
                // is acceptable because handshake overhead is small and the second-
                // connection model also generalizes to future multi-peer scenarios.
                tracer
                    .emit(TraceEvent::new(
                        EventKind::QueryTipStarted,
                        Direction::Internal,
                        json!({}),
                    ))
                    .await?;

                let tip = fetch_tip(target_address, network_magic, tracer).await?;
                let Tip(tip_point, block_number) = &tip;
                let tip_val = json!({
                    "point": point_to_str(tip_point),
                    "block_number": block_number,
                });

                tracer
                    .emit(TraceEvent::new(
                        EventKind::QueryTipCompleted,
                        Direction::Internal,
                        json!({ "tip": tip_val }),
                    ))
                    .await?;

                if let Some(output_var) = &step.output {
                    state.vars.insert(output_var.clone(), tip_val.clone());
                    tracer
                        .emit(TraceEvent::new(
                            EventKind::VariableSet,
                            Direction::Internal,
                            json!({ "name": output_var, "shape": "tip{point,block_number}" }),
                        ))
                        .await?;
                    info!(var = output_var.as_str(), "Stored tip");
                }
                Ok(())
            }

            StepKind::ChainSync => {
                let channel = state
                    .cs_channel
                    .take()
                    .ok_or_else(|| anyhow::anyhow!("chain_sync step: no channel"))?;

                let origin = vec!["origin".to_string()];
                let raw_pts = params.intersection_points.as_deref().unwrap_or(origin.as_slice());
                let intersection_points = raw_pts
                    .iter()
                    .map(|s| parse_point(s))
                    .collect::<anyhow::Result<Vec<Point>>>()?;

                let count = params.count.unwrap_or(10);
                let await_secs = params.await_timeout_secs.unwrap_or(30);

                let summary = run_chain_sync(
                    channel,
                    intersection_points,
                    count,
                    Duration::from_secs(await_secs),
                    tracer,
                )
                .await?;

                let n = summary.collected_points.len();
                state.last_chain_sync_points = summary.collected_points.clone();

                if let Some(output_var) = &step.output {
                    let pts_json: Vec<Value> = summary
                        .collected_points
                        .iter()
                        .map(|p| Value::String(point_to_str(p)))
                        .collect();
                    state.vars.insert(output_var.clone(), Value::Array(pts_json));
                    tracer
                        .emit(TraceEvent::new(
                            EventKind::VariableSet,
                            Direction::Internal,
                            json!({ "name": output_var, "shape": format!("array[{n}]") }),
                        ))
                        .await?;
                    info!(var = output_var.as_str(), points = n, "Stored chain-sync points");
                }

                // Write fixture if --capture-fixture was set.
                if let Some(ref fixture_path) = state.capture_fixture {
                    if !summary.captured_headers.is_empty() {
                        // Write the anchor exactly once per run, truncating any
                        // existing file. Checked by flag rather than file existence
                        // so a pre-existing stale file doesn't suppress the anchor.
                        if !state.fixture_anchor_written {
                            fixture::write_anchor(fixture_path, &Point::Origin)?;
                            state.fixture_anchor_written = true;
                        }
                        for h in &summary.captured_headers {
                            let entry = fixture::FixtureEntry {
                                slot: h.slot,
                                block_hash: fixture::encode_hex(&h.block_hash),
                                block_number: h.block_number,
                                cbor_hex: fixture::encode_hex(&h.cbor),
                                variant: h.variant,
                            };
                            fixture::append_entry(fixture_path, &entry)?;
                        }
                        info!(
                            headers = summary.captured_headers.len(),
                            path = %fixture_path.display(),
                            "Wrote fixture entries"
                        );
                    }
                }

                info!(headers = summary.headers_received, points = n, "Chain-sync step complete");
                Ok(())
            }

            StepKind::BlockFetch => {
                let channel = state
                    .bf_channel
                    .take()
                    .ok_or_else(|| anyhow::anyhow!("block_fetch step: no channel"))?;

                let points: Vec<Point> =
                    match params.points.as_ref().unwrap_or(&BlockFetchPoints::FromChainSync) {
                        BlockFetchPoints::FromChainSync => {
                            anyhow::ensure!(
                                !state.last_chain_sync_points.is_empty(),
                                "block_fetch: from_chain_sync but no chain_sync points available"
                            );
                            std::mem::take(&mut state.last_chain_sync_points)
                        }
                        BlockFetchPoints::Explicit(strings) => strings
                            .iter()
                            .map(|s| parse_point(s))
                            .collect::<anyhow::Result<Vec<Point>>>()?,
                    };

                let batch_size = params.batch_size.unwrap_or(1);
                let summary = run_block_fetch(channel, points, batch_size, tracer).await?;
                info!(
                    blocks = summary.blocks_received,
                    bytes = summary.total_bytes,
                    "Block-fetch step complete"
                );
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
                // Abort background loops before the plexer — they use the channels.
                state.ka_channel.take();
                state.ts_channel.take();
                if let Some(handle) = state.ka_handle.take() { handle.abort(); }
                if let Some(handle) = state.ts_handle.take() { handle.abort(); }
                if let Some(handle) = state.plexer_handle.take() {
                    handle.abort().await;
                }
                tracer
                    .emit(TraceEvent::new(
                        EventKind::ConnectionClosed,
                        Direction::Internal,
                        json!({}),
                    ))
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
                let addr = params.bind_address.as_deref().unwrap_or("0.0.0.0:3001");
                let listener = TcpListener::bind(addr)
                    .await
                    .with_context(|| format!("listen: failed to bind {addr}"))?;
                tracer
                    .emit(TraceEvent::new(
                        EventKind::ServerListenStarted,
                        Direction::Internal,
                        json!({ "bind_address": addr }),
                    ))
                    .await?;
                info!(%addr, "Listener started");
                state.listener = Some(listener);
                Ok(())
            }

            StepKind::AcceptHandshake => {
                let listener = state
                    .listener
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("accept_handshake: no listener (missing listen step?)"))?;

                let (bearer, peer_addr) = Bearer::accept_tcp(listener)
                    .await
                    .context("accept_handshake: Bearer::accept_tcp failed")?;

                // Emit ServerBearerAccepted — TCP landed, multiplexer ready,
                // awaiting handshake initiation from the peer.
                tracer
                    .emit(TraceEvent::new(
                        EventKind::ServerBearerAccepted,
                        Direction::Internal,
                        json!({ "peer_address": peer_addr.to_string() }),
                    ))
                    .await?;

                // Phase 1: subscribe all server-side channels, spawn plexer.
                // We subscribe manually (not via PeerServer) so we retain the raw
                // AgentChannel for chain-sync, which is needed for execute_response_script.
                let mut plexer = Plexer::new(bearer);
                let hs_channel = plexer.subscribe_server(PROTOCOL_N2N_HANDSHAKE);
                let cs_channel = plexer.subscribe_server(CHAIN_SYNC_PROTOCOL);
                let ka_channel = plexer.subscribe_server(PROTOCOL_N2N_KEEP_ALIVE_SERVER);
                // Subscribe TX_SUBMISSION so the demuxer can route frames from the peer.
                // Must be stored in state — if the AgentChannel is dropped, the demuxer's
                // send() fails and the entire demuxer task exits, killing all channels.
                let ts_server_channel = plexer.subscribe_server(TX_SUBMISSION_PROTOCOL);
                let server_plexer = plexer.spawn();

                // Phase 2: complete the handshake as responder.
                let mut hs = HandshakeServer::<pallas_network::miniprotocols::handshake::n2n::VersionData>::new(hs_channel);
                let client_table = hs
                    .receive_proposed_versions()
                    .await
                    .context("accept_handshake: receive_proposed_versions failed")?;

                let mut proposed_versions: Vec<u64> =
                    client_table.values.keys().cloned().collect();
                proposed_versions.sort();

                // direction: received — the version proposal came from the peer.
                tracer
                    .emit(TraceEvent::new(
                        EventKind::HandshakeVersionProposed,
                        Direction::Received,
                        json!({ "versions": proposed_versions, "magic": network_magic }),
                    ))
                    .await?;

                let server_supported: Vec<u64> = (7u64..=14).collect();
                let mut client_pairs: Vec<(u64, _)> =
                    client_table.values.into_iter().collect();
                client_pairs.sort_by(|a, b| b.0.cmp(&a.0));

                let (version, client_data): (u64, pallas_network::miniprotocols::handshake::n2n::VersionData)
                    = match client_pairs
                    .into_iter()
                    .find(|(v, _)| server_supported.contains(v))
                {
                    Some(pair) => pair,
                    None => {
                        let _ = hs.refuse(RefuseReason::VersionMismatch(server_supported)).await;
                        anyhow::bail!(
                            "accept_handshake: no common version (proposed: {proposed_versions:?})"
                        );
                    }
                };

                hs.accept_version(version, client_data.clone())
                    .await
                    .context("accept_handshake: accept_version failed")?;

                // direction: sent — the version acceptance goes from harness to peer.
                tracer
                    .emit(TraceEvent::new(
                        EventKind::HandshakeVersionAccepted,
                        Direction::Sent,
                        json!({ "version": version, "peer_data": format!("{client_data:?}") }),
                    ))
                    .await?;

                tracer
                    .emit(TraceEvent::new(
                        EventKind::ServerHandshakeAccepted,
                        Direction::Internal,
                        json!({ "peer_address": peer_addr.to_string(), "negotiated_version": version }),
                    ))
                    .await?;

                // Spawn server-side keep-alive (respond to pings from the peer).
                let ka_server = pallas_network::miniprotocols::keepalive::Server::new(ka_channel);
                let ka_handle = tokio::spawn(run_keepalive_server(ka_server));

                tracer
                    .emit(TraceEvent::new(
                        EventKind::ProtocolWorkersStarted,
                        Direction::Internal,
                        json!({ "protocols": ["keep-alive"] }),
                    ))
                    .await?;

                info!(version, %peer_addr, "Handshake accepted as server");
                state.server_plexer_handle = Some(server_plexer);
                state.server_cs_channel = Some(cs_channel);
                state.server_ts_channel = Some(ts_server_channel);
                state.server_ka_handle = Some(ka_handle);
                Ok(())
            }

            StepKind::ServeChainSync => {
                let mut cs_channel = state
                    .server_cs_channel
                    .take()
                    .ok_or_else(|| anyhow::anyhow!("serve_chain_sync: no chain-sync channel (missing accept_handshake?)"))?;

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
                                rule_def_to_script(d, fixture_opt.as_ref()).with_context(|| {
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

            StepKind::CloseListener => {
                if let Some(h) = state.server_ka_handle.take() { h.abort(); }
                if let Some(h) = state.server_plexer_handle.take() { h.abort().await; }
                state.server_cs_channel.take();
                state.server_ts_channel.take();
                state.listener.take();
                tracer
                    .emit(TraceEvent::new(
                        EventKind::ServerListenStopped,
                        Direction::Internal,
                        json!({}),
                    ))
                    .await?;
                info!("Listener stopped");
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

/// Abort the keep-alive loop and plexer, emit ConnectionClosed if still
/// connected. Called on error/assertion-failure paths before returning.
async fn cleanup(state: &mut RunnerState, tracer: &Tracer) {
    // Server-side cleanup.
    if let Some(h) = state.server_ka_handle.take() { h.abort(); }
    if let Some(h) = state.server_plexer_handle.take() { h.abort().await; }
    state.server_cs_channel.take();
    state.server_ts_channel.take();
    state.listener.take();

    // Client-side cleanup.
    state.ka_channel.take();
    state.ts_channel.take();
    if let Some(handle) = state.ka_handle.take() { handle.abort(); }
    if let Some(handle) = state.ts_handle.take() { handle.abort(); }
    if let Some(handle) = state.plexer_handle.take() {
        handle.abort().await;
        let _ = tracer
            .emit(TraceEvent::new(
                EventKind::ConnectionClosed,
                Direction::Internal,
                json!({ "reason": "scenario_aborted" }),
            ))
            .await;
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
        let state = RunnerState::default();
        assert!(state.ka_handle.is_none(),  "ka_handle must be None — workers spawn in Handshake, not Connect");
        assert!(state.ts_handle.is_none(),  "ts_handle must be None — workers spawn in Handshake, not Connect");
        assert!(state.ka_channel.is_none(), "ka_channel must be None before Connect runs");
        assert!(state.ts_channel.is_none(), "ts_channel must be None before Connect runs");
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
            expect: None,
        };
        execute_step(&connect, &json!({}), "localhost:3001", crate::DEVNET_MAGIC, &mut state, &tracer)
            .await
            .expect("connect should succeed");

        assert!(state.ka_handle.is_none(),  "ka_handle must be None after Connect");
        assert!(state.ts_handle.is_none(),  "ts_handle must be None after Connect");
        assert!(state.ka_channel.is_some(), "ka_channel must be Some after Connect");
        assert!(state.ts_channel.is_some(), "ts_channel must be Some after Connect");

        let handshake = StepDef {
            kind: StepKind::Handshake,
            raw_params: json!({}),
            output: None,
            expect: None,
        };
        execute_step(&handshake, &json!({}), "localhost:3001", crate::DEVNET_MAGIC, &mut state, &tracer)
            .await
            .expect("handshake should succeed");

        assert!(state.ka_handle.is_some(),  "ka_handle must be Some after Handshake");
        assert!(state.ts_handle.is_some(),  "ts_handle must be Some after Handshake");
        assert!(state.ka_channel.is_none(), "ka_channel must be None after Handshake (consumed)");
        assert!(state.ts_channel.is_none(), "ts_channel must be None after Handshake (consumed)");

        if let Some(h) = state.ka_handle.take() { h.abort(); }
        if let Some(h) = state.ts_handle.take() { h.abort(); }
        if let Some(h) = state.plexer_handle.take() { h.abort().await; }
    }
}
