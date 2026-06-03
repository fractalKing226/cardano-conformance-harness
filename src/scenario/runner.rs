use std::time::{Duration, Instant};

use anyhow::Context as _;
use pallas_network::miniprotocols::{Point, PROTOCOL_N2N_HANDSHAKE};

use crate::miniprotocols::keepalive::{run_keepalive, KEEP_ALIVE_INTERVAL, KEEP_ALIVE_PROTOCOL};
use crate::miniprotocols::txsubmission::{run_tx_submission, TX_SUBMISSION_PROTOCOL};
use pallas_network::multiplexer::{AgentChannel, Bearer, Plexer, RunningPlexer};
use serde_json::{json, Value};
use tokio::task::JoinHandle;
use tracing::info;

use crate::miniprotocols::blockfetch::{run_block_fetch, BLOCK_FETCH_PROTOCOL};
use crate::miniprotocols::chainsync::{run_chain_sync, CHAIN_SYNC_PROTOCOL};
use crate::miniprotocols::handshake::handshake_on_channel;
use crate::scenario::{BlockFetchPoints, Scenario, StepDef, StepKind};
use crate::trace::{Direction, EventKind, TraceEvent, Tracer};

use super::parse_point;

// ── Protocol subscription ─────────────────────────────────────────────────────

/// Returns the set of N2N protocol IDs to subscribe on every connection,
/// based on the (post-handshake) negotiated version.
///
/// All subscriptions must happen before `plexer.spawn()`, so this is called
/// at connect time with `u64::MAX` as a conservative upper bound. Once the
/// harness can defer subscriptions until after handshake, pass the real version.
///
/// When Pallas adds `PROTOCOL_N2N_PEER_SHARING` (not present in 0.36.0),
/// add: `if negotiated_version >= 13 { protocols.push(PROTOCOL_N2N_PEER_SHARING); }`
pub fn subscribed_protocols(_negotiated_version: u64) -> Vec<u16> {
    vec![
        CHAIN_SYNC_PROTOCOL,
        BLOCK_FETCH_PROTOCOL,
        TX_SUBMISSION_PROTOCOL,
        KEEP_ALIVE_PROTOCOL,
        // Peer-Sharing: not in pallas-network 0.36.0.
        // When added: if negotiated_version >= PEER_SHARING_MIN_VERSION {
        //     protocols.push(PEER_SHARING_PROTOCOL);
        // }
    ]
}

// ── Runner ────────────────────────────────────────────────────────────────────

pub struct ScenarioRunner {
    scenario: Scenario,
}

impl ScenarioRunner {
    pub fn new(scenario: Scenario) -> Self {
        Self { scenario }
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

        let mut state = RunnerState::default();
        let mut steps_passed: u64 = 0;
        let mut steps_failed: u64 = 0;

        for (idx, step_def) in self.scenario.steps.iter().enumerate() {
            // Clear the buffer before each step so assertions only see this
            // step's own events.
            tracer.drain_buffer().await;

            tracer
                .emit(TraceEvent::new(
                    EventKind::StepStarted,
                    Direction::Internal,
                    json!({ "index": idx, "kind": step_def.kind.as_str() }),
                ))
                .await?;

            // Drain again so StepStarted is not visible to this step's assertions.
            tracer.drain_buffer().await;

            let step_result = execute_step(
                step_def,
                &self.scenario.target_address,
                self.scenario.network_magic,
                &mut state,
                &tracer,
            )
            .await;

            let step_events = tracer.drain_buffer().await;

            // Evaluate expect clauses.
            let mut assertions_ok = true;
            if let Some(expect) = &step_def.expect {
                for result in evaluate_assertions(expect, &step_events) {
                    if !result.passed {
                        assertions_ok = false;
                    }
                    let kind = if result.passed {
                        EventKind::AssertionPassed
                    } else {
                        EventKind::AssertionFailed
                    };
                    tracer
                        .emit(TraceEvent::new(
                            kind,
                            Direction::Internal,
                            json!({
                                "step_index": idx,
                                "assertion":  result.name,
                                "message":    result.message,
                            }),
                        ))
                        .await?;
                }
            }

            match (step_result, assertions_ok) {
                (Ok(_), true) => {
                    steps_passed += 1;
                    tracer
                        .emit(TraceEvent::new(
                            EventKind::StepCompleted,
                            Direction::Internal,
                            json!({ "index": idx, "outcome": "ok" }),
                        ))
                        .await?;
                }
                (Ok(_), false) => {
                    steps_failed += 1;
                    tracer
                        .emit(TraceEvent::new(
                            EventKind::StepCompleted,
                            Direction::Internal,
                            json!({ "index": idx, "outcome": "assertion_failed" }),
                        ))
                        .await?;
                    cleanup(&mut state, &tracer).await;
                    emit_completed(
                        &tracer,
                        &self.scenario.name,
                        steps_passed,
                        steps_failed,
                        started_at,
                        "assertion_failed",
                    )
                    .await;
                    anyhow::bail!(
                        "scenario \"{}\" step {idx} ({}) failed assertions",
                        self.scenario.name,
                        step_def.kind.as_str()
                    );
                }
                (Err(e), _) => {
                    steps_failed += 1;
                    tracer
                        .emit(TraceEvent::new(
                            EventKind::StepCompleted,
                            Direction::Internal,
                            json!({ "index": idx, "outcome": "error", "error": e.to_string() }),
                        ))
                        .await?;
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
}

// ── Step dispatch ─────────────────────────────────────────────────────────────

async fn execute_step(
    step: &StepDef,
    target_address: &str,
    network_magic: u64,
    state: &mut RunnerState,
    tracer: &Tracer,
) -> anyhow::Result<()> {
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

        StepKind::ChainSync => {
            let channel = state
                .cs_channel
                .take()
                .ok_or_else(|| anyhow::anyhow!("chain_sync step: no channel"))?;

            let origin = vec!["origin".to_string()];
            let raw_points = step.params.intersection_points.as_ref().unwrap_or(&origin);
            let intersection_points = raw_points
                .iter()
                .map(|s| parse_point(s))
                .collect::<anyhow::Result<Vec<Point>>>()?;

            let count = step.params.count.unwrap_or(10);
            let await_secs = step.params.await_timeout_secs.unwrap_or(30);

            let summary = run_chain_sync(
                channel,
                intersection_points,
                count,
                Duration::from_secs(await_secs),
                tracer,
            )
            .await?;

            let n = summary.collected_points.len();
            state.last_chain_sync_points = summary.collected_points;
            info!(headers = summary.headers_received, points = n, "Chain-sync step complete");
            Ok(())
        }

        StepKind::BlockFetch => {
            let channel = state
                .bf_channel
                .take()
                .ok_or_else(|| anyhow::anyhow!("block_fetch step: no channel"))?;

            let points: Vec<Point> =
                match step.params.points.as_ref().unwrap_or(&BlockFetchPoints::FromChainSync) {
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

            let batch_size = step.params.batch_size.unwrap_or(1);
            let summary = run_block_fetch(channel, points, batch_size, tracer).await?;
            info!(
                blocks = summary.blocks_received,
                bytes = summary.total_bytes,
                "Block-fetch step complete"
            );
            Ok(())
        }

        StepKind::Disconnect => {
            // Abort background loops before the plexer — they use the channels.
            // ka_channel / ts_channel are Some only if handshake never ran;
            // dropping them here is harmless (the plexer hasn't seen traffic on them).
            state.ka_channel.take();
            state.ts_channel.take();
            if let Some(handle) = state.ka_handle.take() {
                handle.abort();
            }
            if let Some(handle) = state.ts_handle.take() {
                handle.abort();
            }
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
            let secs = step.params.duration_secs.unwrap_or(0);
            info!(secs, "Sleeping");
            tokio::time::sleep(Duration::from_secs(secs)).await;
            Ok(())
        }
    }
}

/// Abort the keep-alive loop and plexer, emit ConnectionClosed if still
/// connected. Called on error/assertion-failure paths before returning.
async fn cleanup(state: &mut RunnerState, tracer: &Tracer) {
    state.ka_channel.take();
    state.ts_channel.take();
    if let Some(handle) = state.ka_handle.take() {
        handle.abort();
    }
    if let Some(handle) = state.ts_handle.take() {
        handle.abort();
    }
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
        // Same set expected for all currently-deployed N2N versions.
        for &version in &[7u64, 11, 13, 14] {
            let ps = subscribed_protocols(version);
            assert!(ps.contains(&CHAIN_SYNC_PROTOCOL),    "v{version}: missing chain-sync");
            assert!(ps.contains(&BLOCK_FETCH_PROTOCOL),   "v{version}: missing block-fetch");
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
    /// Requires the devnet; verifies the invariant at runtime against a real connection.
    ///
    /// Run with: cargo test -p cardano-conformance-harness scenario::runner::tests::background_workers_spawned_after_handshake_not_before -- --ignored
    #[tokio::test]
    #[ignore = "requires devnet: docker compose up"]
    async fn background_workers_spawned_after_handshake_not_before() {
        use crate::scenario::{StepDef, StepKind, StepParams};
        use tempfile::NamedTempFile;

        let tmp = NamedTempFile::new().unwrap();
        let tracer = crate::trace::Tracer::open(tmp.path()).await.unwrap();
        let mut state = RunnerState::default();

        let connect = StepDef { kind: StepKind::Connect, params: StepParams::default(), expect: None };
        execute_step(&connect, "localhost:3001", crate::DEVNET_MAGIC, &mut state, &tracer)
            .await
            .expect("connect should succeed");

        // After Connect: channels exist but no workers yet.
        assert!(state.ka_handle.is_none(),  "ka_handle must be None after Connect");
        assert!(state.ts_handle.is_none(),  "ts_handle must be None after Connect");
        assert!(state.ka_channel.is_some(), "ka_channel must be Some after Connect");
        assert!(state.ts_channel.is_some(), "ts_channel must be Some after Connect");

        let handshake = StepDef { kind: StepKind::Handshake, params: StepParams::default(), expect: None };
        execute_step(&handshake, "localhost:3001", crate::DEVNET_MAGIC, &mut state, &tracer)
            .await
            .expect("handshake should succeed");

        // After Handshake: channels consumed, workers running.
        assert!(state.ka_handle.is_some(),  "ka_handle must be Some after Handshake");
        assert!(state.ts_handle.is_some(),  "ts_handle must be Some after Handshake");
        assert!(state.ka_channel.is_none(), "ka_channel must be None after Handshake (consumed)");
        assert!(state.ts_channel.is_none(), "ts_channel must be None after Handshake (consumed)");

        // Cleanup.
        if let Some(h) = state.ka_handle.take() { h.abort(); }
        if let Some(h) = state.ts_handle.take() { h.abort(); }
        if let Some(h) = state.plexer_handle.take() { h.abort().await; }
    }
}
