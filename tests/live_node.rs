use std::time::Duration;

use cardano_conformance_harness::miniprotocols::blockfetch::{run_block_fetch, BLOCK_FETCH_PROTOCOL};
use cardano_conformance_harness::miniprotocols::chainsync::{run_chain_sync, CHAIN_SYNC_PROTOCOL};
use cardano_conformance_harness::miniprotocols::handshake::{handshake_on_channel, run_handshake};
use cardano_conformance_harness::miniprotocols::keepalive::{run_keepalive, KEEP_ALIVE_PROTOCOL};
use cardano_conformance_harness::miniprotocols::txsubmission::{run_tx_submission, TX_SUBMISSION_PROTOCOL};
use cardano_conformance_harness::scenario::runner::ScenarioRunner;
use cardano_conformance_harness::scenario::{Assertions, Scenario, StepDef, StepKind};
use cardano_conformance_harness::trace::Tracer;
use cardano_conformance_harness::DEVNET_MAGIC;
use pallas_network::miniprotocols::keepalive::Client as KaClient;
use pallas_network::miniprotocols::PROTOCOL_N2N_HANDSHAKE;
use pallas_network::multiplexer::{Bearer, Plexer};
use serde_json::Value;
use tempfile::NamedTempFile;

// ── Scenario test helpers ─────────────────────────────────────────────────────

fn make_scenario(name: &str, trace_path: &std::path::Path, steps: Vec<StepDef>) -> Scenario {
    Scenario {
        name: name.to_string(),
        description: None,
        target_address: Some(DEVNET_ADDR.to_string()),
        network_magic: DEVNET_MAGIC,
        trace_output_path: trace_path.to_path_buf(),
        expected_outcome: None,
        network: None,
        steps,
    }
}

fn simple_step(kind: StepKind) -> StepDef {
    StepDef { kind, raw_params: serde_json::json!({}), output: None, as_name: None, on_name: None, expect: None }
}

fn chain_sync_step(count: u64) -> StepDef {
    StepDef {
        kind: StepKind::ChainSync,
        raw_params: serde_json::json!({ "count": count }),
        output: None,
        as_name: None,
        on_name: None,
        expect: None,
    }
}

fn sleep_step(secs: u64) -> StepDef {
    StepDef {
        kind: StepKind::Sleep,
        raw_params: serde_json::json!({ "duration_secs": secs }),
        output: None,
        as_name: None,
        on_name: None,
        expect: None,
    }
}

fn step_with_expect(kind: StepKind, expect: Assertions) -> StepDef {
    StepDef { kind, raw_params: serde_json::json!({}), output: None, as_name: None, on_name: None, expect: Some(expect) }
}

const DEVNET_ADDR: &str = "localhost:3001";
const AWAIT_TIMEOUT: Duration = Duration::from_secs(30);

/// Opens one TCP connection, runs handshake + chain-sync for `count` headers.
/// Returns the trace file and the negotiated version.
async fn run_session(count: u64) -> (NamedTempFile, u64) {
    let tmp = NamedTempFile::new().unwrap();
    let tracer = Tracer::open(tmp.path()).await.unwrap();

    let bearer = Bearer::connect_tcp(DEVNET_ADDR).await.unwrap();
    let mut plexer = Plexer::new(bearer);
    let hs_channel = plexer.subscribe_client(PROTOCOL_N2N_HANDSHAKE);
    let cs_channel = plexer.subscribe_client(CHAIN_SYNC_PROTOCOL);
    let plexer_handle = plexer.spawn();

    let version = handshake_on_channel(hs_channel, DEVNET_MAGIC, &tracer)
        .await
        .expect("handshake should succeed against devnet");

    let summary = run_chain_sync(cs_channel, vec![pallas_network::miniprotocols::Point::Origin], count, AWAIT_TIMEOUT, &tracer)
        .await
        .expect("chain-sync should succeed against devnet");

    plexer_handle.abort().await;

    assert_eq!(summary.headers_received, count);
    assert_eq!(summary.exit_reason, "completed");

    (tmp, version)
}

/// Opens one TCP connection, runs handshake + chain-sync for `count` headers,
/// then block-fetch for all collected points. Returns the trace file.
async fn run_full_session(count: u64) -> NamedTempFile {
    let tmp = NamedTempFile::new().unwrap();
    let tracer = Tracer::open(tmp.path()).await.unwrap();

    let bearer = Bearer::connect_tcp(DEVNET_ADDR).await.unwrap();
    let mut plexer = Plexer::new(bearer);
    let hs_channel = plexer.subscribe_client(PROTOCOL_N2N_HANDSHAKE);
    let cs_channel = plexer.subscribe_client(CHAIN_SYNC_PROTOCOL);
    let bf_channel = plexer.subscribe_client(BLOCK_FETCH_PROTOCOL);
    let plexer_handle = plexer.spawn();

    handshake_on_channel(hs_channel, DEVNET_MAGIC, &tracer)
        .await
        .expect("handshake should succeed");

    let cs_summary = run_chain_sync(cs_channel, vec![pallas_network::miniprotocols::Point::Origin], count, AWAIT_TIMEOUT, &tracer)
        .await
        .expect("chain-sync should succeed");

    assert_eq!(cs_summary.headers_received, count);
    assert_eq!(
        cs_summary.collected_points.len() as u64,
        count,
        "collected_points count should equal headers_received"
    );

    run_block_fetch(bf_channel, cs_summary.collected_points, 1, &tracer)
        .await
        .expect("block-fetch should succeed");

    plexer_handle.abort().await;

    tmp
}

fn read_trace(tmp: &NamedTempFile) -> Vec<Value> {
    std::fs::read_to_string(tmp.path())
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).expect("each trace line must be valid JSON"))
        .collect()
}

// ── Handshake tests ──────────────────────────────────────────────────────────

/// Full handshake against the local Docker devnet.
///
/// Run with: cargo test --test live_node -- --ignored
#[tokio::test]
#[ignore = "requires devnet: docker compose up"]
async fn handshake_completes_against_devnet() {
    let tmp = NamedTempFile::new().unwrap();
    let tracer = Tracer::open(tmp.path()).await.unwrap();

    let version = run_handshake(DEVNET_ADDR, DEVNET_MAGIC, &tracer)
        .await
        .expect("handshake against devnet should succeed");

    assert!(version >= 7, "negotiated version should be a valid N2N version, got {version}");

    let events = read_trace(&tmp);

    let expected_kinds = [
        "connection_opened",
        "handshake_started",
        "handshake_version_proposed",
        "handshake_version_accepted",
        "handshake_completed",
        "connection_closed",
    ];
    assert_eq!(events.len(), expected_kinds.len());
    for (event, expected_kind) in events.iter().zip(expected_kinds.iter()) {
        assert_eq!(event["kind"], *expected_kind);
        assert!(event["timestamp"].is_string());
        assert!(event["direction"].is_string());
    }

    assert_eq!(events[3]["payload"]["version"], version);
    assert_eq!(events[4]["payload"]["negotiated_version"], version);

    let proposed = events[2]["payload"]["versions"].as_array().unwrap();
    assert!(proposed.iter().any(|v| v == version));
}

/// Handshake with the wrong magic must be rejected by the node, not cause a panic.
///
/// Run with: cargo test --test live_node -- --ignored
#[tokio::test]
#[ignore = "requires devnet: docker compose up"]
async fn handshake_rejected_with_wrong_magic() {
    let tmp = NamedTempFile::new().unwrap();
    let tracer = Tracer::open(tmp.path()).await.unwrap();

    let result = run_handshake(DEVNET_ADDR, 999_999, &tracer).await;

    assert!(result.is_err(), "handshake with wrong magic should fail");

    let contents = std::fs::read_to_string(tmp.path()).unwrap();
    assert!(!contents.is_empty());
    for line in contents.lines() {
        serde_json::from_str::<Value>(line).expect("every trace line must be valid JSON");
    }

    let last: Value = serde_json::from_str(contents.lines().last().unwrap()).unwrap();
    let last_kind = last["kind"].as_str().unwrap();
    assert!(
        matches!(last_kind, "handshake_version_rejected" | "error" | "connection_closed"),
        "last event on rejected handshake should be a rejection or error, got {last_kind}"
    );
}

// ── Chain-Sync tests ─────────────────────────────────────────────────────────

/// Chain-Sync consumes exactly N headers and the trace has the expected event
/// sequence.
///
/// Run with: cargo test --test live_node -- --ignored
#[tokio::test]
#[ignore = "requires devnet: docker compose up"]
async fn chain_sync_receives_n_headers_from_devnet() {
    let (tmp, _version) = run_session(5).await;
    let events = read_trace(&tmp);

    let cs_events: Vec<&Value> = events
        .iter()
        .filter(|e| e["mini_protocol"] == "chain-sync")
        .collect();

    assert_eq!(cs_events.first().unwrap()["kind"], "chain_sync_started");
    assert_eq!(cs_events.last().unwrap()["kind"], "chain_sync_session_summary");

    let roll_forwards = cs_events
        .iter()
        .filter(|e| e["kind"] == "chain_sync_roll_forward")
        .count();
    assert_eq!(roll_forwards, 5, "expected 5 roll_forward events");

    for e in cs_events.iter().filter(|e| e["kind"] == "chain_sync_roll_forward") {
        assert!(e["payload"]["cbor_hex"].is_string(), "cbor_hex missing");
        assert!(e["payload"]["cbor_len"].is_number(), "cbor_len missing");
        assert!(e["payload"]["variant"].is_number(), "variant missing");
        assert!(e["payload"]["tip"].is_object(), "tip missing");
    }

    let summary = cs_events.last().unwrap();
    assert_eq!(summary["payload"]["headers_received"], 5);
    assert_eq!(summary["payload"]["exit_reason"], "completed");

    // Wire events (sent/received) must carry state_before and state_after.
    // Internal meta-events (started, summary, errors) intentionally omit them.
    for e in cs_events.iter().filter(|e| e["direction"] != "internal") {
        assert!(
            e["state_before"].is_string(),
            "state_before missing on wire event {:?}",
            e["kind"]
        );
        assert!(
            e["state_after"].is_string(),
            "state_after missing on wire event {:?}",
            e["kind"]
        );
    }
}

/// `MsgFindIntersect` at genesis is always answered with `IntersectFound`
/// and the point is "origin".
///
/// Run with: cargo test --test live_node -- --ignored
#[tokio::test]
#[ignore = "requires devnet: docker compose up"]
async fn chain_sync_intersect_found_at_genesis() {
    let (tmp, _version) = run_session(1).await;
    let events = read_trace(&tmp);

    let intersect_found = events
        .iter()
        .find(|e| e["kind"] == "chain_sync_intersect_found")
        .expect("chain_sync_intersect_found event must be present");

    assert_eq!(intersect_found["payload"]["point"], "origin");
    assert!(intersect_found["payload"]["tip"].is_object());
}

// ── Block-Fetch tests ─────────────────────────────────────────────────────────

/// Block-Fetch retrieves the full block body for each header collected by
/// Chain-Sync. Verifies the event sequence, payload fields, and summary.
///
/// Run with: cargo test --test live_node -- --ignored
#[tokio::test]
#[ignore = "requires devnet: docker compose up"]
async fn block_fetch_fetches_blocks_from_devnet() {
    const HEADER_COUNT: u64 = 5;
    let tmp = run_full_session(HEADER_COUNT).await;
    let events = read_trace(&tmp);

    let bf_events: Vec<&Value> = events
        .iter()
        .filter(|e| e["mini_protocol"] == "block-fetch")
        .collect();

    // Session must start with block_fetch_started and end with summary.
    assert_eq!(bf_events.first().unwrap()["kind"], "block_fetch_started");
    assert_eq!(bf_events.last().unwrap()["kind"], "block_fetch_session_summary");

    // With batch_size=1 there is one request-range per block.
    let request_ranges = bf_events
        .iter()
        .filter(|e| e["kind"] == "block_fetch_request_range")
        .count();
    assert_eq!(request_ranges as u64, HEADER_COUNT);

    // Every block event must carry cbor_hex and cbor_len.
    let block_events: Vec<&&Value> = bf_events
        .iter()
        .filter(|e| e["kind"] == "block_fetch_block")
        .collect();
    assert_eq!(
        block_events.len() as u64,
        HEADER_COUNT,
        "expected {HEADER_COUNT} block events"
    );
    for e in &block_events {
        assert!(e["payload"]["cbor_hex"].is_string(), "cbor_hex missing");
        assert!(e["payload"]["cbor_len"].is_number(), "cbor_len missing");
        let cbor_len = e["payload"]["cbor_len"].as_u64().unwrap();
        assert!(cbor_len > 0, "block body must be non-empty");
    }

    // Summary must report the correct counts.
    let summary = bf_events.last().unwrap();
    assert_eq!(summary["payload"]["blocks_received"], HEADER_COUNT);
    assert_eq!(summary["payload"]["no_blocks_responses"], 0);
    assert_eq!(summary["payload"]["exit_reason"], "completed");
    assert!(
        summary["payload"]["total_bytes"].as_u64().unwrap() > 0,
        "total_bytes must be positive"
    );

    // Wire events must have state_before and state_after.
    for e in bf_events.iter().filter(|e| e["direction"] != "internal") {
        assert!(
            e["state_before"].is_string(),
            "state_before missing on {:?}",
            e["kind"]
        );
        assert!(
            e["state_after"].is_string(),
            "state_after missing on {:?}",
            e["kind"]
        );
    }
}

// ── Scenario runner tests ─────────────────────────────────────────────────────

/// ScenarioRunner executes a multi-step scenario and emits scenario-level
/// trace events (scenario_started, step_started/completed, scenario_completed).
///
/// Run with: cargo test --test live_node -- --ignored
#[tokio::test]
#[ignore = "requires devnet: docker compose up"]
async fn scenario_runner_emits_scenario_and_step_events() {
    let tmp = NamedTempFile::new().unwrap();
    let scenario = make_scenario(
        "test_scenario",
        tmp.path(),
        vec![
            simple_step(StepKind::Connect),
            simple_step(StepKind::Handshake),
            chain_sync_step(3),
            simple_step(StepKind::Disconnect),
        ],
    );

    ScenarioRunner::new(scenario).run().await.expect("scenario should succeed");

    let events = read_trace(&tmp);

    // Scenario bookends must be present.
    let first = events.first().unwrap();
    assert_eq!(first["kind"], "scenario_started");
    assert_eq!(first["payload"]["name"], "test_scenario");
    assert_eq!(first["payload"]["steps"], 4);

    let last = events.last().unwrap();
    assert_eq!(last["kind"], "scenario_completed");
    assert_eq!(last["payload"]["outcome"], "completed");
    assert_eq!(last["payload"]["steps_passed"], 4);
    assert_eq!(last["payload"]["steps_failed"], 0);

    // Each step must have a matching started/completed pair.
    let started_count = events.iter().filter(|e| e["kind"] == "step_started").count();
    let completed_count = events.iter().filter(|e| e["kind"] == "step_completed").count();
    assert_eq!(started_count, 4);
    assert_eq!(completed_count, 4);

    // All steps must report outcome "ok".
    for e in events.iter().filter(|e| e["kind"] == "step_completed") {
        assert_eq!(e["payload"]["outcome"], "ok", "step {:?} did not complete ok", e["payload"]["index"]);
    }
}

/// After connect, the Tx-Submission background task immediately sends MsgInit
/// to declare the harness as a producer. This event must appear in the trace.
///
/// Run with: cargo test --test live_node -- --ignored
#[tokio::test]
#[ignore = "requires devnet: docker compose up"]
async fn tx_submission_init_logged_to_trace() {
    let tmp = NamedTempFile::new().unwrap();
    let scenario = make_scenario(
        "tx_sub_init_test",
        tmp.path(),
        vec![
            simple_step(StepKind::Connect),
            simple_step(StepKind::Handshake),
            // Give the background task a moment to send MsgInit.
            sleep_step(1),
            simple_step(StepKind::Disconnect),
        ],
    );

    ScenarioRunner::new(scenario).run().await.expect("scenario should succeed");

    let events = read_trace(&tmp);
    let init_event = events
        .iter()
        .find(|e| e["kind"] == "tx_submission_message" && e["payload"]["msg_kind"] == "init");

    assert!(
        init_event.is_some(),
        "expected a tx_submission_message(init) event in the trace — found kinds: {:?}",
        events.iter().map(|e| &e["kind"]).collect::<Vec<_>>()
    );
    assert_eq!(init_event.unwrap()["direction"], "sent");
    assert_eq!(init_event.unwrap()["mini_protocol"], "tx-submission");
}

/// Assertions that pass emit assertion_passed events; the scenario still
/// completes successfully.
///
/// Run with: cargo test --test live_node -- --ignored
#[tokio::test]
#[ignore = "requires devnet: docker compose up"]
async fn passing_assertions_emit_assertion_passed_events() {
    let tmp = NamedTempFile::new().unwrap();
    let scenario = make_scenario(
        "assertion_pass_test",
        tmp.path(),
        vec![
            simple_step(StepKind::Connect),
            step_with_expect(
                StepKind::Handshake,
                Assertions {
                    must_contain_kind: Some(vec!["handshake_completed".into()]),
                    must_not_contain_kind: Some(vec!["error".into()]),
                    ..Default::default()
                },
            ),
            simple_step(StepKind::Disconnect),
        ],
    );

    ScenarioRunner::new(scenario).run().await.expect("scenario should succeed");

    let events = read_trace(&tmp);

    // No failures.
    assert!(
        events.iter().all(|e| e["kind"] != "assertion_failed"),
        "expected no assertion_failed events"
    );

    // The two assertions must have produced assertion_passed events.
    let passed: Vec<_> = events.iter().filter(|e| e["kind"] == "assertion_passed").collect();
    assert_eq!(passed.len(), 2, "expected 2 assertion_passed events");

    // Scenario must still have completed successfully.
    let completed = events.iter().find(|e| e["kind"] == "scenario_completed").unwrap();
    assert_eq!(completed["payload"]["outcome"], "completed");
    assert_eq!(completed["payload"]["steps_failed"], 0);
}

/// A failing assertion aborts the scenario, emits assertion_failed, and the
/// runner returns an error. scenario_completed carries outcome "assertion_failed".
///
/// Run with: cargo test --test live_node -- --ignored
#[tokio::test]
#[ignore = "requires devnet: docker compose up"]
async fn failing_assertion_aborts_scenario() {
    let tmp = NamedTempFile::new().unwrap();
    let scenario = make_scenario(
        "assertion_fail_test",
        tmp.path(),
        vec![
            simple_step(StepKind::Connect),
            step_with_expect(
                StepKind::Handshake,
                Assertions {
                    // This kind never appears during handshake.
                    must_contain_kind: Some(vec!["chain_sync_roll_forward".into()]),
                    ..Default::default()
                },
            ),
            // This step must NOT execute — scenario should have aborted.
            simple_step(StepKind::Disconnect),
        ],
    );

    let result = ScenarioRunner::new(scenario).run().await;
    assert!(result.is_err(), "runner should return Err on assertion failure");

    let events = read_trace(&tmp);

    let failed = events.iter().find(|e| e["kind"] == "assertion_failed");
    assert!(failed.is_some(), "expected an assertion_failed event");
    assert!(failed.unwrap()["payload"]["assertion"].as_str().unwrap()
        .contains("chain_sync_roll_forward"));

    let completed = events.iter().find(|e| e["kind"] == "scenario_completed").unwrap();
    assert_eq!(completed["payload"]["outcome"], "assertion_failed");
    assert_eq!(completed["payload"]["steps_failed"], 1);
}

// ── Keep-Alive protocol tests ─────────────────────────────────────────────────

/// Keep-alive pings and responses are both logged to the trace.
/// Uses a 1-second interval so the test completes quickly.
///
/// Run with: cargo test --test live_node -- --ignored
#[tokio::test]
#[ignore = "requires devnet: docker compose up"]
async fn keep_alive_sent_and_received_appear_in_trace() {
    let tmp = NamedTempFile::new().unwrap();
    let tracer = Tracer::open(tmp.path()).await.unwrap();

    // Connect and handshake manually so we can pass a short interval to
    // run_keepalive directly.
    let bearer = Bearer::connect_tcp(DEVNET_ADDR).await.unwrap();
    let mut plexer = Plexer::new(bearer);
    let hs_channel = plexer.subscribe_client(PROTOCOL_N2N_HANDSHAKE);
    let ka_channel = plexer.subscribe_client(KEEP_ALIVE_PROTOCOL);
    let plexer_handle = plexer.spawn();

    handshake_on_channel(hs_channel, DEVNET_MAGIC, &tracer)
        .await
        .expect("handshake should succeed");

    let ka_client = KaClient::new(ka_channel);
    let ka_handle = tokio::spawn(run_keepalive(
        ka_client,
        tracer.clone(),
        Duration::from_secs(1), // short interval for testing
    ));

    // Wait long enough for at least one full ping/response cycle.
    tokio::time::sleep(Duration::from_secs(3)).await;

    ka_handle.abort();
    plexer_handle.abort().await;

    let events: Vec<Value> = std::fs::read_to_string(tmp.path())
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();

    let sent: Vec<_> = events.iter()
        .filter(|e| e["kind"] == "keep_alive_sent")
        .collect();
    let received: Vec<_> = events.iter()
        .filter(|e| e["kind"] == "keep_alive_received")
        .collect();

    assert!(!sent.is_empty(), "expected at least one keep_alive_sent event");
    assert!(!received.is_empty(), "expected at least one keep_alive_received event");
    assert_eq!(
        sent.len(), received.len(),
        "every sent ping should have a matching received response"
    );

    // Cookie values must match between sent and received.
    for (s, r) in sent.iter().zip(received.iter()) {
        assert_eq!(
            s["payload"]["cookie"], r["payload"]["cookie"],
            "cookie mismatch between sent and received"
        );
    }

    // Events must carry the mini-protocol label.
    for e in sent.iter().chain(received.iter()) {
        assert_eq!(e["mini_protocol"], "keep-alive");
    }
}

// ── Tx-Submission protocol tests ──────────────────────────────────────────────

/// The Tx-Submission background task sends MsgInit immediately after connect,
/// then responds to any RequestTxIds from the node with an empty list.
/// Verifies init and any subsequent exchange are all in the trace.
///
/// Run with: cargo test --test live_node -- --ignored
#[tokio::test]
#[ignore = "requires devnet: docker compose up"]
async fn tx_submission_init_and_exchange_logged() {
    let tmp = NamedTempFile::new().unwrap();
    let tracer = Tracer::open(tmp.path()).await.unwrap();

    let bearer = Bearer::connect_tcp(DEVNET_ADDR).await.unwrap();
    let mut plexer = Plexer::new(bearer);
    let hs_channel = plexer.subscribe_client(PROTOCOL_N2N_HANDSHAKE);
    let ts_channel = plexer.subscribe_client(TX_SUBMISSION_PROTOCOL);
    let plexer_handle = plexer.spawn();

    handshake_on_channel(hs_channel, DEVNET_MAGIC, &tracer)
        .await
        .expect("handshake should succeed");

    let ts_handle = tokio::spawn(run_tx_submission(ts_channel, tracer.clone()));

    // Give the task time to send MsgInit and receive any node requests.
    tokio::time::sleep(Duration::from_secs(2)).await;

    ts_handle.abort();
    plexer_handle.abort().await;

    let events: Vec<Value> = std::fs::read_to_string(tmp.path())
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();

    let ts_events: Vec<_> = events.iter()
        .filter(|e| e["mini_protocol"] == "tx-submission")
        .collect();

    // MsgInit must always be the first tx-submission event.
    let init = ts_events.first().expect("expected at least one tx_submission_message event");
    assert_eq!(init["kind"], "tx_submission_message");
    assert_eq!(init["payload"]["msg_kind"], "init");
    assert_eq!(init["direction"], "sent");

    // All tx-submission events must be valid JSON with direction and mini_protocol.
    for e in &ts_events {
        assert!(e["direction"].is_string());
        assert_eq!(e["mini_protocol"], "tx-submission");
        assert!(e["payload"]["msg_kind"].is_string());
    }

    // If the node requested transaction IDs, our replies must be present too.
    let requests: Vec<_> = ts_events.iter()
        .filter(|e| e["payload"]["msg_kind"] == "request_tx_ids")
        .collect();
    let replies: Vec<_> = ts_events.iter()
        .filter(|e| e["payload"]["msg_kind"] == "reply_tx_ids")
        .collect();
    assert_eq!(
        requests.len(), replies.len(),
        "every request_tx_ids must have a corresponding reply_tx_ids"
    );
}

// ── Repeat semantics tests ────────────────────────────────────────────────────

/// Successful repeat: all iterations produce RepeatIterationCompleted(ok) and
/// the scenario ends with ScenarioCompleted(completed). Uses only sleep steps —
/// no devnet required.
#[tokio::test]
async fn repeat_all_iterations_complete_successfully() {
    let tmp = NamedTempFile::new().unwrap();
    // No connect/disconnect — sleep steps need no connection.
    let scenario = make_scenario(
        "repeat_success_test",
        tmp.path(),
        vec![StepDef {
            kind: StepKind::Repeat,
            raw_params: serde_json::json!({
                "times": 3,
                "body": [{ "kind": "sleep", "duration_secs": 0 }]
            }),
            output: None,
            as_name: None,
            on_name: None,
            expect: None,
        }],
    );

    ScenarioRunner::new(scenario).run().await.expect("scenario should succeed");

    let events = read_trace(&tmp);

    let started: Vec<_> = events.iter().filter(|e| e["kind"] == "repeat_iteration_started").collect();
    let completed: Vec<_> = events.iter().filter(|e| e["kind"] == "repeat_iteration_completed").collect();
    assert_eq!(started.len(), 3, "3 iterations must have started");
    assert_eq!(completed.len(), 3, "3 iterations must have completed");
    for (i, c) in completed.iter().enumerate() {
        assert_eq!(c["payload"]["iteration"], i, "iteration numbers must be sequential");
        assert_eq!(c["payload"]["outcome"], "ok");
    }

    let sc = events.iter().find(|e| e["kind"] == "scenario_completed").unwrap();
    assert_eq!(sc["payload"]["outcome"], "completed");
}

/// When a body step's assertion fails on iteration N, the trace must record:
/// (a) RepeatIterationCompleted(error) for iteration N,
/// (b) iterations > N never start,
/// (c) ScenarioCompleted(step_error) still fires.
/// Uses only sleep steps — no devnet required.
#[tokio::test]
async fn repeat_error_path_emits_correct_trace_events() {
    let tmp = NamedTempFile::new().unwrap();
    let scenario = make_scenario(
        "repeat_error_test",
        tmp.path(),
        vec![StepDef {
            kind: StepKind::Repeat,
            raw_params: serde_json::json!({
                "times": 3,
                "body": [{
                    "kind": "sleep",
                    "duration_secs": 0,
                    // sleep emits 0 events — min_events: 999 always fails
                    "expect": { "min_events": 999 }
                }]
            }),
            output: None,
            as_name: None,
            on_name: None,
            expect: None,
        }],
    );

    let result = ScenarioRunner::new(scenario).run().await;
    assert!(result.is_err(), "scenario must fail when body assertion fails");

    let events = read_trace(&tmp);

    // Only iteration 0 should have started.
    let started: Vec<_> = events.iter().filter(|e| e["kind"] == "repeat_iteration_started").collect();
    assert_eq!(started.len(), 1, "only the failing iteration should have started");
    assert_eq!(started[0]["payload"]["iteration"], 0);

    // Iteration 0 must have a RepeatIterationCompleted with an error outcome.
    let completed: Vec<_> = events.iter().filter(|e| e["kind"] == "repeat_iteration_completed").collect();
    assert_eq!(completed.len(), 1, "exactly one RepeatIterationCompleted must be emitted");
    assert_eq!(completed[0]["payload"]["iteration"], 0);
    assert_ne!(completed[0]["payload"]["outcome"], "ok", "failing iteration must not report ok");

    // ScenarioCompleted must always fire — even on failure.
    let sc = events.iter().find(|e| e["kind"] == "scenario_completed")
        .expect("scenario_completed must be present even on failure");
    assert_ne!(sc["payload"]["outcome"], "completed");
    assert_eq!(sc["payload"]["steps_failed"], 1);
}

/// Body steps within a repeat iteration see variables set by earlier body steps
/// in the same iteration. query_tip stores the tip in a variable; chain_sync
/// immediately uses it as the intersection point.
///
/// Run with: cargo test --test live_node -- --ignored
#[tokio::test]
#[ignore = "requires devnet: docker compose up"]
async fn repeat_body_variables_visible_within_same_iteration() {
    let tmp = NamedTempFile::new().unwrap();
    let scenario = make_scenario(
        "repeat_variable_visibility",
        tmp.path(),
        vec![
            simple_step(StepKind::Connect),
            simple_step(StepKind::Handshake),
            // Single iteration: query_tip then immediately chain_sync from that tip.
            // If variable visibility worked, chain_sync uses $tip.point; if it didn't,
            // the substitution would fail with "unknown variable: tip".
            StepDef {
                kind: StepKind::Repeat,
                raw_params: serde_json::json!({
                    "times": 1,
                    "body": [
                        { "kind": "query_tip", "output": "tip" },
                        {
                            "kind": "chain_sync",
                            "intersection_points": ["$tip.point"],
                            "count": 2
                        }
                    ]
                }),
                output: None,
                as_name: None,
                on_name: None,
                expect: None,
            },
            simple_step(StepKind::Disconnect),
        ],
    );

    ScenarioRunner::new(scenario).run().await
        .expect("scenario should succeed — $tip.point must be visible to chain_sync in the same iteration");

    let events = read_trace(&tmp);

    // Variable resolution event must be present for the $tip.point reference.
    let var_refs: Vec<_> = events.iter()
        .filter(|e| e["kind"] == "variable_referenced")
        .collect();
    assert!(
        var_refs.iter().any(|e| e["payload"]["reference"] == "$tip.point"),
        "expected a variable_referenced event for $tip.point"
    );

    // chain_sync must have actually run — roll_forward events must appear.
    assert!(
        events.iter().any(|e| e["kind"] == "chain_sync_roll_forward"),
        "chain_sync must have executed successfully using the resolved tip point"
    );

    // No error events.
    assert!(
        events.iter().all(|e| e["kind"] != "error"),
        "no error events expected"
    );
}

// ── Block-Fetch adversarial server scenarios ──────────────────────────────────
//
// Each test pairs a scripted adversarial server harness with the simple client
// scenario in scenarios/client_block_fetch_one_range.json. The server is
// spawned as a background tokio task; after a 50 ms sleep the client runs.
// A 10-second timeout on the server join catches hangs.
//
// Run all: cargo test --test live_node -- --ignored block_fetch_adversarial
//
// These tests do not require the devnet — they create their own server on a
// dedicated port (3010-3015). They do require those ports to be free, which is
// why they are marked #[ignore].

/// Load a scenario file relative to the crate root and override its trace path.
fn load_adversarial_scenario(relative_path: &str, trace_file: &NamedTempFile) -> Scenario {
    let mut s = cardano_conformance_harness::scenario::load(
        std::path::Path::new(relative_path)
    ).expect("adversarial scenario file must parse and validate");
    s.trace_output_path = trace_file.path().to_path_buf();
    s
}

// ScenarioRunner::run() returns Pin<Box<dyn Future>> which is !Send, so tokio::spawn
// can't be used here. Run the server in a LocalSet so spawn_local is available.
//
// Returns (client_trace_events, timed_out).
// `timed_out = true` means the 10-second wall-clock budget expired before both
// the client and server scenarios completed. The caller decides whether that is
// an acceptable conformance finding (e.g. mid_batch_disconnect, where Pallas may
// hang rather than error cleanly) or a hard test failure (the other five).
async fn run_adversarial_pair(server_path: &str, client_port: u16) -> (Vec<Value>, bool) {
    let server_trace = NamedTempFile::new().unwrap();
    let client_trace = NamedTempFile::new().unwrap();

    let server_scenario = load_adversarial_scenario(server_path, &server_trace);
    let mut client_scenario = load_adversarial_scenario(
        "scenarios/client_block_fetch_one_range.json", &client_trace
    );
    client_scenario.target_address = Some(format!("localhost:{client_port}"));

    // The timeout wraps the ENTIRE pair — both the client run (which can hang if
    // Pallas's client doesn't detect the disconnect) and the server run.
    let local = tokio::task::LocalSet::new();
    let timed_out = tokio::time::timeout(
        Duration::from_secs(10),
        local.run_until(async move {
            let server_handle = tokio::task::spawn_local(
                ScenarioRunner::new(server_scenario).run()
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
            let _ = ScenarioRunner::new(client_scenario).run().await;
            let _ = server_handle.await;
        }),
    )
    .await
    .is_err(); // Ok(()) = completed; Err(Elapsed) = timeout

    (read_trace(&client_trace), timed_out)
}

#[tokio::test]
#[ignore = "requires free TCP port 3010; run with: cargo test --test live_node -- --ignored block_fetch_adversarial"]
async fn block_fetch_adversarial_mid_batch_disconnect() {
    let (events, timed_out) = run_adversarial_pair(
        "scenarios/block_fetch_mid_batch_disconnect.json", 3010
    ).await;

    // Both outcomes are valid conformance findings:
    //   - timed_out=true: Pallas's client hung (didn't detect the disconnect) — itself noteworthy.
    //   - timed_out=false: Pallas detected the disconnect and surfaced an error cleanly.
    // Either way the test passes; what matters is which branch fires (document in results.md).
    if timed_out {
        // Pallas did not recover within 10 s — conformance finding: client hangs on mid-stream drop.
        return;
    }
    let summary = events.iter().find(|e| e["kind"] == "block_fetch_session_summary");
    assert!(summary.is_some(), "client trace must contain block_fetch_session_summary");
    let p = &summary.unwrap()["payload"];
    assert_ne!(p["exit_reason"], "completed",
        "mid-batch disconnect must not complete normally; got: {}", p["exit_reason"]);
}

#[tokio::test]
#[ignore = "requires free TCP port 3011; run with: cargo test --test live_node -- --ignored block_fetch_adversarial"]
async fn block_fetch_adversarial_block_outside_batch() {
    let (events, timed_out) = run_adversarial_pair(
        "scenarios/block_fetch_block_outside_batch.json", 3011
    ).await;
    assert!(!timed_out, "out-of-state Block should cause a fast client error, not a hang");

    let summary = events.iter().find(|e| e["kind"] == "block_fetch_session_summary");
    assert!(summary.is_some(), "client trace must contain block_fetch_session_summary");
    let p = &summary.unwrap()["payload"];
    assert_ne!(p["exit_reason"], "completed",
        "out-of-state Block must not produce a clean completion; got: {}", p["exit_reason"]);
    assert_eq!(p["blocks_received"], 0,
        "no blocks should be accepted before StartBatch");
}

#[tokio::test]
#[ignore = "requires free TCP port 3012; run with: cargo test --test live_node -- --ignored block_fetch_adversarial"]
async fn block_fetch_adversarial_batch_done_without_start() {
    let (events, timed_out) = run_adversarial_pair(
        "scenarios/block_fetch_batch_done_without_start.json", 3012
    ).await;
    assert!(!timed_out, "out-of-state BatchDone should cause a fast client error, not a hang");

    let summary = events.iter().find(|e| e["kind"] == "block_fetch_session_summary");
    assert!(summary.is_some(), "client trace must contain block_fetch_session_summary");
    let p = &summary.unwrap()["payload"];
    assert_ne!(p["exit_reason"], "completed",
        "out-of-state BatchDone must not produce a clean completion; got: {}", p["exit_reason"]);
    assert_eq!(p["blocks_received"], 0,
        "no blocks should be delivered for an empty batch starting with BatchDone");
}

#[tokio::test]
#[ignore = "requires free TCP port 3013 and fixtures/devnet_blocks.jsonl with ≥10 entries; run with: cargo test --test live_node -- --ignored block_fetch_adversarial"]
async fn block_fetch_adversarial_excessive_blocks() {
    let (events, timed_out) = run_adversarial_pair(
        "scenarios/block_fetch_excessive_blocks.json", 3013
    ).await;
    assert!(!timed_out, "excessive blocks should complete (or error), not hang");

    let summary = events.iter().find(|e| e["kind"] == "block_fetch_session_summary");
    assert!(summary.is_some(), "client trace must contain block_fetch_session_summary");
    let p = &summary.unwrap()["payload"];
    // Outcome is implementation-dependent — record what Pallas actually does.
    // The assertion just verifies the summary field is present and numeric.
    assert!(p["blocks_received"].is_number(),
        "blocks_received must be present in session summary");
    // Document: p["exit_reason"] tells us whether Pallas accepted or rejected extras.
}

#[tokio::test]
#[ignore = "requires free TCP port 3014; run with: cargo test --test live_node -- --ignored block_fetch_adversarial"]
async fn block_fetch_adversarial_malformed_block() {
    let (events, timed_out) = run_adversarial_pair(
        "scenarios/block_fetch_malformed_block.json", 3014
    ).await;
    assert!(!timed_out, "malformed CBOR should cause a fast decode error, not a hang");

    let summary = events.iter().find(|e| e["kind"] == "block_fetch_session_summary");
    assert!(summary.is_some(), "client trace must contain block_fetch_session_summary");
    let p = &summary.unwrap()["payload"];
    assert_ne!(p["exit_reason"], "completed",
        "malformed CBOR must not produce a clean completion; got: {}", p["exit_reason"]);
}

#[tokio::test]
#[ignore = "requires free TCP port 3015; run with: cargo test --test live_node -- --ignored block_fetch_adversarial"]
async fn block_fetch_adversarial_no_blocks_after_start() {
    let (events, timed_out) = run_adversarial_pair(
        "scenarios/block_fetch_no_blocks_after_start.json", 3015
    ).await;
    assert!(!timed_out, "NoBlocks after StartBatch should cause a fast client error, not a hang");

    let summary = events.iter().find(|e| e["kind"] == "block_fetch_session_summary");
    assert!(summary.is_some(), "client trace must contain block_fetch_session_summary");
    let p = &summary.unwrap()["payload"];
    assert_ne!(p["exit_reason"], "completed",
        "NoBlocks after StartBatch must not complete normally; got: {}", p["exit_reason"]);
    assert_eq!(p["blocks_received"], 0,
        "no blocks should be delivered when NoBlocks follows StartBatch");
}

// ── Network declaration integration tests ─────────────────────────────────────
//
// Require free TCP ports (3020, 3021) and fixtures/devnet_genesis.jsonl.
// Do NOT require the devnet — the harness serves its own connections.
// Run with: cargo test --test live_node -- --ignored network_declaration

/// Run one server scenario and connect one chain-sync client requesting `count` headers.
/// Returns the server's trace events.
async fn run_server_with_chain_sync_client_n(
    server_path: &str,
    port: u16,
    count: u64,
) -> Vec<Value> {
    let server_trace = NamedTempFile::new().unwrap();
    let mut server_scenario = cardano_conformance_harness::scenario::load(
        std::path::Path::new(server_path)
    ).expect("scenario must parse");
    server_scenario.trace_output_path = server_trace.path().to_path_buf();

    let local = tokio::task::LocalSet::new();
    let _ = tokio::time::timeout(Duration::from_secs(15), local.run_until(async move {
        let server_handle = tokio::task::spawn_local(
            ScenarioRunner::new(server_scenario).run()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;

        let addr = format!("localhost:{port}");
        let bearer = Bearer::connect_tcp(addr.as_str()).await.unwrap();
        let mut plexer = Plexer::new(bearer);
        let hs_ch = plexer.subscribe_client(PROTOCOL_N2N_HANDSHAKE);
        let cs_ch = plexer.subscribe_client(CHAIN_SYNC_PROTOCOL);
        let _ka = plexer.subscribe_client(pallas_network::miniprotocols::PROTOCOL_N2N_KEEP_ALIVE);
        plexer.spawn();

        let tmp = NamedTempFile::new().unwrap();
        let tracer = Tracer::open(tmp.path()).await.unwrap();
        let _ = handshake_on_channel(hs_ch, cardano_conformance_harness::DEVNET_MAGIC, &tracer).await;
        let _ = run_chain_sync(cs_ch, vec![pallas_network::miniprotocols::Point::Origin],
            count, Duration::from_secs(8), &tracer).await;
        let _ = server_handle.await;
    })).await;

    read_trace(&server_trace)
}

/// Convenience wrapper that requests 3 headers — used by network-declaration tests.
async fn run_server_with_chain_sync_client(server_path: &str, port: u16) -> Vec<Value> {
    run_server_with_chain_sync_client_n(server_path, port, 3).await
}

/// Encode a `u64` as CBOR unsigned integer (minimal form).
fn cbor_uint(v: u64) -> Vec<u8> {
    if v <= 23         { vec![v as u8] }
    else if v <= 0xFF  { vec![0x18, v as u8] }
    else if v <= 0xFFFF { vec![0x19, (v >> 8) as u8, (v & 0xFF) as u8] }
    else { vec![0x1a, (v>>24) as u8, ((v>>16)&0xFF) as u8, ((v>>8)&0xFF) as u8, (v&0xFF) as u8] }
}

/// Write a synthetic chain-sync JSONL fixture with N entries at evenly-spaced slots.
/// Each entry contains minimal but parseable CBOR: `array(2)[array(2)[bn, slot], null]`,
/// which is exactly the shape `extract_header_fields` expects (outer array, inner array
/// starting with block_number then slot, then anything for the signature).
fn write_synthetic_chain_fixture(path: &std::path::Path, slot_step: u64, count: u64) {
    use std::io::Write;
    let mut f = std::fs::File::create(path).unwrap();
    writeln!(f, r#"{{"anchor":true}}"#).unwrap();
    for bn in 0..count {
        let slot = (bn + 1) * slot_step;
        let bn_bytes  = cbor_uint(bn);
        let sl_bytes  = cbor_uint(slot);
        let mut cbor  = vec![0x82u8, 0x82];  // array(2), array(2)
        cbor.extend_from_slice(&bn_bytes);
        cbor.extend_from_slice(&sl_bytes);
        cbor.push(0xf6);                      // null (signature placeholder)
        let cbor_hex: String = cbor.iter().map(|b| format!("{b:02x}")).collect();
        let hash_hex = format!("{slot:064x}");
        writeln!(f,
            r#"{{"slot":{slot},"block_hash":"{hash_hex}","block_number":{bn},"cbor_hex":"{cbor_hex}","variant":6}}"#
        ).unwrap();
    }
}

#[tokio::test]
#[ignore = "requires free TCP port 3021 and fixtures/devnet_genesis.jsonl; run with: cargo test --test live_node -- --ignored network_declaration"]
async fn network_declaration_as_peer_overrides_accept_handshake_peer_id() {
    // accept_handshake sets peer_id "original_id"; serve_chain_sync uses
    // as_peer "declared_peer". All chain-sync wire events must carry
    // peer_id "declared_peer" — confirming the override takes effect.
    let events = run_server_with_chain_sync_client(
        "scenarios/as_peer_overrides_peer_id.json", 3021
    ).await;

    let cs_wire: Vec<&Value> = events.iter()
        .filter(|e| e["mini_protocol"] == "chain-sync"
               && matches!(e["direction"].as_str(), Some("sent") | Some("received")))
        .collect();
    assert!(!cs_wire.is_empty(), "expected chain-sync wire events in server trace");

    for e in &cs_wire {
        assert_eq!(e["peer_id"], "declared_peer",
            "wire event must be attributed to 'declared_peer' (as_peer override), not 'original_id': {e}");
    }
    assert!(
        cs_wire.iter().all(|e| e["peer_id"] != "original_id"),
        "no chain-sync wire events should carry 'original_id' after as_peer override"
    );
}

#[tokio::test]
#[ignore = "requires free TCP port 3020 and fixtures/devnet_genesis.jsonl; run with: cargo test --test live_node -- --ignored network_declaration"]
async fn network_declaration_emits_network_declared_trace_event() {
    // Connect one client to the two_peers scenario (the second parallel branch
    // will time out — that's fine). Verify the trace contains a network_declared
    // event listing both peers with correct ids.
    let events = run_server_with_chain_sync_client(
        "scenarios/two_peers_different_chains.json", 3020
    ).await;

    let nd = events.iter().find(|e| e["kind"] == "network_declared");
    assert!(nd.is_some(), "trace must contain network_declared event");
    let peers = nd.unwrap()["payload"]["peers"].as_array().unwrap();
    assert_eq!(peers.len(), 2);
    let ids: Vec<&str> = peers.iter().map(|p| p["id"].as_str().unwrap()).collect();
    assert!(ids.contains(&"honest_peer"));
    assert!(ids.contains(&"adversary"));
}

#[tokio::test]
#[ignore = "requires free TCP port 3022 and fixtures/devnet_genesis.jsonl; run with: cargo test --test live_node -- --ignored network_declaration"]
async fn network_with_time_slot_context_on_wire_events() {
    // The network_with_time scenario starts at slot 100, advances to 200 via
    // advance_to_slot, then ticks 50 more to reach slot 250. After those two
    // slot steps, it serves chain-sync. Every wire event should carry slot: 250.
    let events = run_server_with_chain_sync_client(
        "scenarios/network_with_time.json", 3022
    ).await;

    // Two SlotAdvanced events must appear.
    let slot_events: Vec<&Value> = events.iter()
        .filter(|e| e["kind"] == "slot_advanced")
        .collect();
    assert_eq!(slot_events.len(), 2, "expected two slot_advanced events");
    assert_eq!(slot_events[0]["payload"]["from_slot"], 100);
    assert_eq!(slot_events[0]["payload"]["to_slot"],   200);
    assert_eq!(slot_events[0]["payload"]["reason"], "advance_to_slot");
    assert_eq!(slot_events[1]["payload"]["from_slot"], 200);
    assert_eq!(slot_events[1]["payload"]["to_slot"],   250);
    assert_eq!(slot_events[1]["payload"]["reason"], "tick_slots");

    // All chain-sync wire events (emitted at slot 250) must carry slot: 250.
    let cs_wire: Vec<&Value> = events.iter()
        .filter(|e| e["mini_protocol"] == "chain-sync"
               && matches!(e["direction"].as_str(), Some("sent") | Some("received")))
        .collect();
    assert!(!cs_wire.is_empty(), "expected chain-sync wire events");
    for e in &cs_wire {
        assert_eq!(e["slot"], 250,
            "chain-sync wire event must carry slot 250: {e}");
    }
}

// ── Peer state (slice 3) integration tests ────────────────────────────────────

#[tokio::test]
#[ignore = "requires free TCP port 3023 and fixtures/devnet_genesis.jsonl; run with: cargo test --test live_node -- --ignored peer_state"]
async fn peer_runtime_extension_serves_21_headers() {
    // Server serves a 20-entry fixture peer extended at runtime to 21 entries.
    // Client requests all 21; server must send all 21 roll_forward messages.
    let events = run_server_with_chain_sync_client_n(
        "scenarios/peer_runtime_extension.json", 3023, 21
    ).await;

    assert!(
        events.iter().any(|e| e["kind"] == "peer_chain_extended"),
        "trace must contain peer_chain_extended event"
    );
    let rf = events.iter()
        .filter(|e| e["kind"] == "chain_sync_roll_forward" && e["direction"] == "sent")
        .count();
    assert_eq!(rf, 21,
        "server must have sent 21 roll_forward messages (20 from fixture + 1 runtime extension)");
}

#[tokio::test]
#[ignore = "requires free TCP port 3024; run with: cargo test --test live_node -- --ignored peer_state"]
async fn slot_filtered_serving_exposes_only_visible_entries() {
    // Constructed fixture: 10 entries at slots 100, 200, 300, …, 1000 (step 100).
    // Advance to slot 550: visible = {100, 200, 300, 400, 500} = 5 entries.
    // Client requests exactly 5; server sends 5 then AwaitReply.
    let tmp_fixture = NamedTempFile::new().unwrap();
    write_synthetic_chain_fixture(tmp_fixture.path(), 100, 10);

    // Load the slot_filtered scenario and point it at the synthetic fixture.
    let mut scenario = cardano_conformance_harness::scenario::load(
        std::path::Path::new("scenarios/slot_filtered_serving.json")
    ).expect("slot_filtered_serving.json must parse");
    scenario.network.as_mut().unwrap().peers[0].chain_sync_fixture =
        Some(tmp_fixture.path().to_string_lossy().into_owned());
    // Override the advance_to_slot target by modifying the step's raw_params.
    // The scenario has steps[0] = advance_to_slot. Override slot → 550.
    if let serde_json::Value::Object(ref mut m) = scenario.steps[0].raw_params {
        m.insert("slot".into(), serde_json::json!(550u64));
    }
    // Rebind to a temp trace file and run.
    let server_trace = NamedTempFile::new().unwrap();
    scenario.trace_output_path = server_trace.path().to_path_buf();
    // Port 3024 is declared in the scenario's listen step — no change needed.

    let local = tokio::task::LocalSet::new();
    let _ = tokio::time::timeout(Duration::from_secs(15), local.run_until(async move {
        let server_handle = tokio::task::spawn_local(
            ScenarioRunner::new(scenario).run()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;

        let bearer = Bearer::connect_tcp("localhost:3024").await.unwrap();
        let mut plexer = Plexer::new(bearer);
        let hs_ch = plexer.subscribe_client(PROTOCOL_N2N_HANDSHAKE);
        let cs_ch = plexer.subscribe_client(CHAIN_SYNC_PROTOCOL);
        let _ka = plexer.subscribe_client(pallas_network::miniprotocols::PROTOCOL_N2N_KEEP_ALIVE);
        plexer.spawn();
        let tmp = NamedTempFile::new().unwrap();
        let tracer = Tracer::open(tmp.path()).await.unwrap();
        let _ = handshake_on_channel(hs_ch, cardano_conformance_harness::DEVNET_MAGIC, &tracer).await;
        // 5 entries visible — request exactly 5.
        let _ = run_chain_sync(cs_ch, vec![pallas_network::miniprotocols::Point::Origin],
            5, Duration::from_secs(8), &tracer).await;
        let _ = server_handle.await;
    })).await;

    let events = read_trace(&server_trace);

    let sa = events.iter().find(|e| e["kind"] == "slot_advanced");
    assert!(sa.is_some(), "slot_advanced event must appear");
    assert_eq!(sa.unwrap()["payload"]["to_slot"], 550u64);

    let rf = events.iter()
        .filter(|e| e["kind"] == "chain_sync_roll_forward" && e["direction"] == "sent")
        .count();
    assert_eq!(rf, 5,
        "server must send exactly 5 headers (entries at slots 100-500, visible at slot 550)");
}

// ── Automatic production (slice 4) integration tests ─────────────────────────

#[tokio::test]
#[ignore = "requires free TCP port 3025; run with: cargo test --test live_node -- --ignored auto_production"]
async fn auto_production_every_5_slots_serves_7_headers() {
    // Network starts at slot 99, rule fires at 100..130 step 5 → 7 blocks.
    // Client requests 7 headers; server must send all 7.
    let events = run_server_with_chain_sync_client_n(
        "scenarios/auto_production_every_5_slots.json", 3025, 7
    ).await;

    // 7 production_rule_fired events (all non-skipped)
    let fired: Vec<&Value> = events.iter()
        .filter(|e| e["kind"] == "production_rule_fired" && e["payload"]["skipped"] == false)
        .collect();
    assert_eq!(fired.len(), 7,
        "expected 7 production_rule_fired events (slots 100,105,…,130)");
    assert_eq!(fired[0]["payload"]["slot"], 100u64);
    assert_eq!(fired[6]["payload"]["slot"], 130u64);

    // 7 peer_chain_extended events with source: production_rule
    let extended: Vec<&Value> = events.iter()
        .filter(|e| e["kind"] == "peer_chain_extended"
               && e["payload"]["source"] == "production_rule")
        .collect();
    assert_eq!(extended.len(), 7);

    // 7 roll_forward events sent to the client
    let rf = events.iter()
        .filter(|e| e["kind"] == "chain_sync_roll_forward" && e["direction"] == "sent")
        .count();
    assert_eq!(rf, 7, "server must serve all 7 produced blocks");
}

#[tokio::test]
#[ignore = "requires free TCP port 3026; run with: cargo test --test live_node -- --ignored auto_production"]
async fn auto_production_two_peers_different_rules() {
    // steady_peer: every 5 slots from 100 → 5 blocks at {100,105,110,115,120}
    // sparse_peer: at_slots [102,107,117] → 3 blocks
    // Two clients connect in parallel; each sees its peer's production.
    let events = run_server_with_chain_sync_client_n(
        "scenarios/auto_production_two_peers_different_rules.json", 3026, 5
    ).await;

    let steady_fired: Vec<&Value> = events.iter()
        .filter(|e| e["kind"] == "production_rule_fired"
               && e["payload"]["peer_id"] == "steady_peer"
               && e["payload"]["skipped"] == false)
        .collect();
    assert_eq!(steady_fired.len(), 5, "steady_peer must produce 5 blocks");

    let sparse_fired: Vec<&Value> = events.iter()
        .filter(|e| e["kind"] == "production_rule_fired"
               && e["payload"]["peer_id"] == "sparse_peer"
               && e["payload"]["skipped"] == false)
        .collect();
    assert_eq!(sparse_fired.len(), 3, "sparse_peer must produce 3 blocks at [102,107,117]");
}

// ── Adversarial production (slice 5) integration tests ────────────────────────

#[tokio::test]
#[ignore = "requires free TCP port 3027; run with: cargo test --test live_node -- --ignored adversarial_production"]
async fn conflicting_forks_chains_agree_pre_fork_diverge_post_fork() {
    // peer_a and peer_b share first_slot/interval/fork_slot but have different
    // fork_markers. The server trace carries peer_chain_extended events for both.
    // Pre-fork blocks (slots 100, 105, 110) must have matching hashes.
    // Post-fork blocks (slots 115+) must have different hashes for the two peers.
    let events = run_server_with_chain_sync_client_n(
        "scenarios/conflicting_forks.json", 3027, 7
    ).await;

    let chain_a: Vec<&Value> = events.iter()
        .filter(|e| e["kind"] == "peer_chain_extended" && e["payload"]["peer_id"] == "peer_a")
        .collect();
    let chain_b: Vec<&Value> = events.iter()
        .filter(|e| e["kind"] == "peer_chain_extended" && e["payload"]["peer_id"] == "peer_b")
        .collect();
    assert_eq!(chain_a.len(), 7, "peer_a must produce 7 blocks (slots 100-130 step 5)");
    assert_eq!(chain_b.len(), 7, "peer_b must produce 7 blocks");

    // Slots 100, 105, 110 are pre-fork — hashes must be identical.
    for i in 0..3 {
        assert_eq!(chain_a[i]["payload"]["block_hash"], chain_b[i]["payload"]["block_hash"],
            "pre-fork block {} hashes must match", i);
        assert_eq!(chain_a[i]["payload"]["defect_kind"], Value::Null,
            "pre-fork blocks must have no defect_kind");
    }
    // Slots 115+ are post-fork — hashes must diverge.
    for i in 3..7 {
        assert_ne!(chain_a[i]["payload"]["block_hash"], chain_b[i]["payload"]["block_hash"],
            "post-fork block {} hashes must diverge", i);
        assert_eq!(chain_a[i]["payload"]["defect_kind"], "fork_divergence");
        assert_eq!(chain_b[i]["payload"]["defect_kind"], "fork_divergence");
    }
}

#[tokio::test]
#[ignore = "requires free TCP port 3028; run with: cargo test --test live_node -- --ignored adversarial_production"]
async fn sparse_chain_has_slot_gaps_but_sequential_block_numbers() {
    // skips_slots rule skips indices 1 (slot 105) and 3 (slot 115).
    // Produced blocks: slots 100, 110, 120, 125, 130 = 5 blocks.
    let events = run_server_with_chain_sync_client_n(
        "scenarios/sparse_chain.json", 3028, 5
    ).await;

    let extended: Vec<&Value> = events.iter()
        .filter(|e| e["kind"] == "peer_chain_extended")
        .collect();
    assert_eq!(extended.len(), 5, "5 blocks must be produced (2 slots skipped)");

    // No block at slot 105 or 115.
    let slots: Vec<u64> = extended.iter()
        .map(|e| e["payload"]["slot"].as_u64().unwrap())
        .collect();
    assert!(!slots.contains(&105), "slot 105 must be skipped");
    assert!(!slots.contains(&115), "slot 115 must be skipped");

    // Block numbers are sequential 0-4.
    let bns: Vec<u64> = extended.iter()
        .map(|e| e["payload"]["block_number"].as_u64().unwrap())
        .collect();
    assert_eq!(bns, vec![0, 1, 2, 3, 4], "block_numbers must be sequential");

    // All produced blocks carry defect_kind: "sparse_chain".
    assert!(extended.iter().all(|e| e["payload"]["defect_kind"] == "sparse_chain"));

    // Server sent 5 roll_forward messages.
    let rf = events.iter()
        .filter(|e| e["kind"] == "chain_sync_roll_forward" && e["direction"] == "sent")
        .count();
    assert_eq!(rf, 5);
}

#[tokio::test]
#[ignore = "requires free TCP port 3029; run with: cargo test --test live_node -- --ignored adversarial_production"]
async fn broken_chain_integrity_defect_kind_present_from_break_slot() {
    // broken_prev_hash rule breaks at slot 115.
    // Blocks before 115: defect_kind absent (honest). Blocks at/after 115: defect_kind = "broken_prev_hash".
    // The server produces 7 blocks (100, 105, ..., 130). Client behavior is a conformance finding.
    let events = run_server_with_chain_sync_client_n(
        "scenarios/broken_chain_integrity.json", 3029, 7
    ).await;

    let extended: Vec<&Value> = events.iter()
        .filter(|e| e["kind"] == "peer_chain_extended")
        .collect();
    assert_eq!(extended.len(), 7, "7 blocks must be produced");

    // First 3 blocks (slots 100, 105, 110) are pre-break: no defect.
    for e in &extended[..3] {
        assert_eq!(e["payload"]["defect_kind"], Value::Null,
            "pre-break blocks must have no defect_kind");
    }
    // Remaining blocks (slots 115-130) are post-break: defect present.
    for e in &extended[3..] {
        assert_eq!(e["payload"]["defect_kind"], "broken_prev_hash",
            "post-break blocks must carry defect_kind: broken_prev_hash");
    }

    // How many roll_forwards the client accepted is the conformance finding.
    let rf = events.iter()
        .filter(|e| e["kind"] == "chain_sync_roll_forward" && e["direction"] == "sent")
        .count();
    // Accept any count: 0 (client errors immediately), 3 (errors at break),
    // or 7 (client doesn't validate hashes). Record what Pallas does.
    eprintln!("broken_chain_integrity: Pallas accepted {rf} roll_forwards before the chain_sync ended");
    assert!(rf <= 7, "client cannot receive more than 7 headers");
}

