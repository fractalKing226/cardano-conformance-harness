use std::time::Duration;

use cardano_conformance_harness::miniprotocols::blockfetch::{run_block_fetch, BLOCK_FETCH_PROTOCOL};
use cardano_conformance_harness::miniprotocols::chainsync::{run_chain_sync, CHAIN_SYNC_PROTOCOL};
use cardano_conformance_harness::miniprotocols::handshake::{handshake_on_channel, run_handshake};
use cardano_conformance_harness::miniprotocols::keepalive::{run_keepalive, KEEP_ALIVE_PROTOCOL};
use cardano_conformance_harness::miniprotocols::txsubmission::{run_tx_submission, TX_SUBMISSION_PROTOCOL};
use cardano_conformance_harness::scenario::runner::ScenarioRunner;
use cardano_conformance_harness::scenario::{Assertions, Scenario, StepDef, StepKind, StepParams};
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
        target_address: DEVNET_ADDR.to_string(),
        network_magic: DEVNET_MAGIC,
        trace_output_path: trace_path.to_path_buf(),
        expected_outcome: None,
        steps,
    }
}

fn simple_step(kind: StepKind) -> StepDef {
    StepDef { kind, params: StepParams::default(), expect: None }
}

fn chain_sync_step(count: u64) -> StepDef {
    StepDef {
        kind: StepKind::ChainSync,
        params: StepParams { count: Some(count), ..Default::default() },
        expect: None,
    }
}

fn sleep_step(secs: u64) -> StepDef {
    StepDef {
        kind: StepKind::Sleep,
        params: StepParams { duration_secs: Some(secs), ..Default::default() },
        expect: None,
    }
}

fn step_with_expect(kind: StepKind, expect: Assertions) -> StepDef {
    StepDef { kind, params: StepParams::default(), expect: Some(expect) }
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
