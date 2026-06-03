use std::time::Duration;

use cardano_conformance_harness::miniprotocols::blockfetch::{run_block_fetch, BLOCK_FETCH_PROTOCOL};
use cardano_conformance_harness::miniprotocols::chainsync::{run_chain_sync, CHAIN_SYNC_PROTOCOL};
use cardano_conformance_harness::miniprotocols::handshake::{handshake_on_channel, run_handshake};
use cardano_conformance_harness::trace::Tracer;
use cardano_conformance_harness::DEVNET_MAGIC;
use pallas_network::miniprotocols::PROTOCOL_N2N_HANDSHAKE;
use pallas_network::multiplexer::{Bearer, Plexer};
use serde_json::Value;
use tempfile::NamedTempFile;

const DEVNET_ADDR: &str = "localhost:3001";
const AWAIT_TIMEOUT: Duration = Duration::from_secs(30);

/// Opens one TCP connection, runs handshake + chain-sync for `count` headers.
/// Returns the trace file and the negotiated version.
async fn run_session(count: u64) -> (NamedTempFile, u64) {
    let tmp = NamedTempFile::new().unwrap();
    let mut tracer = Tracer::open(tmp.path()).await.unwrap();

    let bearer = Bearer::connect_tcp(DEVNET_ADDR).await.unwrap();
    let mut plexer = Plexer::new(bearer);
    let hs_channel = plexer.subscribe_client(PROTOCOL_N2N_HANDSHAKE);
    let cs_channel = plexer.subscribe_client(CHAIN_SYNC_PROTOCOL);
    let plexer_handle = plexer.spawn();

    let version = handshake_on_channel(hs_channel, DEVNET_MAGIC, &mut tracer)
        .await
        .expect("handshake should succeed against devnet");

    let summary = run_chain_sync(cs_channel, vec![pallas_network::miniprotocols::Point::Origin], count, AWAIT_TIMEOUT, &mut tracer)
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
    let mut tracer = Tracer::open(tmp.path()).await.unwrap();

    let bearer = Bearer::connect_tcp(DEVNET_ADDR).await.unwrap();
    let mut plexer = Plexer::new(bearer);
    let hs_channel = plexer.subscribe_client(PROTOCOL_N2N_HANDSHAKE);
    let cs_channel = plexer.subscribe_client(CHAIN_SYNC_PROTOCOL);
    let bf_channel = plexer.subscribe_client(BLOCK_FETCH_PROTOCOL);
    let plexer_handle = plexer.spawn();

    handshake_on_channel(hs_channel, DEVNET_MAGIC, &mut tracer)
        .await
        .expect("handshake should succeed");

    let cs_summary = run_chain_sync(cs_channel, vec![pallas_network::miniprotocols::Point::Origin], count, AWAIT_TIMEOUT, &mut tracer)
        .await
        .expect("chain-sync should succeed");

    assert_eq!(cs_summary.headers_received, count);
    assert_eq!(
        cs_summary.collected_points.len() as u64,
        count,
        "collected_points count should equal headers_received"
    );

    run_block_fetch(bf_channel, cs_summary.collected_points, 1, &mut tracer)
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
    let mut tracer = Tracer::open(tmp.path()).await.unwrap();

    let version = run_handshake(DEVNET_ADDR, DEVNET_MAGIC, &mut tracer)
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
    let mut tracer = Tracer::open(tmp.path()).await.unwrap();

    let result = run_handshake(DEVNET_ADDR, 999_999, &mut tracer).await;

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
