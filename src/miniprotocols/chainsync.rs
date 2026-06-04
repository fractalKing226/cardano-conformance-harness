use std::time::{Duration, Instant};

use pallas_codec::minicbor;
use pallas_crypto::hash::Hasher;
use pallas_network::miniprotocols::chainsync::{HeaderContent, NextResponse, N2NClient};
use pallas_network::miniprotocols::Point;
use pallas_network::multiplexer::AgentChannel;
use serde_json::json;
use tokio::time::timeout;
use tracing::{debug, info, warn};

use crate::trace::{Direction, EventKind, TraceEvent, Tracer};

pub use pallas_network::miniprotocols::PROTOCOL_N2N_CHAIN_SYNC as CHAIN_SYNC_PROTOCOL;

const MINI_PROTOCOL: &str = "chain-sync";

/// A captured header as it arrives from a RollForward message.
/// Used to populate fixture files for server-side replay.
#[derive(Clone)]
pub struct CapturedHeader {
    pub slot: u64,
    pub block_hash: Vec<u8>,
    pub block_number: u64,
    pub cbor: Vec<u8>,
    /// Era variant byte from `HeaderContent.variant` (0=Byron, 6=Conway, …).
    pub variant: u8,
}

pub struct ChainSyncSummary {
    pub headers_received: u64,
    pub rollbacks: u64,
    pub must_reply_triggered: bool,
    pub messages_sent: u64,
    pub messages_received: u64,
    pub exit_reason: String,
    pub duration_ms: u64,
    /// Points (slot + block hash) collected from RollForward messages, in order.
    /// Used to drive Block-Fetch after Chain-Sync completes.
    pub collected_points: Vec<Point>,
    /// Full header captures for fixture file writing (`--capture-fixture`).
    pub captured_headers: Vec<CapturedHeader>,
}

fn encode_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

pub fn format_point(point: &Point) -> serde_json::Value {
    match point {
        Point::Origin => json!("origin"),
        Point::Specific(slot, hash) => json!({
            "slot": slot,
            "hash": encode_hex(hash),
        }),
    }
}

fn format_tip(tip: &pallas_network::miniprotocols::chainsync::Tip) -> serde_json::Value {
    let pallas_network::miniprotocols::chainsync::Tip(point, block_number) = tip;
    json!({
        "point": format_point(point),
        "block_number": block_number,
    })
}

fn state_str(client: &N2NClient) -> String {
    format!("{:?}", client.state())
}

/// Extracts (slot, block_number, block_hash) from a Shelley+ header.
///
/// The block hash is blake2b-256 of the raw header bytes. The block_number and
/// slot are decoded from the CBOR structure:
///   array(2)[array(N)[block_number, slot, ...], signature]
///
/// Byron headers (variant 0) are not supported here.
pub fn extract_header_fields(header: &HeaderContent) -> anyhow::Result<(u64, u64, Vec<u8>)> {
    let hash: Vec<u8> = Hasher::<256>::hash(&header.cbor).as_ref().to_vec();
    let mut d = minicbor::Decoder::new(&header.cbor);
    d.array()
        .map_err(|e| anyhow::anyhow!("header cbor: expected outer array: {e}"))?;
    d.array()
        .map_err(|e| anyhow::anyhow!("header cbor: expected inner array: {e}"))?;
    let block_number = d
        .u64()
        .map_err(|e| anyhow::anyhow!("header cbor: expected block_number u64: {e}"))?;
    let slot = d
        .u64()
        .map_err(|e| anyhow::anyhow!("header cbor: expected slot u64: {e}"))?;
    Ok((block_number, slot, hash))
}

#[allow(dead_code)]
fn extract_point(header: &HeaderContent) -> anyhow::Result<Point> {
    let (_, slot, hash) = extract_header_fields(header)?;
    Ok(Point::Specific(slot, hash))
}

/// Runs a Chain-Sync session on a pre-allocated multiplexer channel.
///
/// Sends `MsgFindIntersect` with the supplied `intersection_points` (use
/// `vec![Point::Origin]` to start from genesis), then consumes `count` block
/// headers via `MsgRequestNext`, logging every message in both directions.
/// Terminates cleanly with `MsgDone`.
/// Returns a summary that includes the collected Points for each RollForward,
/// which callers can pass directly to Block-Fetch.
pub async fn run_chain_sync(
    channel: AgentChannel,
    intersection_points: Vec<Point>,
    count: u64,
    await_timeout: Duration,
    tracer: &Tracer,
) -> anyhow::Result<ChainSyncSummary> {
    let started_at = Instant::now();
    let mut client = N2NClient::new(channel);

    let mut headers_received: u64 = 0;
    let mut rollbacks: u64 = 0;
    let mut must_reply_triggered = false;
    let mut messages_sent: u64 = 0;
    let mut messages_received: u64 = 0;
    let mut collected_points: Vec<Point> = Vec::new();
    let mut captured_headers: Vec<CapturedHeader> = Vec::new();

    tracer
        .emit(
            TraceEvent::new(
                EventKind::ChainSyncStarted,
                Direction::Internal,
                json!({ "target_headers": count }),
            )
            .with_protocol(MINI_PROTOCOL),
        )
        .await?;

    // FindIntersect
    let point_values: Vec<serde_json::Value> =
        intersection_points.iter().map(format_point).collect();
    let sb = state_str(&client);
    tracer
        .emit(
            TraceEvent::new(
                EventKind::ChainSyncFindIntersect,
                Direction::Sent,
                json!({ "points": point_values }),
            )
            .with_states(MINI_PROTOCOL, sb, "Intersect"),
        )
        .await?;
    messages_sent += 1;

    let intersect_result = match client.find_intersect(intersection_points).await {
        Ok(r) => r,
        Err(e) => {
            let msg = e.to_string();
            let _ = tracer
                .emit(
                    TraceEvent::new(
                        EventKind::Error,
                        Direction::Internal,
                        json!({ "phase": "find_intersect", "error": msg }),
                    )
                    .with_protocol(MINI_PROTOCOL),
                )
                .await;
            return Err(anyhow::anyhow!("find_intersect failed: {e}"));
        }
    };
    messages_received += 1;

    let sa = state_str(&client);
    match &intersect_result {
        (Some(point), tip) => {
            info!("Intersect found at {:?}", point);
            tracer
                .emit(
                    TraceEvent::new(
                        EventKind::ChainSyncIntersectFound,
                        Direction::Received,
                        json!({
                            "point": format_point(point),
                            "tip": format_tip(tip),
                        }),
                    )
                    .with_states(MINI_PROTOCOL, "Intersect", &sa),
                )
                .await?;
        }
        (None, tip) => {
            warn!("Intersect not found; tip is {:?}", tip.0);
            tracer
                .emit(
                    TraceEvent::new(
                        EventKind::ChainSyncIntersectNotFound,
                        Direction::Received,
                        json!({ "tip": format_tip(tip) }),
                    )
                    .with_states(MINI_PROTOCOL, "Intersect", &sa),
                )
                .await?;
        }
    }

    // Main request loop
    while headers_received < count {
        let sb = state_str(&client);
        tracer
            .emit(
                TraceEvent::new(
                    EventKind::ChainSyncRequestNext,
                    Direction::Sent,
                    json!({ "headers_received_so_far": headers_received }),
                )
                .with_states(MINI_PROTOCOL, sb, "CanAwait"),
            )
            .await?;
        messages_sent += 1;

        let response = match client.request_next().await {
            Ok(r) => r,
            Err(e) => {
                let msg = e.to_string();
                let _ = tracer
                    .emit(
                        TraceEvent::new(
                            EventKind::Error,
                            Direction::Internal,
                            json!({ "phase": "request_next", "error": msg }),
                        )
                        .with_protocol(MINI_PROTOCOL),
                    )
                    .await;
                let summary = build_summary(
                    headers_received,
                    rollbacks,
                    must_reply_triggered,
                    messages_sent,
                    messages_received,
                    collected_points,
                    captured_headers.clone(),
                    started_at,
                    "error",
                );
                emit_summary(tracer, &summary).await;
                return Err(anyhow::anyhow!("request_next failed: {e}"));
            }
        };

        match response {
            NextResponse::RollForward(header, tip) => {
                messages_received += 1;
                headers_received += 1;
                debug!(headers_received, "RollForward");
                match extract_header_fields(&header) {
                    Ok((block_number, slot, hash)) => {
                        collected_points.push(Point::Specific(slot, hash.clone()));
                        captured_headers.push(CapturedHeader {
                            slot,
                            block_hash: hash,
                            block_number,
                            cbor: header.cbor.clone(),
                            variant: header.variant,
                        });
                    }
                    Err(e) => warn!("Could not extract fields from header: {e}"),
                }
                tracer
                    .emit(
                        TraceEvent::new(
                            EventKind::ChainSyncRollForward,
                            Direction::Received,
                            roll_forward_payload(&header, &tip),
                        )
                        .with_states(MINI_PROTOCOL, "CanAwait", state_str(&client)),
                    )
                    .await?;
            }
            NextResponse::RollBackward(point, tip) => {
                messages_received += 1;
                rollbacks += 1;
                warn!("RollBackward to {:?}", point);
                tracer
                    .emit(
                        TraceEvent::new(
                            EventKind::ChainSyncRollBackward,
                            Direction::Received,
                            json!({
                                "rollback_to": format_point(&point),
                                "tip": format_tip(&tip),
                            }),
                        )
                        .with_states(MINI_PROTOCOL, "CanAwait", state_str(&client)),
                    )
                    .await?;
            }
            NextResponse::Await => {
                must_reply_triggered = true;
                info!("AwaitReply received; waiting up to {}s", await_timeout.as_secs());
                tracer
                    .emit(
                        TraceEvent::new(
                            EventKind::ChainSyncAwaitReply,
                            Direction::Received,
                            json!({ "timeout_secs": await_timeout.as_secs() }),
                        )
                        .with_states(MINI_PROTOCOL, "CanAwait", "MustReply"),
                    )
                    .await?;

                let actual = match timeout(await_timeout, client.recv_while_must_reply()).await {
                    Ok(Ok(r)) => r,
                    Ok(Err(e)) => {
                        let msg = e.to_string();
                        let _ = tracer
                            .emit(
                                TraceEvent::new(
                                    EventKind::Error,
                                    Direction::Internal,
                                    json!({ "phase": "recv_while_must_reply", "error": msg }),
                                )
                                .with_protocol(MINI_PROTOCOL),
                            )
                            .await;
                        let summary = build_summary(
                            headers_received,
                            rollbacks,
                            must_reply_triggered,
                            messages_sent,
                            messages_received,
                            collected_points,
                            captured_headers.clone(),
                            started_at,
                            "error",
                        );
                        emit_summary(tracer, &summary).await;
                        return Err(anyhow::anyhow!("recv_while_must_reply failed: {e}"));
                    }
                    Err(_elapsed) => {
                        let _ = tracer
                            .emit(
                                TraceEvent::new(
                                    EventKind::Error,
                                    Direction::Internal,
                                    json!({
                                        "phase": "recv_while_must_reply",
                                        "error": "timeout",
                                        "timeout_secs": await_timeout.as_secs(),
                                    }),
                                )
                                .with_protocol(MINI_PROTOCOL),
                            )
                            .await;
                        let summary = build_summary(
                            headers_received,
                            rollbacks,
                            must_reply_triggered,
                            messages_sent,
                            messages_received,
                            collected_points,
                            captured_headers.clone(),
                            started_at,
                            "timeout",
                        );
                        emit_summary(tracer, &summary).await;
                        return Err(anyhow::anyhow!(
                            "chain-sync await timed out after {}s",
                            await_timeout.as_secs()
                        ));
                    }
                };

                match actual {
                    NextResponse::RollForward(header, tip) => {
                        messages_received += 1;
                        headers_received += 1;
                        debug!(headers_received, "RollForward (after await)");
                        match extract_header_fields(&header) {
                            Ok((block_number, slot, hash)) => {
                                collected_points.push(Point::Specific(slot, hash.clone()));
                                captured_headers.push(CapturedHeader {
                                    slot,
                                    block_hash: hash,
                                    block_number,
                                    cbor: header.cbor.clone(),
                                    variant: header.variant,
                                });
                            }
                            Err(e) => warn!("Could not extract fields from header: {e}"),
                        }
                        tracer
                            .emit(
                                TraceEvent::new(
                                    EventKind::ChainSyncRollForward,
                                    Direction::Received,
                                    roll_forward_payload(&header, &tip),
                                )
                                .with_states(MINI_PROTOCOL, "MustReply", state_str(&client)),
                            )
                            .await?;
                    }
                    NextResponse::RollBackward(point, tip) => {
                        messages_received += 1;
                        rollbacks += 1;
                        tracer
                            .emit(
                                TraceEvent::new(
                                    EventKind::ChainSyncRollBackward,
                                    Direction::Received,
                                    json!({
                                        "rollback_to": format_point(&point),
                                        "tip": format_tip(&tip),
                                    }),
                                )
                                .with_states(MINI_PROTOCOL, "MustReply", state_str(&client)),
                            )
                            .await?;
                    }
                    NextResponse::Await => {
                        return Err(anyhow::anyhow!(
                            "unexpected Await response in MustReply state"
                        ));
                    }
                }
            }
        }
    }

    // Send MsgDone
    let sb = state_str(&client);
    if let Err(e) = client.send_done().await {
        let _ = tracer
            .emit(
                TraceEvent::new(
                    EventKind::Error,
                    Direction::Internal,
                    json!({ "phase": "send_done", "error": e.to_string() }),
                )
                .with_protocol(MINI_PROTOCOL),
            )
            .await;
    } else {
        messages_sent += 1;
        tracer
            .emit(
                TraceEvent::new(EventKind::ChainSyncDone, Direction::Sent, json!({}))
                    .with_states(MINI_PROTOCOL, sb, "Done"),
            )
            .await?;
    }

    let summary = build_summary(
        headers_received,
        rollbacks,
        must_reply_triggered,
        messages_sent,
        messages_received,
        collected_points,
        captured_headers.clone(),
        started_at,
        "completed",
    );
    emit_summary(tracer, &summary).await;

    info!(
        headers_received = summary.headers_received,
        duration_ms = summary.duration_ms,
        "Chain-sync session complete"
    );

    Ok(summary)
}

fn roll_forward_payload(
    header: &HeaderContent,
    tip: &pallas_network::miniprotocols::chainsync::Tip,
) -> serde_json::Value {
    json!({
        "variant": header.variant,
        "cbor_hex": encode_hex(&header.cbor),
        "cbor_len": header.cbor.len(),
        "tip": format_tip(tip),
    })
}

#[allow(clippy::too_many_arguments)]
fn build_summary(
    headers_received: u64,
    rollbacks: u64,
    must_reply_triggered: bool,
    messages_sent: u64,
    messages_received: u64,
    collected_points: Vec<Point>,
    captured_headers: Vec<CapturedHeader>,
    started_at: Instant,
    exit_reason: &str,
) -> ChainSyncSummary {
    ChainSyncSummary {
        headers_received,
        rollbacks,
        must_reply_triggered,
        messages_sent,
        messages_received,
        exit_reason: exit_reason.to_string(),
        duration_ms: started_at.elapsed().as_millis() as u64,
        collected_points,
        captured_headers,
    }
}

async fn emit_summary(tracer: &Tracer, summary: &ChainSyncSummary) {
    let _ = tracer
        .emit(
            TraceEvent::new(
                EventKind::ChainSyncSessionSummary,
                Direction::Internal,
                json!({
                    "headers_received":     summary.headers_received,
                    "rollbacks":            summary.rollbacks,
                    "must_reply_triggered": summary.must_reply_triggered,
                    "messages_sent":        summary.messages_sent,
                    "messages_received":    summary.messages_received,
                    "exit_reason":          summary.exit_reason,
                    "duration_ms":          summary.duration_ms,
                    "collected_points":     summary.collected_points.len(),
                }),
            )
            .with_protocol(MINI_PROTOCOL),
        )
        .await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_hex_roundtrip() {
        let bytes = vec![0xde, 0xad, 0xbe, 0xef];
        assert_eq!(encode_hex(&bytes), "deadbeef");
    }

    #[test]
    fn format_point_origin() {
        let v = format_point(&Point::Origin);
        assert_eq!(v, "origin");
    }

    #[test]
    fn format_point_specific() {
        let v = format_point(&Point::Specific(42, vec![0xab, 0xcd]));
        assert_eq!(v["slot"], 42);
        assert_eq!(v["hash"], "abcd");
    }

    #[test]
    fn roll_forward_payload_has_required_keys() {
        use pallas_network::miniprotocols::chainsync::Tip;

        let header = HeaderContent {
            variant: 6,
            byron_prefix: None,
            cbor: vec![0x01, 0x02, 0x03],
        };
        let tip = Tip(Point::Origin, 100);
        let payload = roll_forward_payload(&header, &tip);

        assert_eq!(payload["variant"], 6);
        assert_eq!(payload["cbor_hex"], "010203");
        assert_eq!(payload["cbor_len"], 3);
        assert!(payload["tip"].is_object());
    }

    #[test]
    fn extract_point_parses_slot_and_hashes() {
        // Minimal well-formed Conway header CBOR:
        // array(2)[ array(10)[block_number=0, slot=62, ...8 null placeholders], sig=null ]
        // We only need the outer structure to be valid enough for the decoder.
        let mut enc = minicbor::Encoder::new(vec![]);
        enc.array(2).unwrap();        // outer array(2)
        enc.array(10).unwrap();       // header fields array(10)
        enc.u64(0).unwrap();          // block_number
        enc.u64(62).unwrap();         // slot
        for _ in 0..8 { enc.null().unwrap(); } // remaining fields
        enc.null().unwrap();          // signature placeholder
        let cbor = enc.into_writer();

        let header = HeaderContent {
            variant: 6,
            byron_prefix: None,
            cbor: cbor.clone(),
        };

        let point = extract_point(&header).unwrap();
        match point {
            Point::Specific(slot, hash) => {
                assert_eq!(slot, 62);
                // hash = blake2b-256 of the cbor bytes
                let expected: Vec<u8> = Hasher::<256>::hash(&cbor).as_ref().to_vec();
                assert_eq!(hash, expected);
            }
            Point::Origin => panic!("expected Specific point"),
        }
    }

    #[test]
    fn build_summary_fields() {
        let summary = build_summary(5, 1, true, 12, 10, vec![], vec![], Instant::now(), "completed");
        assert_eq!(summary.headers_received, 5);
        assert_eq!(summary.rollbacks, 1);
        assert!(summary.must_reply_triggered);
        assert_eq!(summary.exit_reason, "completed");
        assert!(summary.collected_points.is_empty());
    }
}
