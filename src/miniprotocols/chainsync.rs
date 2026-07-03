use std::time::{Duration, Instant};

use tokio::time::timeout;

use net_core::mux::{CodecRecv, CodecSend};
use net_core::protocols::chainsync::{self, ChainSync, ChainSyncEvent};
use net_core::protocols::{Role, Runner};
use net_core::types::{Point as NcPoint, Tip as NcTip, WrappedHeader};
use pallas_network::miniprotocols::Point;
use serde_json::json;
use tracing::{debug, info, warn};

use crate::trace::{Direction, EventKind, TraceEvent, Tracer};

pub const CHAIN_SYNC_PROTOCOL: u16 = net_core::protocols::chainsync::PROTOCOL_ID;

const MINI_PROTOCOL: &str = "chain-sync";

/// A captured header as it arrives from a RollForward message.
/// Used to populate fixture files for server-side replay.
#[derive(Clone)]
pub struct CapturedHeader {
    pub slot: u64,
    pub block_hash: Vec<u8>,
    pub block_number: u64,
    pub cbor: Vec<u8>,
    /// Era variant byte from the header's era tag (0=Byron, 6=Conway, …).
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

fn format_nc_point(point: &NcPoint) -> serde_json::Value {
    match point {
        NcPoint::Origin => json!("origin"),
        NcPoint::Specific { slot, hash } => json!({
            "slot": slot,
            "hash": encode_hex(hash),
        }),
    }
}

fn format_nc_tip(tip: &NcTip) -> serde_json::Value {
    json!({
        "point": format_nc_point(&tip.point),
        "block_number": tip.block_no,
    })
}

/// Convert pallas Point to net-core Point.
pub fn to_nc_point(p: &Point) -> NcPoint {
    match p {
        Point::Origin => NcPoint::Origin,
        Point::Specific(slot, hash) => {
            let mut h = [0u8; 32];
            let len = hash.len().min(32);
            h[..len].copy_from_slice(&hash[..len]);
            NcPoint::Specific { slot: *slot, hash: h }
        }
    }
}

/// Convert net-core Point to pallas Point.
#[allow(dead_code)]
fn from_nc_point(p: &NcPoint) -> Point {
    match p {
        NcPoint::Origin => Point::Origin,
        NcPoint::Specific { slot, hash } => Point::Specific(*slot, hash.to_vec()),
    }
}

/// Runs a Chain-Sync session on a pre-allocated codec pair.
///
/// Sends `MsgFindIntersect` with the supplied `intersection_points` (use
/// `vec![Point::Origin]` to start from genesis), then consumes `count` block
/// headers via `MsgRequestNext`, logging every message in both directions.
/// Terminates cleanly with `MsgDone`.
/// Returns a summary that includes the collected Points for each RollForward,
/// which callers can pass directly to Block-Fetch.
pub async fn run_chain_sync(
    codec_send: CodecSend,
    codec_recv: CodecRecv,
    intersection_points: Vec<Point>,
    count: u64,
    await_timeout: Duration,
    tracer: &Tracer,
) -> anyhow::Result<ChainSyncSummary> {
    let started_at = Instant::now();
    let mut runner = Runner::<ChainSync>::new(Role::Client, codec_send, codec_recv);

    let mut headers_received: u64 = 0;
    let mut rollbacks: u64 = 0;
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
    tracer
        .emit(
            TraceEvent::new(
                EventKind::ChainSyncFindIntersect,
                Direction::Sent,
                json!({ "points": point_values }),
            )
            .with_states(MINI_PROTOCOL, "Idle", "Intersect"),
        )
        .await?;
    messages_sent += 1;

    let nc_points: Vec<NcPoint> = intersection_points.iter().map(to_nc_point).collect();

    match chainsync::find_intersection(&mut runner, nc_points).await {
        Ok(Some((point, tip))) => {
            messages_received += 1;
            info!("Intersect found at {:?}", point);
            tracer
                .emit(
                    TraceEvent::new(
                        EventKind::ChainSyncIntersectFound,
                        Direction::Received,
                        json!({
                            "point": format_nc_point(&point),
                            "tip": format_nc_tip(&tip),
                        }),
                    )
                    .with_states(MINI_PROTOCOL, "Intersect", "Idle"),
                )
                .await?;
        }
        Ok(None) => {
            messages_received += 1;
            warn!("Intersect not found");
            tracer
                .emit(
                    TraceEvent::new(
                        EventKind::ChainSyncIntersectNotFound,
                        Direction::Received,
                        json!({}),
                    )
                    .with_states(MINI_PROTOCOL, "Intersect", "Idle"),
                )
                .await?;
        }
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
    }

    // Main request loop
    while headers_received < count {
        tracer
            .emit(
                TraceEvent::new(
                    EventKind::ChainSyncRequestNext,
                    Direction::Sent,
                    json!({ "headers_received_so_far": headers_received }),
                )
                .with_states(MINI_PROTOCOL, "Idle", "CanAwait"),
            )
            .await?;
        messages_sent += 1;

        let request_future = chainsync::request_next(&mut runner);
        let result = if await_timeout.is_zero() {
            request_future.await.map_err(|e| anyhow::anyhow!("{e}"))
        } else {
            match timeout(await_timeout, request_future).await {
                Ok(r) => r.map_err(|e| anyhow::anyhow!("{e}")),
                Err(_elapsed) => {
                    let summary = build_summary(
                        headers_received, rollbacks, false,
                        messages_sent, messages_received,
                        collected_points, captured_headers.clone(),
                        started_at, "await_timeout",
                    );
                    emit_summary(tracer, &summary).await;
                    return Ok(summary);
                }
            }
        };
        let event = match result {
            Ok(ev) => ev,
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
                    false,
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
        messages_received += 1;

        match event {
            ChainSyncEvent::RollForward { header, tip } => {
                headers_received += 1;
                debug!(headers_received, "RollForward");
                extract_and_capture(&header, &mut collected_points, &mut captured_headers);
                tracer
                    .emit(
                        TraceEvent::new(
                            EventKind::ChainSyncRollForward,
                            Direction::Received,
                            roll_forward_payload(&header, &tip),
                        )
                        .with_states(MINI_PROTOCOL, "CanAwait", "Idle"),
                    )
                    .await?;
            }
            ChainSyncEvent::RollBackward { point, tip } => {
                rollbacks += 1;
                warn!("RollBackward to {:?}", point);
                tracer
                    .emit(
                        TraceEvent::new(
                            EventKind::ChainSyncRollBackward,
                            Direction::Received,
                            json!({
                                "rollback_to": format_nc_point(&point),
                                "tip": format_nc_tip(&tip),
                            }),
                        )
                        .with_states(MINI_PROTOCOL, "CanAwait", "Idle"),
                    )
                    .await?;
            }
        }
    }

    // Send MsgDone
    if let Err(e) = chainsync::done(&mut runner).await {
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
                    .with_states(MINI_PROTOCOL, "Idle", "Done"),
            )
            .await?;
    }

    let summary = build_summary(
        headers_received,
        rollbacks,
        false,
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

/// Extract slot, block_number, hash from a WrappedHeader and push to collectors.
fn extract_and_capture(
    header: &WrappedHeader,
    collected_points: &mut Vec<Point>,
    captured_headers: &mut Vec<CapturedHeader>,
) {
    if let Some(ref info) = header.parsed {
        let point = header.point();
        if let Some(NcPoint::Specific { slot, hash }) = point {
            collected_points.push(Point::Specific(slot, hash.to_vec()));
            captured_headers.push(CapturedHeader {
                slot,
                block_hash: hash.to_vec(),
                block_number: info.block_number,
                cbor: header.raw.clone(),
                variant: info.era,
            });
        }
    } else {
        warn!("WrappedHeader has no parsed fields — skipping capture");
    }
}

fn roll_forward_payload(header: &WrappedHeader, tip: &NcTip) -> serde_json::Value {
    let variant = header.parsed.as_ref().map(|i| i.era).unwrap_or(0);
    json!({
        "variant": variant,
        "cbor_hex": encode_hex(&header.raw),
        "cbor_len": header.raw.len(),
        "tip": format_nc_tip(tip),
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
    fn round_trip_point_conversion() {
        let p = Point::Specific(100, vec![0xaa; 32]);
        let nc = to_nc_point(&p);
        let back = from_nc_point(&nc);
        assert!(matches!(back, Point::Specific(100, ref h) if h == &vec![0xaau8; 32]));
    }

    #[test]
    fn build_summary_fields() {
        let summary =
            build_summary(5, 1, true, 12, 10, vec![], vec![], Instant::now(), "completed");
        assert_eq!(summary.headers_received, 5);
        assert_eq!(summary.rollbacks, 1);
        assert!(summary.must_reply_triggered);
        assert_eq!(summary.exit_reason, "completed");
        assert!(summary.collected_points.is_empty());
    }
}
