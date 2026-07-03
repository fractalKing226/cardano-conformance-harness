use std::time::Instant;

use net_core::mux::{CodecRecv, CodecSend};
use net_core::protocols::blockfetch::{self, BlockFetch};
use net_core::protocols::{Role, Runner};
use net_core::types::Point as NcPoint;
use pallas_network::miniprotocols::Point;
use serde_json::json;
use tracing::{debug, info, warn};

use crate::miniprotocols::chainsync::format_point;
use crate::trace::{Direction, EventKind, TraceEvent, Tracer};

pub const BLOCK_FETCH_PROTOCOL: u16 = net_core::protocols::blockfetch::PROTOCOL_ID;

const MINI_PROTOCOL: &str = "block-fetch";

/// A captured block body as received from a Block-Fetch response.
/// Used to populate Block-Fetch fixture files (`--capture-block-fixture`).
#[derive(Clone)]
pub struct CapturedBlock {
    pub slot: u64,
    pub block_hash: Vec<u8>,
    pub cbor: Vec<u8>,
}

pub struct BlockFetchSummary {
    pub range_requests: u64,
    pub blocks_received: u64,
    pub no_blocks_responses: u64,
    pub total_bytes: u64,
    pub exit_reason: String,
    pub duration_ms: u64,
    /// Block bodies captured for fixture writing (`--capture-block-fixture`).
    pub captured_blocks: Vec<CapturedBlock>,
}

fn encode_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Convert pallas Point to net-core Point.
fn to_nc_point(p: &Point) -> NcPoint {
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

/// Runs a Block-Fetch session on a pre-allocated codec pair.
///
/// Issues one `MsgRequestRange` per batch (using `batch_size` consecutive points
/// per range). Each batch is drained fully before the next request is issued
/// (non-pipelined). Terminates with `MsgClientDone`.
pub async fn run_block_fetch(
    codec_send: CodecSend,
    codec_recv: CodecRecv,
    points: Vec<Point>,
    batch_size: usize,
    tracer: &Tracer,
) -> anyhow::Result<BlockFetchSummary> {
    let started_at = Instant::now();
    let mut runner = Runner::<BlockFetch>::new(Role::Client, codec_send, codec_recv);

    let mut range_requests: u64 = 0;
    let mut blocks_received: u64 = 0;
    let mut no_blocks_responses: u64 = 0;
    let mut total_bytes: u64 = 0;
    let mut captured_blocks: Vec<CapturedBlock> = Vec::new();

    let effective_batch = batch_size.max(1);

    tracer
        .emit(
            TraceEvent::new(
                EventKind::BlockFetchStarted,
                Direction::Internal,
                json!({
                    "total_points": points.len(),
                    "batch_size": effective_batch,
                }),
            )
            .with_protocol(MINI_PROTOCOL),
        )
        .await?;

    for batch in points.chunks(effective_batch) {
        let from = batch.first().unwrap();
        let to = batch.last().unwrap();
        let capture_point: Option<(u64, Vec<u8>)> = if batch.len() == 1 {
            match from {
                Point::Specific(slot, hash) => Some((*slot, hash.clone())),
                Point::Origin => None,
            }
        } else {
            None
        };

        tracer
            .emit(
                TraceEvent::new(
                    EventKind::BlockFetchRequestRange,
                    Direction::Sent,
                    json!({
                        "from": format_point(from),
                        "to":   format_point(to),
                        "batch_len": batch.len(),
                    }),
                )
                .with_states(MINI_PROTOCOL, "Idle", "Busy"),
            )
            .await?;

        let from_nc = to_nc_point(from);
        let to_nc = to_nc_point(to);

        let has_blocks = match blockfetch::request_range(&mut runner, from_nc, to_nc).await {
            Ok(r) => r,
            Err(e) => {
                let msg = e.to_string();
                let _ = tracer
                    .emit(
                        TraceEvent::new(
                            EventKind::Error,
                            Direction::Internal,
                            json!({ "phase": "request_range", "error": msg }),
                        )
                        .with_protocol(MINI_PROTOCOL),
                    )
                    .await;
                let summary = build_summary(
                    range_requests,
                    blocks_received,
                    no_blocks_responses,
                    total_bytes,
                    std::mem::take(&mut captured_blocks),
                    started_at,
                    "error",
                );
                emit_summary(tracer, &summary).await;
                return Err(anyhow::anyhow!("request_range failed: {e}"));
            }
        };
        range_requests += 1;

        if !has_blocks {
            // Server responded with MsgNoBlocks
            no_blocks_responses += 1;
            warn!("NoBlocks for range {:?}..{:?}", from, to);
            tracer
                .emit(
                    TraceEvent::new(
                        EventKind::BlockFetchNoBlocks,
                        Direction::Received,
                        json!({
                            "from": format_point(from),
                            "to":   format_point(to),
                        }),
                    )
                    .with_states(MINI_PROTOCOL, "Busy", "Idle"),
                )
                .await?;
        } else {
            // Server responded with MsgStartBatch
            tracer
                .emit(
                    TraceEvent::new(
                        EventKind::BlockFetchStartBatch,
                        Direction::Received,
                        json!({}),
                    )
                    .with_states(MINI_PROTOCOL, "Busy", "Streaming"),
                )
                .await?;

            let mut batch_blocks: u64 = 0;

            loop {
                match blockfetch::recv_block(&mut runner).await {
                    Ok(Some(body)) => {
                        let body = body.raw;
                        total_bytes += body.len() as u64;
                        blocks_received += 1;
                        batch_blocks += 1;
                        debug!(blocks_received, bytes = body.len(), "Block received");
                        if let Some((slot, ref hash)) = capture_point {
                            captured_blocks.push(CapturedBlock {
                                slot,
                                block_hash: hash.clone(),
                                cbor: body.clone(),
                            });
                        }
                        tracer
                            .emit(
                                TraceEvent::new(
                                    EventKind::BlockFetchBlock,
                                    Direction::Received,
                                    json!({
                                        "cbor_hex": encode_hex(&body),
                                        "cbor_len": body.len(),
                                    }),
                                )
                                .with_states(MINI_PROTOCOL, "Streaming", "Streaming"),
                            )
                            .await?;
                    }
                    Ok(None) => {
                        // MsgBatchDone
                        tracer
                            .emit(
                                TraceEvent::new(
                                    EventKind::BlockFetchBatchDone,
                                    Direction::Received,
                                    json!({ "blocks_in_batch": batch_blocks }),
                                )
                                .with_states(MINI_PROTOCOL, "Streaming", "Idle"),
                            )
                            .await?;
                        break;
                    }
                    Err(e) => {
                        let msg = e.to_string();
                        let _ = tracer
                            .emit(
                                TraceEvent::new(
                                    EventKind::Error,
                                    Direction::Internal,
                                    json!({ "phase": "recv_block", "error": msg }),
                                )
                                .with_protocol(MINI_PROTOCOL),
                            )
                            .await;
                        let summary = build_summary(
                            range_requests,
                            blocks_received,
                            no_blocks_responses,
                            total_bytes,
                            std::mem::take(&mut captured_blocks),
                            started_at,
                            "error",
                        );
                        emit_summary(tracer, &summary).await;
                        return Err(anyhow::anyhow!("recv_block failed: {e}"));
                    }
                }
            }
        }
    }

    // Send MsgClientDone
    if let Err(e) = blockfetch::done(&mut runner).await {
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
        tracer
            .emit(
                TraceEvent::new(EventKind::BlockFetchClientDone, Direction::Sent, json!({}))
                    .with_states(MINI_PROTOCOL, "Idle", "Done"),
            )
            .await?;
    }

    let summary = build_summary(
        range_requests,
        blocks_received,
        no_blocks_responses,
        total_bytes,
        captured_blocks,
        started_at,
        "completed",
    );
    emit_summary(tracer, &summary).await;

    info!(
        blocks_received = summary.blocks_received,
        total_bytes = summary.total_bytes,
        duration_ms = summary.duration_ms,
        "Block-fetch session complete"
    );

    Ok(summary)
}

fn build_summary(
    range_requests: u64,
    blocks_received: u64,
    no_blocks_responses: u64,
    total_bytes: u64,
    captured_blocks: Vec<CapturedBlock>,
    started_at: Instant,
    exit_reason: &str,
) -> BlockFetchSummary {
    BlockFetchSummary {
        range_requests,
        blocks_received,
        no_blocks_responses,
        total_bytes,
        exit_reason: exit_reason.to_string(),
        duration_ms: started_at.elapsed().as_millis() as u64,
        captured_blocks,
    }
}

async fn emit_summary(tracer: &Tracer, summary: &BlockFetchSummary) {
    let _ = tracer
        .emit(
            TraceEvent::new(
                EventKind::BlockFetchSessionSummary,
                Direction::Internal,
                json!({
                    "range_requests":      summary.range_requests,
                    "blocks_received":     summary.blocks_received,
                    "no_blocks_responses": summary.no_blocks_responses,
                    "total_bytes":         summary.total_bytes,
                    "exit_reason":         summary.exit_reason,
                    "duration_ms":         summary.duration_ms,
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
    fn encode_hex_known_value() {
        assert_eq!(encode_hex(&[0xca, 0xfe]), "cafe");
    }

    #[test]
    fn build_summary_fields() {
        let s = build_summary(3, 10, 1, 4096, vec![], Instant::now(), "completed");
        assert_eq!(s.range_requests, 3);
        assert_eq!(s.blocks_received, 10);
        assert_eq!(s.no_blocks_responses, 1);
        assert_eq!(s.total_bytes, 4096);
        assert_eq!(s.exit_reason, "completed");
    }

    #[test]
    fn block_payload_has_required_keys() {
        let body = vec![0xab, 0xcd, 0xef];
        let payload = json!({
            "cbor_hex": encode_hex(&body),
            "cbor_len": body.len(),
        });
        assert_eq!(payload["cbor_hex"], "abcdef");
        assert_eq!(payload["cbor_len"], 3);
    }
}
