use std::time::Instant;

use pallas_network::miniprotocols::blockfetch::Client;
use pallas_network::miniprotocols::Point;
use pallas_network::multiplexer::AgentChannel;
use serde_json::json;
use tracing::{debug, info, warn};

use crate::miniprotocols::chainsync::format_point;
use crate::trace::{Direction, EventKind, TraceEvent, Tracer};

pub use pallas_network::miniprotocols::PROTOCOL_N2N_BLOCK_FETCH as BLOCK_FETCH_PROTOCOL;

const MINI_PROTOCOL: &str = "block-fetch";

pub struct BlockFetchSummary {
    pub range_requests: u64,
    pub blocks_received: u64,
    pub no_blocks_responses: u64,
    pub total_bytes: u64,
    pub exit_reason: String,
    pub duration_ms: u64,
}

fn encode_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn state_str(client: &Client) -> String {
    format!("{:?}", client.state())
}

/// Runs a Block-Fetch session on a pre-allocated multiplexer channel.
///
/// Issues one `MsgRequestRange` per batch (using `batch_size` consecutive points
/// per range). Each batch is drained fully before the next request is issued
/// (non-pipelined). Terminates with `MsgClientDone`.
pub async fn run_block_fetch(
    channel: AgentChannel,
    points: Vec<Point>,
    batch_size: usize,
    tracer: &mut Tracer,
) -> anyhow::Result<BlockFetchSummary> {
    let started_at = Instant::now();
    let mut client = Client::new(channel);

    let mut range_requests: u64 = 0;
    let mut blocks_received: u64 = 0;
    let mut no_blocks_responses: u64 = 0;
    let mut total_bytes: u64 = 0;

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

        let sb = state_str(&client);
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
                .with_states(MINI_PROTOCOL, sb, "Busy"),
            )
            .await?;

        let has_blocks = match client.request_range((from.clone(), to.clone())).await {
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
                    started_at,
                    "error",
                );
                emit_summary(tracer, &summary).await;
                return Err(anyhow::anyhow!("request_range failed: {e}"));
            }
        };
        range_requests += 1;

        match has_blocks {
            None => {
                // Server responded with MsgNoBlocks — range not available
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
                        .with_states(MINI_PROTOCOL, "Busy", state_str(&client)),
                    )
                    .await?;
            }
            Some(()) => {
                // Server responded with MsgStartBatch — blocks are streaming
                tracer
                    .emit(
                        TraceEvent::new(
                            EventKind::BlockFetchStartBatch,
                            Direction::Received,
                            json!({}),
                        )
                        .with_states(MINI_PROTOCOL, "Busy", state_str(&client)),
                    )
                    .await?;

                let mut batch_blocks: u64 = 0;

                loop {
                    let sb = state_str(&client);
                    match client.recv_while_streaming().await {
                        Ok(Some(body)) => {
                            total_bytes += body.len() as u64;
                            blocks_received += 1;
                            batch_blocks += 1;
                            debug!(blocks_received, bytes = body.len(), "Block received");
                            tracer
                                .emit(
                                    TraceEvent::new(
                                        EventKind::BlockFetchBlock,
                                        Direction::Received,
                                        json!({
                                            "cbor_hex": encode_hex(&body),
                                            "cbor_len": body.len(),
                                            // TODO: extract slot/hash/block_number from body CBOR
                                        }),
                                    )
                                    .with_states(MINI_PROTOCOL, &sb, state_str(&client)),
                                )
                                .await?;
                        }
                        Ok(None) => {
                            // MsgBatchDone — end of this batch
                            tracer
                                .emit(
                                    TraceEvent::new(
                                        EventKind::BlockFetchBatchDone,
                                        Direction::Received,
                                        json!({ "blocks_in_batch": batch_blocks }),
                                    )
                                    .with_states(MINI_PROTOCOL, &sb, state_str(&client)),
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
                                        json!({ "phase": "recv_while_streaming", "error": msg }),
                                    )
                                    .with_protocol(MINI_PROTOCOL),
                                )
                                .await;
                            let summary = build_summary(
                                range_requests,
                                blocks_received,
                                no_blocks_responses,
                                total_bytes,
                                started_at,
                                "error",
                            );
                            emit_summary(tracer, &summary).await;
                            return Err(anyhow::anyhow!("recv_while_streaming failed: {e}"));
                        }
                    }
                }
            }
        }
    }

    // Send MsgClientDone
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
        tracer
            .emit(
                TraceEvent::new(EventKind::BlockFetchClientDone, Direction::Sent, json!({}))
                    .with_states(MINI_PROTOCOL, sb, "Done"),
            )
            .await?;
    }

    let summary = build_summary(
        range_requests,
        blocks_received,
        no_blocks_responses,
        total_bytes,
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
    }
}

async fn emit_summary(tracer: &mut Tracer, summary: &BlockFetchSummary) {
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
        let s = build_summary(3, 10, 1, 4096, Instant::now(), "completed");
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
