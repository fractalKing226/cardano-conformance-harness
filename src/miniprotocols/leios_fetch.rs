use std::time::Instant;

use net_core::mux::{CodecRecv, CodecSend};
use net_core::protocols::leios_fetch::{self, LeiosFetch};
use net_core::protocols::{Role, Runner};
use net_core::types::Point as NcPoint;
use serde_json::json;
use tracing::{debug, info, warn};

use crate::trace::{Direction, EventKind, TraceEvent, Tracer};

pub const LEIOS_FETCH_PROTOCOL: u16 = net_core::protocols::leios_fetch::PROTOCOL_ID;

const MINI_PROTOCOL: &str = "leios-fetch";

pub struct LeiosFetchSummary {
    pub blocks_requested: u64,
    pub blocks_received: u64,
    pub exit_reason: String,
    pub duration_ms: u64,
}

fn encode_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn format_nc_point(point: &NcPoint) -> serde_json::Value {
    match point {
        NcPoint::Origin => json!("origin"),
        NcPoint::Specific { slot, hash } => json!({ "slot": slot, "hash": encode_hex(hash) }),
    }
}

/// Runs a LeiosFetch session, fetching each EB in `points` from the server.
///
/// Sends a `MsgLeiosBlockRequest` for each point and logs the returned block.
/// Terminates cleanly with `MsgDone`.
pub async fn run_leios_fetch(
    codec_send: CodecSend,
    codec_recv: CodecRecv,
    points: Vec<NcPoint>,
    tracer: &Tracer,
) -> anyhow::Result<LeiosFetchSummary> {
    let started_at = Instant::now();
    let mut runner = Runner::<LeiosFetch>::new(Role::Client, codec_send, codec_recv);

    let mut blocks_requested: u64 = 0;
    let mut blocks_received: u64 = 0;

    tracer
        .emit(
            TraceEvent::new(
                EventKind::LeiosFetchStarted,
                Direction::Internal,
                json!({ "target_blocks": points.len() }),
            )
            .with_protocol(MINI_PROTOCOL),
        )
        .await?;

    for point in &points {
        tracer
            .emit(
                TraceEvent::new(
                    EventKind::LeiosFetchBlockRequest,
                    Direction::Sent,
                    json!({ "point": format_nc_point(point) }),
                )
                .with_protocol(MINI_PROTOCOL),
            )
            .await?;
        blocks_requested += 1;

        match leios_fetch::fetch_block(&mut runner, point.clone()).await {
            Ok(block) => {
                blocks_received += 1;
                debug!(blocks_received, block_len = block.len(), "LeiosFetch block received");
                tracer
                    .emit(
                        TraceEvent::new(
                            EventKind::LeiosFetchBlock,
                            Direction::Received,
                            json!({ "point": format_nc_point(point), "block_len": block.len() }),
                        )
                        .with_protocol(MINI_PROTOCOL),
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
                            json!({ "phase": "fetch_block", "error": msg, "point": format_nc_point(point) }),
                        )
                        .with_protocol(MINI_PROTOCOL),
                    )
                    .await;
                let summary = LeiosFetchSummary {
                    blocks_requested,
                    blocks_received,
                    exit_reason: "error".to_string(),
                    duration_ms: started_at.elapsed().as_millis() as u64,
                };
                emit_summary(tracer, &summary).await;
                return Err(anyhow::anyhow!("leios_fetch fetch_block failed: {e}"));
            }
        }
    }

    if let Err(e) = leios_fetch::done(&mut runner).await {
        warn!("leios_fetch done failed (connection may already be closed): {e}");
    } else {
        let _ = tracer
            .emit(
                TraceEvent::new(EventKind::LeiosFetchDone, Direction::Sent, json!({}))
                    .with_protocol(MINI_PROTOCOL),
            )
            .await;
    }

    let summary = LeiosFetchSummary {
        blocks_requested,
        blocks_received,
        exit_reason: "completed".to_string(),
        duration_ms: started_at.elapsed().as_millis() as u64,
    };
    emit_summary(tracer, &summary).await;

    info!(
        blocks_received = summary.blocks_received,
        duration_ms = summary.duration_ms,
        "LeiosFetch session complete"
    );

    Ok(summary)
}

async fn emit_summary(tracer: &Tracer, summary: &LeiosFetchSummary) {
    let _ = tracer
        .emit(
            TraceEvent::new(
                EventKind::LeiosFetchSessionSummary,
                Direction::Internal,
                json!({
                    "blocks_requested": summary.blocks_requested,
                    "blocks_received":  summary.blocks_received,
                    "exit_reason":      summary.exit_reason,
                    "duration_ms":      summary.duration_ms,
                }),
            )
            .with_protocol(MINI_PROTOCOL),
        )
        .await;
}
