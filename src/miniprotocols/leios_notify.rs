use std::time::{Duration, Instant};

use tokio::time::timeout;

use net_core::mux::{CodecRecv, CodecSend};
use net_core::protocols::leios_notify::{self, LeiosNotify, LeiosNotifyEvent};
use net_core::protocols::{Role, Runner};
use net_core::types::Point as NcPoint;
use serde_json::json;
use tracing::{debug, info, warn};

use crate::trace::{Direction, EventKind, TraceEvent, Tracer};

pub const LEIOS_NOTIFY_PROTOCOL: u16 = net_core::protocols::leios_notify::PROTOCOL_ID;

const MINI_PROTOCOL: &str = "leios-notify";

pub struct LeiosNotifySummary {
    pub events_received: u64,
    pub block_announcements: u64,
    pub block_offers: u64,
    pub block_txs_offers: u64,
    pub vote_messages: u64,
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

/// Runs a LeiosNotify session, pulling `count` notifications from the server.
///
/// Uses a long-poll per-request: the server may hold each request until Leios
/// data is available. `await_timeout` bounds how long we wait per request; a
/// zero value means wait indefinitely.
pub async fn run_leios_notify(
    codec_send: CodecSend,
    codec_recv: CodecRecv,
    count: u64,
    await_timeout: Duration,
    tracer: &Tracer,
) -> anyhow::Result<LeiosNotifySummary> {
    let started_at = Instant::now();
    let mut runner = Runner::<LeiosNotify>::new(Role::Client, codec_send, codec_recv);

    let mut events_received: u64 = 0;
    let mut block_announcements: u64 = 0;
    let mut block_offers: u64 = 0;
    let mut block_txs_offers: u64 = 0;
    let mut vote_messages: u64 = 0;

    tracer
        .emit(
            TraceEvent::new(
                EventKind::LeiosNotifyStarted,
                Direction::Internal,
                json!({ "target_events": count }),
            )
            .with_protocol(MINI_PROTOCOL),
        )
        .await?;

    while events_received < count {
        let request_future = leios_notify::request_next(&mut runner);
        let result = if await_timeout.is_zero() {
            request_future.await.map_err(|e| anyhow::anyhow!("{e}"))
        } else {
            match timeout(await_timeout, request_future).await {
                Ok(r) => r.map_err(|e| anyhow::anyhow!("{e}")),
                Err(_elapsed) => {
                    let summary = build_summary(
                        events_received,
                        block_announcements,
                        block_offers,
                        block_txs_offers,
                        vote_messages,
                        started_at,
                        "await_timeout",
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
                    events_received,
                    block_announcements,
                    block_offers,
                    block_txs_offers,
                    vote_messages,
                    started_at,
                    "error",
                );
                emit_summary(tracer, &summary).await;
                return Err(anyhow::anyhow!("leios_notify request_next failed: {e}"));
            }
        };
        events_received += 1;

        match event {
            LeiosNotifyEvent::BlockAnnouncement { header } => {
                block_announcements += 1;
                debug!(events_received, "LeiosNotify BlockAnnouncement");
                tracer
                    .emit(
                        TraceEvent::new(
                            EventKind::LeiosNotifyBlockAnnouncement,
                            Direction::Received,
                            json!({ "header_len": header.raw.len() }),
                        )
                        .with_protocol(MINI_PROTOCOL),
                    )
                    .await?;
            }
            LeiosNotifyEvent::BlockOffer { point, eb_size } => {
                block_offers += 1;
                debug!(events_received, "LeiosNotify BlockOffer");
                tracer
                    .emit(
                        TraceEvent::new(
                            EventKind::LeiosNotifyBlockOffer,
                            Direction::Received,
                            json!({ "point": format_nc_point(&point), "eb_size": eb_size }),
                        )
                        .with_protocol(MINI_PROTOCOL),
                    )
                    .await?;
            }
            LeiosNotifyEvent::BlockTxsOffer { point } => {
                block_txs_offers += 1;
                debug!(events_received, "LeiosNotify BlockTxsOffer");
                tracer
                    .emit(
                        TraceEvent::new(
                            EventKind::LeiosNotifyBlockTxsOffer,
                            Direction::Received,
                            json!({ "point": format_nc_point(&point) }),
                        )
                        .with_protocol(MINI_PROTOCOL),
                    )
                    .await?;
            }
            LeiosNotifyEvent::Votes { votes } => {
                vote_messages += 1;
                debug!(events_received, "LeiosNotify Votes");
                tracer
                    .emit(
                        TraceEvent::new(
                            EventKind::LeiosNotifyVotes,
                            Direction::Received,
                            json!({ "vote_count": votes.len() }),
                        )
                        .with_protocol(MINI_PROTOCOL),
                    )
                    .await?;
            }
        }
    }

    if let Err(e) = leios_notify::done(&mut runner).await {
        warn!("leios_notify done failed (connection may already be closed): {e}");
    } else {
        let _ = tracer
            .emit(
                TraceEvent::new(EventKind::LeiosNotifyDone, Direction::Sent, json!({}))
                    .with_protocol(MINI_PROTOCOL),
            )
            .await;
    }

    let summary = build_summary(
        events_received,
        block_announcements,
        block_offers,
        block_txs_offers,
        vote_messages,
        started_at,
        "completed",
    );
    emit_summary(tracer, &summary).await;

    info!(
        events_received = summary.events_received,
        duration_ms = summary.duration_ms,
        "LeiosNotify session complete"
    );

    Ok(summary)
}

fn build_summary(
    events_received: u64,
    block_announcements: u64,
    block_offers: u64,
    block_txs_offers: u64,
    vote_messages: u64,
    started_at: Instant,
    exit_reason: &str,
) -> LeiosNotifySummary {
    LeiosNotifySummary {
        events_received,
        block_announcements,
        block_offers,
        block_txs_offers,
        vote_messages,
        exit_reason: exit_reason.to_string(),
        duration_ms: started_at.elapsed().as_millis() as u64,
    }
}

async fn emit_summary(tracer: &Tracer, summary: &LeiosNotifySummary) {
    let _ = tracer
        .emit(
            TraceEvent::new(
                EventKind::LeiosNotifySessionSummary,
                Direction::Internal,
                json!({
                    "events_received":    summary.events_received,
                    "block_announcements": summary.block_announcements,
                    "block_offers":       summary.block_offers,
                    "block_txs_offers":   summary.block_txs_offers,
                    "vote_messages":      summary.vote_messages,
                    "exit_reason":        summary.exit_reason,
                    "duration_ms":        summary.duration_ms,
                }),
            )
            .with_protocol(MINI_PROTOCOL),
        )
        .await;
}
