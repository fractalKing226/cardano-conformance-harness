use std::time::{Duration, Instant};

use pallas_network::miniprotocols::chainsync::{ClientRequest, N2NServer, Tip};
use pallas_network::miniprotocols::Point;
use serde_json::json;
use tracing::{debug, info, warn};

use crate::scenario::fixture::{Cursor, FixtureChain};
use crate::trace::{Direction, EventKind, TraceEvent, Tracer};

const MINI_PROTOCOL: &str = "chain-sync";

pub struct ChainSyncServerSummary {
    pub headers_served: u64,
    pub intersects_handled: u64,
    pub await_triggered: bool,
    pub exit_reason: String,
    pub duration_ms: u64,
}

/// Serve a fixture chain to one connected Chain-Sync client.
///
/// The caller passes the already-constructed `N2NServer` (obtained from
/// `PeerServer.chainsync` after a successful handshake). The server reads its
/// entire behavioral contract — fixture content and tip-hold duration — from
/// the `chain` and `await_timeout` arguments. No chain content or timing is
/// hard-coded here.
pub async fn run_chain_sync_server(
    server: &mut N2NServer,
    chain: &FixtureChain,
    await_timeout: Duration,
    tracer: &Tracer,
) -> anyhow::Result<ChainSyncServerSummary> {
    let started_at = Instant::now();
    let mut cursor = Cursor::new(chain);
    let mut headers_served: u64 = 0;
    let mut intersects_handled: u64 = 0;
    let mut await_triggered = false;

    tracer
        .emit(
            TraceEvent::new(
                EventKind::ServerChainSyncStarted,
                Direction::Internal,
                json!({
                    "fixture_entries": chain.entries.len(),
                    "await_timeout_secs": await_timeout.as_secs(),
                }),
            )
            .with_protocol(MINI_PROTOCOL),
        )
        .await?;

    loop {
        // `recv_while_idle` returns Ok(None) when client sends MsgDone.
        let request = match server.recv_while_idle().await {
            Ok(Some(r)) => r,
            Ok(None) => {
                // Client sent MsgDone — clean close.
                debug!("Chain-Sync client sent Done");
                break;
            }
            Err(e) => {
                warn!("Chain-Sync recv_while_idle error: {e}");
                let summary = build_summary(
                    headers_served, intersects_handled, await_triggered,
                    started_at, "recv_error",
                );
                emit_server_summary(tracer, &summary).await;
                return Err(anyhow::anyhow!("recv_while_idle failed: {e}"));
            }
        };

        match request {
            ClientRequest::Intersect(points) => {
                intersects_handled += 1;
                let tip = cursor.tip();

                // Log the received FindIntersect (direction: received — peer sent it to us).
                let point_values: Vec<serde_json::Value> = points
                    .iter()
                    .map(|p| format_point(p))
                    .collect();
                tracer
                    .emit(
                        TraceEvent::new(
                            EventKind::ChainSyncFindIntersect,
                            Direction::Received,
                            json!({ "points": point_values }),
                        )
                        .with_states(MINI_PROTOCOL, "Idle", "Intersect"),
                    )
                    .await?;

                match cursor.find_intersect(&points) {
                    Some(pos) => {
                        cursor.set_pos(pos);
                        let found_point = cursor.current_point();
                        info!("IntersectFound at pos {pos}");
                        server
                            .send_intersect_found(found_point.clone(), tip.clone())
                            .await
                            .map_err(|e| anyhow::anyhow!("send_intersect_found failed: {e}"))?;
                        tracer
                            .emit(
                                TraceEvent::new(
                                    EventKind::ChainSyncIntersectFound,
                                    Direction::Sent,
                                    json!({
                                        "point": format_point(&found_point),
                                        "tip":   format_tip(&tip),
                                    }),
                                )
                                .with_states(MINI_PROTOCOL, "Intersect", "Idle"),
                            )
                            .await?;
                    }
                    None => {
                        info!("IntersectNotFound — no match in fixture");
                        server
                            .send_intersect_not_found(tip.clone())
                            .await
                            .map_err(|e| anyhow::anyhow!("send_intersect_not_found failed: {e}"))?;
                        tracer
                            .emit(
                                TraceEvent::new(
                                    EventKind::ChainSyncIntersectNotFound,
                                    Direction::Sent,
                                    json!({ "tip": format_tip(&tip) }),
                                )
                                .with_states(MINI_PROTOCOL, "Intersect", "Idle"),
                            )
                            .await?;
                    }
                }
            }

            ClientRequest::RequestNext => {
                // Log the received RequestNext (direction: received — peer sent it).
                tracer
                    .emit(
                        TraceEvent::new(
                            EventKind::ChainSyncRequestNext,
                            Direction::Received,
                            json!({ "headers_served_so_far": headers_served }),
                        )
                        .with_states(MINI_PROTOCOL, "Idle", "CanAwait"),
                    )
                    .await?;

                let tip = cursor.tip();

                match cursor.advance() {
                    Some(entry) => {
                        // Send the next header.
                        let point = Point::Specific(
                            entry.slot,
                            decode_hex(&entry.block_hash),
                        );
                        let header_content = make_header_content(&entry.cbor_hex);
                        server
                            .send_roll_forward(header_content, tip.clone())
                            .await
                            .map_err(|e| anyhow::anyhow!("send_roll_forward failed: {e}"))?;
                        headers_served += 1;
                        tracer
                            .emit(
                                TraceEvent::new(
                                    EventKind::ChainSyncRollForward,
                                    Direction::Sent,
                                    json!({
                                        "slot":         entry.slot,
                                        "block_hash":   entry.block_hash,
                                        "block_number": entry.block_number,
                                        "cbor_len":     entry.cbor_hex.len() / 2,
                                        "tip":          format_tip(&tip),
                                    }),
                                )
                                .with_states(MINI_PROTOCOL, "CanAwait", "Idle"),
                            )
                            .await?;
                        debug!(headers_served, slot = entry.slot, "Served header");
                    }
                    None => {
                        // At tip — send AwaitReply, hold, then close.
                        await_triggered = true;
                        info!(
                            timeout_secs = await_timeout.as_secs(),
                            "At fixture tip — sending AwaitReply"
                        );
                        server
                            .send_await_reply()
                            .await
                            .map_err(|e| anyhow::anyhow!("send_await_reply failed: {e}"))?;
                        tracer
                            .emit(
                                TraceEvent::new(
                                    EventKind::ChainSyncAwaitReply,
                                    Direction::Sent,
                                    json!({ "hold_secs": await_timeout.as_secs() }),
                                )
                                .with_states(MINI_PROTOCOL, "CanAwait", "MustReply"),
                            )
                            .await?;

                        // Hold in MustReply for the configured duration, then close.
                        tokio::time::sleep(await_timeout).await;
                        info!("Await timeout reached — closing session");
                        break;
                    }
                }
            }
        }
    }

    let summary = build_summary(
        headers_served, intersects_handled, await_triggered,
        started_at, "completed",
    );
    emit_server_summary(tracer, &summary).await;

    info!(
        headers_served = summary.headers_served,
        duration_ms = summary.duration_ms,
        "Chain-Sync server session complete"
    );

    Ok(summary)
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn format_point(point: &Point) -> serde_json::Value {
    match point {
        Point::Origin => serde_json::json!("origin"),
        Point::Specific(slot, hash) => serde_json::json!({
            "slot": slot,
            "hash": encode_hex(hash),
        }),
    }
}

fn format_tip(tip: &Tip) -> serde_json::Value {
    let Tip(point, block_number) = tip;
    serde_json::json!({ "point": format_point(point), "block_number": block_number })
}

fn encode_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn decode_hex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .filter_map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok())
        .collect()
}

fn make_header_content(cbor_hex: &str) -> pallas_network::miniprotocols::chainsync::HeaderContent {
    pallas_network::miniprotocols::chainsync::HeaderContent {
        variant: 6, // Conway — appropriate for fixture-replayed headers
        byron_prefix: None,
        cbor: decode_hex(cbor_hex),
    }
}

fn build_summary(
    headers_served: u64,
    intersects_handled: u64,
    await_triggered: bool,
    started_at: Instant,
    exit_reason: &str,
) -> ChainSyncServerSummary {
    ChainSyncServerSummary {
        headers_served,
        intersects_handled,
        await_triggered,
        exit_reason: exit_reason.to_string(),
        duration_ms: started_at.elapsed().as_millis() as u64,
    }
}

async fn emit_server_summary(tracer: &Tracer, summary: &ChainSyncServerSummary) {
    let _ = tracer
        .emit(
            TraceEvent::new(
                EventKind::ServerChainSyncCompleted,
                Direction::Internal,
                json!({
                    "headers_served":     summary.headers_served,
                    "intersects_handled": summary.intersects_handled,
                    "await_triggered":    summary.await_triggered,
                    "exit_reason":        summary.exit_reason,
                    "duration_ms":        summary.duration_ms,
                }),
            )
            .with_protocol(MINI_PROTOCOL),
        )
        .await;
}
