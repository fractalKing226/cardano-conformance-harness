use std::time::{Duration, Instant};

use bytes::Bytes;
use net_core::mux::{CodecRecv, CodecSend};
use pallas_network::miniprotocols::chainsync::{HeaderContent, Message, Tip};
use pallas_network::miniprotocols::Point;
use serde_json::json;
use tracing::{debug, info, warn};

use crate::scenario::fixture::{Cursor, FixtureChain, encode_hex};
use crate::scenario::response_rules::{HeaderSource, On, ScriptRule, ScriptSend, TipSpec};
use crate::trace::{Direction, EventKind, TraceEvent, Tracer};

const MINI_PROTOCOL: &str = "chain-sync";

// ── Protocol state tracker ────────────────────────────────────────────────────

/// Independent protocol state tracker for trace annotation.
///
/// The serve loop maintains this explicitly because we bypass the typed state
/// machine. If a rule sends a message illegal in the tracked state, the
/// annotation reflects that — exactly the diagnostic information a verifier
/// needs to detect protocol violations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerCsState {
    Idle,
    Intersect,
    CanAwait,
    MustReply,
    Done,
}

impl ServerCsState {
    pub fn as_str(self) -> &'static str {
        match self {
            ServerCsState::Idle      => "Idle",
            ServerCsState::Intersect => "Intersect",
            ServerCsState::CanAwait  => "CanAwait",
            ServerCsState::MustReply => "MustReply",
            ServerCsState::Done      => "Done",
        }
    }

    fn after_receive(self, msg: &ReceivedRequest) -> Self {
        match msg {
            ReceivedRequest::FindIntersect(_) => ServerCsState::Intersect,
            ReceivedRequest::RequestNext       => ServerCsState::CanAwait,
            ReceivedRequest::Done              => ServerCsState::Done,
        }
    }

    fn after_send(self, send: &ScriptSend) -> Self {
        match send {
            ScriptSend::IntersectFound { .. }
            | ScriptSend::IntersectNotFound { .. }
            | ScriptSend::RollForward { .. }
            | ScriptSend::RollBackward { .. }
            | ScriptSend::CursorFindIntersect
            | ScriptSend::CursorAdvance         => ServerCsState::Idle,
            ScriptSend::AwaitReply { .. }        => ServerCsState::MustReply,
            ScriptSend::Disconnect               => ServerCsState::Done,
            // Wait and RawBytes don't change the tracked state.
            ScriptSend::Wait { .. }
            | ScriptSend::RawBytes { .. }        => self,
            // Block-Fetch sends are never executed by the Chain-Sync loop;
            // treat them as no-ops for state tracking.
            ScriptSend::StartBatch
            | ScriptSend::Block { .. }
            | ScriptSend::BatchDone
            | ScriptSend::NoBlocks
            | ScriptSend::StreamBatch { .. }
            | ScriptSend::CursorRange
            | ScriptSend::SendSequence { .. }    => self,
        }
    }
}

// ── Incoming request ──────────────────────────────────────────────────────────

#[derive(Debug)]
pub enum ReceivedRequest {
    FindIntersect(Vec<Point>),
    RequestNext,
    Done,
}

impl ReceivedRequest {
    pub fn kind_str(&self) -> &'static str {
        match self {
            ReceivedRequest::FindIntersect(_) => "find_intersect",
            ReceivedRequest::RequestNext       => "request_next",
            ReceivedRequest::Done              => "done",
        }
    }

    fn matches_on(&self, on: &On) -> bool {
        matches!(on, On::Any) || match (self, on) {
            (ReceivedRequest::FindIntersect(_), On::FindIntersect) => true,
            (ReceivedRequest::RequestNext, On::RequestNext)         => true,
            (ReceivedRequest::Done, On::Done)                       => true,
            _ => false,
        }
    }
}

// ── Summary ───────────────────────────────────────────────────────────────────

pub struct ChainSyncServerSummary {
    pub headers_served: u64,
    pub intersects_handled: u64,
    pub await_triggered: bool,
    pub rules_applied: u64,
    pub exit_reason: String,
    pub duration_ms: u64,
}

// ── Entry point ───────────────────────────────────────────────────────────────

/// Execute a response script on a Chain-Sync server channel.
///
/// This is the single execution path for both fixture-generated honest scripts
/// and user-specified adversarial scripts. The `rules` slice is consumed in
/// order; `cursor` and `fixture` are only consulted by `CursorFindIntersect`
/// and `CursorAdvance` rules.
///
/// All sends bypass the typed state machine. The harness has no opinion on
/// whether a rule is spec-correct.
pub async fn execute_response_script(
    codec_send: &CodecSend,
    codec_recv: &mut CodecRecv,
    rules: &[ScriptRule],
    fixture: Option<&FixtureChain>,
    tracer: &Tracer,
) -> anyhow::Result<ChainSyncServerSummary> {
    let started_at = Instant::now();
    let mut state = ServerCsState::Idle;
    let mut headers_served: u64 = 0;
    let mut intersects_handled: u64 = 0;
    let mut await_triggered = false;
    let mut rules_applied: u64 = 0;
    let mut rule_idx = 0usize;
    let mut cursor = fixture.map(Cursor::new);

    tracer
        .emit(
            TraceEvent::new(
                EventKind::ServerChainSyncStarted,
                Direction::Internal,
                json!({
                    "script_rules": rules.len(),
                    "fixture_entries": fixture.map(|f| f.entries.len()),
                }),
            )
            .with_protocol(MINI_PROTOCOL),
        )
        .await?;

    loop {
        // Receive the next message from the client.
        let request = match recv_request(codec_recv).await {
            Ok(r) => r,
            Err(e) => {
                warn!("Chain-Sync server recv error: {e}");
                break;
            }
        };

        let state_before = state;
        state = state.after_receive(&request);

        // Emit receive event with direction: received.
        emit_receive_event(tracer, &request, state_before, state).await?;

        // MsgDone — client closed session cleanly.
        if matches!(request, ReceivedRequest::Done) {
            break;
        }

        // Find the next unconsumed matching rule.
        let rule_pos = rules[rule_idx..]
            .iter()
            .position(|r| request.matches_on(&r.on))
            .map(|p| p + rule_idx);

        let rule = match rule_pos {
            Some(pos) => {
                if !rules[pos].repeatable {
                    rule_idx = pos + 1;
                }
                &rules[pos]
            }
            None => {
                anyhow::bail!(
                    "serve_chain_sync: no matching rule for {:?} (exhausted {} rules)",
                    request.kind_str(),
                    rules.len()
                );
            }
        };

        tracer
            .emit(TraceEvent::new(
                EventKind::ResponseRuleApplied,
                Direction::Internal,
                json!({
                    "rule_index":  rule_pos.unwrap_or(0),
                    "on":          rule.on_str(),
                    "send":        rule.send.kind_str(),
                }),
            ))
            .await?;
        rules_applied += 1;

        let state_before_send = state;

        let done = execute_send(
            rule,
            &request,
            codec_send,
            cursor.as_mut(),
            fixture,
            tracer,
            state_before_send,
            &mut headers_served,
            &mut intersects_handled,
            &mut await_triggered,
        )
        .await?;

        state = state.after_send(&rule.send);

        if done {
            break;
        }
    }

    let summary = ChainSyncServerSummary {
        headers_served,
        intersects_handled,
        await_triggered,
        rules_applied,
        exit_reason: "completed".into(),
        duration_ms: started_at.elapsed().as_millis() as u64,
    };

    tracer
        .emit(
            TraceEvent::new(
                EventKind::ServerChainSyncCompleted,
                Direction::Internal,
                json!({
                    "headers_served":     summary.headers_served,
                    "intersects_handled": summary.intersects_handled,
                    "await_triggered":    summary.await_triggered,
                    "rules_applied":      summary.rules_applied,
                    "exit_reason":        summary.exit_reason,
                    "duration_ms":        summary.duration_ms,
                }),
            )
            .with_protocol(MINI_PROTOCOL),
        )
        .await?;

    info!(
        headers_served,
        rules_applied,
        duration_ms = summary.duration_ms,
        "Chain-Sync server session complete"
    );

    Ok(summary)
}

// ── Send execution ────────────────────────────────────────────────────────────

/// Execute one scripted send. Returns `true` if the session should end.
#[allow(clippy::too_many_arguments)]
async fn execute_send(
    rule: &ScriptRule,
    request: &ReceivedRequest,
    codec_send: &CodecSend,
    cursor: Option<&mut Cursor<'_>>,
    fixture: Option<&FixtureChain>,
    tracer: &Tracer,
    state_before: ServerCsState,
    headers_served: &mut u64,
    intersects_handled: &mut u64,
    await_triggered: &mut bool,
) -> anyhow::Result<bool> {
    match &rule.send {
        ScriptSend::CursorFindIntersect => {
            let cursor = cursor.ok_or_else(|| anyhow::anyhow!("CursorFindIntersect with no fixture"))?;
            *intersects_handled += 1;
            let tip = cursor.tip();
            let points = match request {
                ReceivedRequest::FindIntersect(pts) => pts.as_slice(),
                _ => &[],
            };
            match cursor.find_intersect(points) {
                Some(pos) => {
                    cursor.set_pos(pos);
                    let found_pt = cursor.current_point();
                    let msg = Message::<HeaderContent>::IntersectFound(found_pt.clone(), tip.clone());
                    send_message(codec_send, &msg).await?;
                    tracer
                        .emit(
                            TraceEvent::new(
                                EventKind::ChainSyncIntersectFound,
                                Direction::Sent,
                                json!({
                                    "point": crate::miniprotocols::chainsync::format_point(&found_pt),
                                    "tip":   format_tip(&tip),
                                }),
                            )
                            .with_states(MINI_PROTOCOL, state_before.as_str(), "Idle"),
                        )
                        .await?;
                }
                None => {
                    let msg = Message::<HeaderContent>::IntersectNotFound(tip.clone());
                    send_message(codec_send, &msg).await?;
                    tracer
                        .emit(
                            TraceEvent::new(
                                EventKind::ChainSyncIntersectNotFound,
                                Direction::Sent,
                                json!({ "tip": format_tip(&tip) }),
                            )
                            .with_states(MINI_PROTOCOL, state_before.as_str(), "Idle"),
                        )
                        .await?;
                }
            }
        }

        ScriptSend::CursorAdvance => {
            let cursor = cursor.ok_or_else(|| anyhow::anyhow!("CursorAdvance with no fixture"))?;
            let tip = cursor.tip();
            match cursor.advance() {
                Some(entry) => {
                    let hc = entry_to_header_content(entry);
                    let msg = Message::RollForward(hc, tip.clone());
                    send_message(codec_send, &msg).await?;
                    *headers_served += 1;
                    debug!(slot = entry.slot, "Served header via CursorAdvance");
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
                            .with_states(MINI_PROTOCOL, state_before.as_str(), "Idle"),
                        )
                        .await?;
                }
                None => {
                    *await_triggered = true;
                    let msg = Message::<HeaderContent>::AwaitReply;
                    send_message(codec_send, &msg).await?;
                    tracer
                        .emit(
                            TraceEvent::new(
                                EventKind::ChainSyncAwaitReply,
                                Direction::Sent,
                                json!({ "hold_secs": 0 }),
                            )
                            .with_states(MINI_PROTOCOL, state_before.as_str(), "MustReply"),
                        )
                        .await?;
                }
            }
        }

        ScriptSend::RollForward { source, tip } => {
            let fixture_tip = fixture.map(|f| f.tip());
            let wire_tip = tip_spec_to_tip(tip.as_ref(), fixture_tip.as_ref());
            let hc = header_source_to_content(source);
            let (slot, hash, block_number) = extract_header_info(source);
            let msg = Message::RollForward(hc, wire_tip.clone());
            send_message(codec_send, &msg).await?;
            *headers_served += 1;
            tracer
                .emit(
                    TraceEvent::new(
                        EventKind::ChainSyncRollForward,
                        Direction::Sent,
                        json!({
                            "slot":         slot,
                            "block_hash":   hash,
                            "block_number": block_number,
                            "tip":          format_tip(&wire_tip),
                        }),
                    )
                    .with_states(MINI_PROTOCOL, state_before.as_str(), "Idle"),
                )
                .await?;
        }

        ScriptSend::RollBackward { point, tip } => {
            let fixture_tip = fixture.map(|f| f.tip());
            let wire_tip = tip_spec_to_tip(tip.as_ref(), fixture_tip.as_ref());
            let pt = parse_point_str(point)?;
            let msg = Message::<HeaderContent>::RollBackward(pt.clone(), wire_tip.clone());
            send_message(codec_send, &msg).await?;
            tracer
                .emit(
                    TraceEvent::new(
                        EventKind::ChainSyncRollBackward,
                        Direction::Sent,
                        json!({
                            "rollback_to": crate::miniprotocols::chainsync::format_point(&pt),
                            "tip":         format_tip(&wire_tip),
                        }),
                    )
                    .with_states(MINI_PROTOCOL, state_before.as_str(), "Idle"),
                )
                .await?;
        }

        ScriptSend::IntersectFound { point, tip } => {
            let fixture_tip = fixture.map(|f| f.tip());
            let wire_tip = tip_spec_to_tip(tip.as_ref(), fixture_tip.as_ref());
            let pt = parse_point_str(point)?;
            let msg = Message::<HeaderContent>::IntersectFound(pt.clone(), wire_tip.clone());
            send_message(codec_send, &msg).await?;
            *intersects_handled += 1;
            tracer
                .emit(
                    TraceEvent::new(
                        EventKind::ChainSyncIntersectFound,
                        Direction::Sent,
                        json!({
                            "point": crate::miniprotocols::chainsync::format_point(&pt),
                            "tip":   format_tip(&wire_tip),
                        }),
                    )
                    .with_states(MINI_PROTOCOL, state_before.as_str(), "Idle"),
                )
                .await?;
        }

        ScriptSend::IntersectNotFound { tip } => {
            let fixture_tip = fixture.map(|f| f.tip());
            let wire_tip = tip_spec_to_tip(tip.as_ref(), fixture_tip.as_ref());
            let msg = Message::<HeaderContent>::IntersectNotFound(wire_tip.clone());
            send_message(codec_send, &msg).await?;
            tracer
                .emit(
                    TraceEvent::new(
                        EventKind::ChainSyncIntersectNotFound,
                        Direction::Sent,
                        json!({ "tip": format_tip(&wire_tip) }),
                    )
                    .with_states(MINI_PROTOCOL, state_before.as_str(), "Idle"),
                )
                .await?;
        }

        ScriptSend::AwaitReply { hold_secs } => {
            *await_triggered = true;
            let msg = Message::<HeaderContent>::AwaitReply;
            send_message(codec_send, &msg).await?;
            tracer
                .emit(
                    TraceEvent::new(
                        EventKind::ChainSyncAwaitReply,
                        Direction::Sent,
                        json!({ "hold_secs": hold_secs }),
                    )
                    .with_states(MINI_PROTOCOL, state_before.as_str(), "MustReply"),
                )
                .await?;
            if *hold_secs > 0 {
                tokio::time::sleep(Duration::from_secs(*hold_secs)).await;
            }
        }

        ScriptSend::Wait { duration_secs } => {
            info!(secs = duration_secs, "Script wait");
            tokio::time::sleep(Duration::from_secs(*duration_secs)).await;
            return Ok(false);
        }

        ScriptSend::Disconnect => {
            info!("Script disconnect — closing channel");
            return Ok(true);
        }

        ScriptSend::RawBytes { bytes } => {
            send_raw(codec_send, bytes).await?;
            tracer
                .emit(TraceEvent::new(
                    EventKind::Error,
                    Direction::Sent,
                    json!({ "phase": "raw_bytes", "hex": encode_hex(bytes), "byte_len": bytes.len() }),
                ))
                .await?;
        }
        // Block-Fetch sends are only executed by blockfetch_server.rs — error if reached here.
        ScriptSend::StartBatch | ScriptSend::Block { .. } | ScriptSend::BatchDone
        | ScriptSend::NoBlocks | ScriptSend::StreamBatch { .. } | ScriptSend::CursorRange
        | ScriptSend::SendSequence { .. } => {
            anyhow::bail!("Block-Fetch send rule {:?} reached Chain-Sync execution loop", rule.send.kind_str());
        }
    }
    Ok(false)
}

// ── Raw channel I/O ───────────────────────────────────────────────────────────

/// Receive one complete Chain-Sync message from the codec.
async fn recv_request(codec_recv: &mut CodecRecv) -> anyhow::Result<ReceivedRequest> {
    let msg = codec_recv
        .recv::<Message<HeaderContent>>(1 << 20)
        .await
        .map_err(|e| anyhow::anyhow!("chain-sync server recv: {e}"))?;
    match msg {
        Message::FindIntersect(pts) => Ok(ReceivedRequest::FindIntersect(pts)),
        Message::RequestNext        => Ok(ReceivedRequest::RequestNext),
        Message::Done               => Ok(ReceivedRequest::Done),
        other => anyhow::bail!("unexpected Chain-Sync client message in server loop: {other:?}"),
    }
}

/// CBOR-encode a Chain-Sync message and send it.
async fn send_message(
    codec_send: &CodecSend,
    msg: &Message<HeaderContent>,
) -> anyhow::Result<()> {
    codec_send
        .send(msg)
        .await
        .map_err(|e| anyhow::anyhow!("chain-sync server send: {e}"))
}

/// Send arbitrary pre-encoded bytes (for adversarial raw_bytes rules).
pub async fn send_raw(codec_send: &CodecSend, bytes: &[u8]) -> anyhow::Result<()> {
    codec_send
        .send_raw(Bytes::from(bytes.to_vec()))
        .await
        .map_err(|e| anyhow::anyhow!("chain-sync server send_raw: {e}"))
}

// ── Receive event emission ────────────────────────────────────────────────────

async fn emit_receive_event(
    tracer: &Tracer,
    request: &ReceivedRequest,
    state_before: ServerCsState,
    state_after: ServerCsState,
) -> anyhow::Result<()> {
    match request {
        ReceivedRequest::FindIntersect(pts) => {
            let point_values: Vec<serde_json::Value> = pts
                .iter()
                .map(|p| crate::miniprotocols::chainsync::format_point(p))
                .collect();
            tracer
                .emit(
                    TraceEvent::new(
                        EventKind::ChainSyncFindIntersect,
                        Direction::Received,
                        json!({ "points": point_values }),
                    )
                    .with_states(MINI_PROTOCOL, state_before.as_str(), state_after.as_str()),
                )
                .await?;
        }
        ReceivedRequest::RequestNext => {
            tracer
                .emit(
                    TraceEvent::new(
                        EventKind::ChainSyncRequestNext,
                        Direction::Received,
                        json!({}),
                    )
                    .with_states(MINI_PROTOCOL, state_before.as_str(), state_after.as_str()),
                )
                .await?;
        }
        ReceivedRequest::Done => {
            tracer
                .emit(
                    TraceEvent::new(
                        EventKind::ChainSyncDone,
                        Direction::Received,
                        json!({}),
                    )
                    .with_states(MINI_PROTOCOL, state_before.as_str(), state_after.as_str()),
                )
                .await?;
        }
    }
    Ok(())
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn format_tip(tip: &Tip) -> serde_json::Value {
    let Tip(point, block_number) = tip;
    json!({
        "point":        crate::miniprotocols::chainsync::format_point(point),
        "block_number": block_number,
    })
}

fn zero_tip() -> Tip {
    Tip(Point::Origin, 0)
}

fn tip_spec_to_tip(spec: Option<&TipSpec>, fixture_tip: Option<&Tip>) -> Tip {
    match spec {
        Some(s) => {
            let pt = parse_point_str(&s.point).unwrap_or(Point::Origin);
            Tip(pt, s.block_number)
        }
        None => fixture_tip.cloned().unwrap_or_else(zero_tip),
    }
}

fn parse_point_str(s: &str) -> anyhow::Result<Point> {
    crate::scenario::parse_point(s)
}

fn entry_to_header_content(
    entry: &crate::scenario::fixture::FixtureEntry,
) -> HeaderContent {
    HeaderContent {
        variant: entry.variant,
        byron_prefix: None,
        cbor: decode_hex_str(&entry.cbor_hex),
    }
}

fn header_source_to_content(source: &HeaderSource) -> HeaderContent {
    match source {
        HeaderSource::FixtureEntry(entry) => entry_to_header_content(entry),
        HeaderSource::Literal { cbor, variant } => HeaderContent {
            variant: *variant,
            byron_prefix: None,
            cbor: cbor.clone(),
        },
    }
}

fn extract_header_info(source: &HeaderSource) -> (u64, String, u64) {
    match source {
        HeaderSource::FixtureEntry(e) => (e.slot, e.block_hash.clone(), e.block_number),
        HeaderSource::Literal { .. }  => (0, String::new(), 0),
    }
}

fn decode_hex_str(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .filter_map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok())
        .collect()
}
