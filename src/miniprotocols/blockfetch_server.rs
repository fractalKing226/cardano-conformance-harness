use std::time::{Duration, Instant};

use pallas_codec::minicbor;
use pallas_network::miniprotocols::blockfetch::Message;
use pallas_network::miniprotocols::Point;
use pallas_network::multiplexer::{AgentChannel, Error as PlexerError, MAX_SEGMENT_PAYLOAD_LENGTH};
use serde_json::json;
use tracing::{debug, info, warn};

use crate::scenario::block_fixture::{BlockFixtureChain, encode_hex as bf_hex};
use crate::scenario::response_rules::{BlockSource, ScriptRule, ScriptSend, StreamBatchSources, On};
use crate::trace::{Direction, EventKind, TraceEvent, Tracer};

const MINI_PROTOCOL: &str = "block-fetch";

// ── Protocol state tracker ────────────────────────────────────────────────────

/// Independent state tracker for Block-Fetch server trace annotation.
/// Annotation only — the execution loop never enforces spec compliance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerBfState {
    Idle,
    Busy,
    Streaming,
    Done,
}

impl ServerBfState {
    pub fn as_str(self) -> &'static str {
        match self {
            ServerBfState::Idle      => "Idle",
            ServerBfState::Busy      => "Busy",
            ServerBfState::Streaming => "Streaming",
            ServerBfState::Done      => "Done",
        }
    }
}

// ── Incoming request ──────────────────────────────────────────────────────────

#[derive(Debug)]
pub enum BlockFetchRequest {
    RequestRange(Point, Point),
    ClientDone,
}

impl BlockFetchRequest {
    fn kind_str(&self) -> &'static str {
        match self {
            BlockFetchRequest::RequestRange(..) => "request_range",
            BlockFetchRequest::ClientDone       => "done",
        }
    }

    fn matches_on(&self, on: &On) -> bool {
        matches!(on, On::Any) || match (self, on) {
            (BlockFetchRequest::RequestRange(..), On::RequestRange) => true,
            (BlockFetchRequest::ClientDone,       On::Done)         => true,
            _ => false,
        }
    }
}

// ── Summary ───────────────────────────────────────────────────────────────────

pub struct BlockFetchServerSummary {
    pub range_requests: u64,
    pub blocks_served: u64,
    pub no_blocks_responses: u64,
    pub rules_applied: u64,
    pub exit_reason: String,
    pub duration_ms: u64,
}

// ── Entry point ───────────────────────────────────────────────────────────────

/// Execute a Block-Fetch response script on a raw server-side channel.
///
/// The harness has no opinion about whether responses are spec-conformant.
/// Every send uses `enqueue_chunk` directly, bypassing Pallas's state machine.
pub async fn execute_block_fetch_script(
    channel: &mut AgentChannel,
    rules: &[ScriptRule],
    fixture: Option<&BlockFixtureChain>,
    tracer: &Tracer,
) -> anyhow::Result<BlockFetchServerSummary> {
    let started_at = Instant::now();
    let mut state = ServerBfState::Idle;
    let mut range_requests: u64 = 0;
    let mut blocks_served: u64 = 0;
    let mut no_blocks_responses: u64 = 0;
    let mut rules_applied: u64 = 0;
    let mut rule_idx = 0usize;

    tracer
        .emit(
            TraceEvent::new(
                EventKind::ServerBlockFetchStarted,
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
        let request = match recv_request(channel).await {
            Ok(r) => r,
            Err(e) => {
                warn!("Block-Fetch server recv error: {e}");
                break;
            }
        };

        // Update state and emit receive event.
        let state_before = state;
        state = match &request {
            BlockFetchRequest::RequestRange(..) => ServerBfState::Busy,
            BlockFetchRequest::ClientDone       => ServerBfState::Done,
        };

        emit_receive_event(tracer, &request, state_before, state).await?;

        if matches!(request, BlockFetchRequest::ClientDone) {
            break;
        }
        if let BlockFetchRequest::RequestRange(..) = &request { range_requests += 1; }

        // Find next matching rule.
        let rule_pos = rules[rule_idx..]
            .iter()
            .position(|r| request.matches_on(&r.on))
            .map(|p| p + rule_idx);

        let rule = match rule_pos {
            Some(pos) => {
                if !rules[pos].repeatable { rule_idx = pos + 1; }
                &rules[pos]
            }
            None => {
                anyhow::bail!(
                    "serve_block_fetch: no matching rule for {:?} (exhausted {} rules)",
                    request.kind_str(),
                    rules.len()
                );
            }
        };

        tracer
            .emit(TraceEvent::new(
                EventKind::ResponseRuleApplied,
                Direction::Internal,
                json!({ "rule_index": rule_pos.unwrap_or(0), "on": rule.on_str(), "send": rule.send.kind_str() }),
            ))
            .await?;
        rules_applied += 1;

        let done = execute_send(
            &rule.send,
            &request,
            channel,
            fixture,
            tracer,
            state,
            &mut state,
            &mut blocks_served,
            &mut no_blocks_responses,
        ).await?;

        if done { break; }
    }

    let summary = BlockFetchServerSummary {
        range_requests, blocks_served, no_blocks_responses,
        rules_applied,
        exit_reason: "completed".into(),
        duration_ms: started_at.elapsed().as_millis() as u64,
    };

    tracer
        .emit(
            TraceEvent::new(
                EventKind::ServerBlockFetchCompleted,
                Direction::Internal,
                json!({
                    "range_requests":      summary.range_requests,
                    "blocks_served":       summary.blocks_served,
                    "no_blocks_responses": summary.no_blocks_responses,
                    "rules_applied":       summary.rules_applied,
                    "exit_reason":         summary.exit_reason,
                    "duration_ms":         summary.duration_ms,
                }),
            )
            .with_protocol(MINI_PROTOCOL),
        )
        .await?;

    info!(blocks_served, duration_ms = summary.duration_ms, "Block-Fetch server complete");
    Ok(summary)
}

// ── Send execution ────────────────────────────────────────────────────────────

/// Dispatches a `ScriptSend`. Handles `SendSequence` by iterating sub-sends via
/// `execute_single_send`; delegates everything else to `execute_single_send` directly.
/// Returns `true` if the session loop should exit (only `Disconnect` sub-sends do this).
#[allow(clippy::too_many_arguments)]
async fn execute_send(
    send: &ScriptSend,
    request: &BlockFetchRequest,
    channel: &mut AgentChannel,
    fixture: Option<&BlockFixtureChain>,
    tracer: &Tracer,
    state_before: ServerBfState,
    state: &mut ServerBfState,
    blocks_served: &mut u64,
    no_blocks_responses: &mut u64,
) -> anyhow::Result<bool> {
    if let ScriptSend::SendSequence { sends } = send {
        for sub_send in sends {
            let sub_before = *state;
            if execute_single_send(sub_send, request, channel, fixture, tracer, sub_before, state, blocks_served, no_blocks_responses).await? {
                return Ok(true);
            }
        }
        return Ok(false);
    }
    execute_single_send(send, request, channel, fixture, tracer, state_before, state, blocks_served, no_blocks_responses).await
}

#[allow(clippy::too_many_arguments)]
async fn execute_single_send(
    send: &ScriptSend,
    request: &BlockFetchRequest,
    channel: &mut AgentChannel,
    fixture: Option<&BlockFixtureChain>,
    tracer: &Tracer,
    state_before: ServerBfState,
    state: &mut ServerBfState,
    blocks_served: &mut u64,
    no_blocks_responses: &mut u64,
) -> anyhow::Result<bool> {
    match send {
        ScriptSend::NoBlocks => {
            send_message(channel, &Message::NoBlocks).await?;
            *state = ServerBfState::Idle;
            *no_blocks_responses += 1;
            tracer
                .emit(
                    TraceEvent::new(EventKind::BlockFetchNoBlocks, Direction::Sent, json!({}))
                        .with_states(MINI_PROTOCOL, state_before.as_str(), "Idle"),
                )
                .await?;
        }

        ScriptSend::StartBatch => {
            send_message(channel, &Message::StartBatch).await?;
            *state = ServerBfState::Streaming;
            tracer
                .emit(
                    TraceEvent::new(EventKind::BlockFetchStartBatch, Direction::Sent, json!({}))
                        .with_states(MINI_PROTOCOL, state_before.as_str(), "Streaming"),
                )
                .await?;
        }

        ScriptSend::Block { source } => {
            let body = block_source_bytes(source);
            let (slot, hash) = block_source_ident(source);
            send_message(channel, &Message::Block { body }).await?;
            *blocks_served += 1;
            debug!(slot, "Served block");
            tracer
                .emit(
                    TraceEvent::new(
                        EventKind::BlockFetchBlock,
                        Direction::Sent,
                        json!({ "slot": slot, "block_hash": hash, "cbor_len": block_source_len(source) }),
                    )
                    .with_states(MINI_PROTOCOL, "Streaming", "Streaming"),
                )
                .await?;
        }

        ScriptSend::BatchDone => {
            send_message(channel, &Message::BatchDone).await?;
            *state = ServerBfState::Idle;
            tracer
                .emit(
                    TraceEvent::new(
                        EventKind::BlockFetchBatchDone,
                        Direction::Sent,
                        json!({ "blocks_in_batch": *blocks_served }),
                    )
                    .with_states(MINI_PROTOCOL, "Streaming", "Idle"),
                )
                .await?;
        }

        ScriptSend::StreamBatch { sources } => {
            let entries: Vec<(Vec<u8>, u64, String)> = match sources {
                StreamBatchSources::Explicit(block_sources) => block_sources
                    .iter()
                    .map(|s| (block_source_bytes(s), block_source_slot(s), block_source_hash(s)))
                    .collect(),
                StreamBatchSources::FromRequest => {
                    if let BlockFetchRequest::RequestRange(from, to) = request {
                        match fixture.and_then(|f| f.find_range(from, to)) {
                            Some(entries) => entries
                                .iter()
                                .map(|e| {
                                    let bytes = e.body_bytes();
                                    let len = bytes.len();
                                    let _ = len;
                                    (bytes, e.slot, e.block_hash.clone())
                                })
                                .collect(),
                            None => {
                                // Unsatisfiable range → send NoBlocks.
                                send_message(channel, &Message::NoBlocks).await?;
                                *state = ServerBfState::Idle;
                                *no_blocks_responses += 1;
                                tracer
                                    .emit(
                                        TraceEvent::new(EventKind::BlockFetchNoBlocks, Direction::Sent, json!({}))
                                            .with_states(MINI_PROTOCOL, state_before.as_str(), "Idle"),
                                    )
                                    .await?;
                                return Ok(false);
                            }
                        }
                    } else {
                        vec![]
                    }
                }
            };

            if entries.is_empty() {
                send_message(channel, &Message::NoBlocks).await?;
                *state = ServerBfState::Idle;
                *no_blocks_responses += 1;
                tracer
                    .emit(
                        TraceEvent::new(EventKind::BlockFetchNoBlocks, Direction::Sent, json!({}))
                            .with_states(MINI_PROTOCOL, state_before.as_str(), "Idle"),
                    )
                    .await?;
                return Ok(false);
            }

            // StartBatch
            send_message(channel, &Message::StartBatch).await?;
            *state = ServerBfState::Streaming;
            tracer
                .emit(
                    TraceEvent::new(EventKind::BlockFetchStartBatch, Direction::Sent, json!({}))
                        .with_states(MINI_PROTOCOL, state_before.as_str(), "Streaming"),
                )
                .await?;

            let batch_count = entries.len() as u64;
            for (body, slot, hash) in &entries {
                let cbor_len = body.len();
                send_message(channel, &Message::Block { body: body.clone() }).await?;
                *blocks_served += 1;
                debug!(slot, "Served block in stream_batch");
                tracer
                    .emit(
                        TraceEvent::new(
                            EventKind::BlockFetchBlock,
                            Direction::Sent,
                            json!({ "slot": slot, "block_hash": hash, "cbor_len": cbor_len }),
                        )
                        .with_states(MINI_PROTOCOL, "Streaming", "Streaming"),
                    )
                    .await?;
            }

            // BatchDone
            send_message(channel, &Message::BatchDone).await?;
            *state = ServerBfState::Idle;
            tracer
                .emit(
                    TraceEvent::new(
                        EventKind::BlockFetchBatchDone,
                        Direction::Sent,
                        json!({ "blocks_in_batch": batch_count }),
                    )
                    .with_states(MINI_PROTOCOL, "Streaming", "Idle"),
                )
                .await?;
        }

        ScriptSend::Wait { duration_secs } => {
            tokio::time::sleep(Duration::from_secs(*duration_secs)).await;
            return Ok(false);
        }

        ScriptSend::Disconnect => {
            info!("Block-Fetch script disconnect");
            return Ok(true);
        }

        ScriptSend::RawBytes { bytes } => {
            send_raw(channel, bytes).await?;
            tracer
                .emit(TraceEvent::new(
                    EventKind::Error,
                    Direction::Sent,
                    json!({ "phase": "raw_bytes", "hex": bf_hex(bytes), "byte_len": bytes.len() }),
                ))
                .await?;
        }

        // SendSequence is handled by execute_send before reaching here.
        ScriptSend::SendSequence { .. } =>
            anyhow::bail!("SendSequence reached execute_single_send — this is a bug"),

        // Chain-Sync sends are only executed by chainsync_server.rs.
        other => {
            anyhow::bail!("Chain-Sync send rule {:?} reached Block-Fetch execution loop", other.kind_str());
        }
    }
    Ok(false)
}

// ── Raw channel I/O ───────────────────────────────────────────────────────────

async fn recv_request(channel: &mut AgentChannel) -> anyhow::Result<BlockFetchRequest> {
    let mut buf = Vec::new();
    loop {
        let chunk = channel
            .dequeue_chunk()
            .await
            .map_err(|e: PlexerError| anyhow::anyhow!("bf channel recv: {e}"))?;
        buf.extend_from_slice(&chunk);
        let mut decoder = minicbor::Decoder::new(&buf);
        match decoder.decode::<Message>() {
            Ok(msg) => {
                return Ok(match msg {
                    Message::RequestRange { range: (from, to) } => BlockFetchRequest::RequestRange(from, to),
                    Message::ClientDone                          => BlockFetchRequest::ClientDone,
                    other => anyhow::bail!("unexpected Block-Fetch client message: {other:?}"),
                });
            }
            Err(ref e) if e.is_end_of_input() => continue,
            Err(e) => anyhow::bail!("Block-Fetch decode error: {e}"),
        }
    }
}

async fn send_message(channel: &mut AgentChannel, msg: &Message) -> anyhow::Result<()> {
    let bytes = minicbor::to_vec(msg)
        .map_err(|e| anyhow::anyhow!("Block-Fetch CBOR encode: {e}"))?;
    send_raw(channel, &bytes).await
}

async fn send_raw(channel: &mut AgentChannel, bytes: &[u8]) -> anyhow::Result<()> {
    for chunk in bytes.chunks(MAX_SEGMENT_PAYLOAD_LENGTH) {
        channel
            .enqueue_chunk(chunk.to_vec())
            .await
            .map_err(|e: PlexerError| anyhow::anyhow!("bf channel send: {e}"))?;
    }
    Ok(())
}

// ── Receive event emission ────────────────────────────────────────────────────

async fn emit_receive_event(
    tracer: &Tracer,
    request: &BlockFetchRequest,
    state_before: ServerBfState,
    state_after: ServerBfState,
) -> anyhow::Result<()> {
    match request {
        BlockFetchRequest::RequestRange(from, to) => {
            tracer
                .emit(
                    TraceEvent::new(
                        EventKind::BlockFetchRequestRange,
                        Direction::Received,
                        json!({
                            "from": format_point(from),
                            "to":   format_point(to),
                        }),
                    )
                    .with_states(MINI_PROTOCOL, state_before.as_str(), state_after.as_str()),
                )
                .await?;
        }
        BlockFetchRequest::ClientDone => {
            tracer
                .emit(
                    TraceEvent::new(
                        EventKind::BlockFetchClientDone,
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

fn format_point(p: &Point) -> serde_json::Value {
    match p {
        Point::Origin => serde_json::json!("origin"),
        Point::Specific(slot, hash) => serde_json::json!({ "slot": slot, "hash": bf_hex(hash) }),
    }
}

fn block_source_bytes(s: &BlockSource) -> Vec<u8> {
    match s {
        BlockSource::FixtureEntry(e) => e.body_bytes(),
        BlockSource::Literal(b) => b.clone(),
    }
}

fn block_source_slot(s: &BlockSource) -> u64 {
    match s { BlockSource::FixtureEntry(e) => e.slot, BlockSource::Literal(_) => 0 }
}

fn block_source_hash(s: &BlockSource) -> String {
    match s { BlockSource::FixtureEntry(e) => e.block_hash.clone(), BlockSource::Literal(_) => String::new() }
}

fn block_source_len(s: &BlockSource) -> usize {
    match s { BlockSource::FixtureEntry(e) => e.block_cbor_hex.len() / 2, BlockSource::Literal(b) => b.len() }
}

fn block_source_ident(s: &BlockSource) -> (u64, String) {
    (block_source_slot(s), block_source_hash(s))
}
