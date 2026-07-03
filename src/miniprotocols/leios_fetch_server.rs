use std::time::{Duration, Instant};

use bytes::Bytes;
use net_core::mux::{CodecRecv, CodecSend};
use net_core::protocols::leios_fetch::{LeiosFetch, Message};
use net_core::protocols::{Role, Runner};
use net_core::types::Point as NcPoint;
use serde::Deserialize;
use serde_json::json;
use tracing::{info, warn};

use crate::trace::{Direction, EventKind, TraceEvent, Tracer};

pub const LEIOS_FETCH_PROTOCOL: u16 = net_core::protocols::leios_fetch::PROTOCOL_ID;

const MINI_PROTOCOL: &str = "leios-fetch";

// ── Rule types ────────────────────────────────────────────────────────────────

/// A scripted response rule for the LeiosFetch server.
#[derive(Debug, Clone, Deserialize)]
pub struct LeiosFetchRule {
    /// When to fire: "fetch_block", "done", or "any".
    pub on: String,
    /// What to send.
    pub send: LeiosFetchSend,
    /// Whether this rule is repeatable (re-matches without advancing the index).
    #[serde(default)]
    pub repeatable: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LeiosFetchSend {
    /// "block", "wait", "disconnect", or "raw_bytes".
    pub kind: String,
    /// For "block": hex-encoded raw EB bytes.
    pub block_bytes: Option<String>,
    /// For "wait": seconds to sleep.
    pub duration_secs: Option<u64>,
    /// For "raw_bytes": hex-encoded bytes to send verbatim.
    pub bytes: Option<String>,
}

// ── Incoming request ──────────────────────────────────────────────────────────

#[derive(Debug)]
enum FetchRequest {
    FetchBlock(NcPoint),
    Done,
}

impl FetchRequest {
    fn kind_str(&self) -> &'static str {
        match self {
            FetchRequest::FetchBlock(_) => "fetch_block",
            FetchRequest::Done          => "done",
        }
    }

    fn matches_on(&self, on: &str) -> bool {
        on == "any" || match (self, on) {
            (FetchRequest::FetchBlock(_), "fetch_block") => true,
            (FetchRequest::Done,          "done")        => true,
            _ => false,
        }
    }
}

// ── Summary ───────────────────────────────────────────────────────────────────

pub struct LeiosFetchServerSummary {
    pub blocks_served: u64,
    pub exit_reason: String,
    pub duration_ms: u64,
}

// ── Entry point ───────────────────────────────────────────────────────────────

/// Execute a scripted LeiosFetch server session.
pub async fn execute_leios_fetch_script(
    codec_send: CodecSend,
    codec_recv: CodecRecv,
    rules: Vec<LeiosFetchRule>,
    tracer: &Tracer,
) -> anyhow::Result<LeiosFetchServerSummary> {
    let started_at = Instant::now();
    let mut blocks_served: u64 = 0;
    let mut runner = Runner::<LeiosFetch>::new(Role::Server, codec_send, codec_recv);
    let mut rule_idx = 0usize;

    tracer
        .emit(
            TraceEvent::new(EventKind::ServerLeiosFetchStarted, Direction::Internal, json!({}))
                .with_protocol(MINI_PROTOCOL),
        )
        .await?;

    loop {
        // Receive the next client message.
        let msg = match runner.recv().await {
            Ok(m) => m,
            Err(e) => {
                warn!("LeiosFetch server recv error: {e}");
                break;
            }
        };

        let request = match msg {
            Message::MsgLeiosBlockRequest { point } => FetchRequest::FetchBlock(point),
            Message::MsgDone => FetchRequest::Done,
            other => {
                warn!("LeiosFetch server: unexpected message {other:?}");
                continue;
            }
        };

        if matches!(request, FetchRequest::Done) {
            tracer
                .emit(
                    TraceEvent::new(EventKind::LeiosFetchDone, Direction::Received, json!({}))
                        .with_protocol(MINI_PROTOCOL),
                )
                .await?;
            break;
        }

        // Find the next matching rule.
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
                    "serve_leios_fetch: no matching rule for {:?} (exhausted {} rules)",
                    request.kind_str(),
                    rules.len()
                );
            }
        };

        tracer
            .emit(TraceEvent::new(
                EventKind::ResponseRuleApplied,
                Direction::Internal,
                json!({ "on": rule.on, "send": rule.send.kind }),
            ))
            .await?;

        let done = execute_send_rule(&rule.send, &mut runner, tracer, &mut blocks_served).await?;
        if done {
            break;
        }
    }

    let summary = LeiosFetchServerSummary {
        blocks_served,
        exit_reason: "completed".into(),
        duration_ms: started_at.elapsed().as_millis() as u64,
    };

    tracer
        .emit(
            TraceEvent::new(
                EventKind::ServerLeiosFetchCompleted,
                Direction::Internal,
                json!({
                    "blocks_served": summary.blocks_served,
                    "exit_reason":   summary.exit_reason,
                    "duration_ms":   summary.duration_ms,
                }),
            )
            .with_protocol(MINI_PROTOCOL),
        )
        .await?;

    info!(
        blocks_served = summary.blocks_served,
        duration_ms = summary.duration_ms,
        "LeiosFetch server session complete"
    );

    Ok(summary)
}

// ── Send execution ────────────────────────────────────────────────────────────

async fn execute_send_rule(
    send: &LeiosFetchSend,
    runner: &mut Runner<LeiosFetch>,
    tracer: &Tracer,
    blocks_served: &mut u64,
) -> anyhow::Result<bool> {
    match send.kind.as_str() {
        "block" => {
            let block = decode_hex_opt(&send.block_bytes, "block_bytes")?;
            let block_len = block.len();
            runner
                .send(&Message::MsgLeiosBlock { block })
                .await
                .map_err(|e| anyhow::anyhow!("leios_fetch_server send: {e}"))?;
            *blocks_served += 1;
            tracer
                .emit(
                    TraceEvent::new(
                        EventKind::LeiosFetchBlock,
                        Direction::Sent,
                        json!({ "block_len": block_len }),
                    )
                    .with_protocol(MINI_PROTOCOL),
                )
                .await?;
        }

        "wait" => {
            let secs = send.duration_secs.unwrap_or(0);
            if secs > 0 {
                tokio::time::sleep(Duration::from_secs(secs)).await;
            }
            return Ok(false);
        }

        "disconnect" => {
            info!("LeiosFetch server: script disconnect");
            return Ok(true);
        }

        "raw_bytes" => {
            let raw = decode_hex_opt(&send.bytes, "bytes")?;
            runner
                .send_raw(Bytes::from(raw))
                .await
                .map_err(|e| anyhow::anyhow!("leios_fetch_server send_raw: {e}"))?;
            tracer
                .emit(
                    TraceEvent::new(
                        EventKind::Error,
                        Direction::Sent,
                        json!({ "phase": "raw_bytes", "kind": "leios_fetch_server" }),
                    )
                    .with_protocol(MINI_PROTOCOL),
                )
                .await?;
        }

        other => {
            anyhow::bail!("leios_fetch_server: unknown send kind \"{other}\"");
        }
    }
    Ok(false)
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn decode_hex_opt(opt: &Option<String>, field: &str) -> anyhow::Result<Vec<u8>> {
    let s = opt
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("leios_fetch_server: missing {field}"))?;
    decode_hex(s).map_err(|e| anyhow::anyhow!("leios_fetch_server: {field}: {e}"))
}

fn decode_hex(s: &str) -> anyhow::Result<Vec<u8>> {
    if s.len() % 2 != 0 {
        anyhow::bail!("odd-length hex string");
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).map_err(|e| anyhow::anyhow!("{e}")))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use net_core::bearer::mem::MemBearer;
    use net_core::mux::scheduler::{AnyScheduler, TrafficClass};
    use net_core::mux::{Mux, MuxConfig, ProtocolConfig, RunningMux, MODE_INITIATOR, MODE_RESPONDER};
    use net_core::protocols::leios_fetch::{LeiosFetch, Message, INGRESS_LIMIT};
    use net_core::protocols::{Role, Runner};
    use net_core::types::Point as NcPoint;
    use tempfile::NamedTempFile;

    // ── Test helpers ──────────────────────────────────────────────────────────

    fn make_pair() -> (CodecSend, CodecRecv, CodecSend, CodecRecv, RunningMux, RunningMux) {
        let (bearer_cli, bearer_srv) = MemBearer::pair();

        let proto = ProtocolConfig {
            id: LEIOS_FETCH_PROTOCOL,
            traffic_class: TrafficClass::Default(1),
            ingress_limit: INGRESS_LIMIT,
            egress_queue_size: 16,
        };

        let mut cli_mux = Mux::new(MuxConfig::default(), AnyScheduler::default(), MODE_INITIATOR);
        let (cli_send, cli_recv) = cli_mux.register(&proto);
        let cli_running = cli_mux.run(bearer_cli);

        let mut srv_mux = Mux::new(MuxConfig::default(), AnyScheduler::default(), MODE_RESPONDER);
        let (srv_send, srv_recv) = srv_mux.register(&proto);
        let srv_running = srv_mux.run(bearer_srv);

        (
            CodecSend::new(cli_send), CodecRecv::new(cli_recv),
            CodecSend::new(srv_send), CodecRecv::new(srv_recv),
            cli_running, srv_running,
        )
    }

    fn block_rule(block_hex: &str) -> LeiosFetchRule {
        LeiosFetchRule {
            on: "fetch_block".into(),
            send: LeiosFetchSend {
                kind: "block".into(),
                block_bytes: Some(block_hex.into()),
                duration_secs: None,
                bytes: None,
            },
            repeatable: false,
        }
    }

    fn repeatable_block_rule(block_hex: &str) -> LeiosFetchRule {
        LeiosFetchRule { repeatable: true, ..block_rule(block_hex) }
    }

    fn origin_point() -> NcPoint { NcPoint::Origin }

    async fn tracer() -> (crate::trace::Tracer, NamedTempFile) {
        let tmp = NamedTempFile::new().unwrap();
        let t = crate::trace::Tracer::open(tmp.path()).await.unwrap();
        (t, tmp)
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    /// Client sends FetchBlock, server responds with the scripted block bytes.
    #[tokio::test]
    async fn block_served_on_fetch_request() {
        let (cli_send, cli_recv, srv_send, srv_recv, cli_mux, srv_mux) = make_pair();
        let (trc, _tmp) = tracer().await;

        // "a0" = CBOR empty map — block bytes are spliced raw into the CBOR stream
        // and must be valid CBOR themselves.
        let rules = vec![block_rule("a0")];

        let (srv_result, block_bytes) = tokio::join!(
            execute_leios_fetch_script(srv_send, srv_recv, rules, &trc),
            async {
                let mut runner = Runner::<LeiosFetch>::new(Role::Client, cli_send, cli_recv);
                runner.send(&Message::MsgLeiosBlockRequest { point: origin_point() }).await.unwrap();
                let msg = runner.recv().await.unwrap();
                match msg {
                    Message::MsgLeiosBlock { block } => block,
                    other => panic!("expected MsgLeiosBlock, got {other:?}"),
                }
            }
        );

        let summary = srv_result.unwrap();
        assert_eq!(summary.blocks_served, 1);
        assert_eq!(block_bytes, vec![0xa0]); // 0xa0 = CBOR empty map

        cli_mux.abort();
        srv_mux.abort();
    }

    /// Repeatable rule fires on every request without advancing the rule index.
    #[tokio::test]
    async fn repeatable_rule_fires_multiple_times() {
        let (cli_send, cli_recv, srv_send, srv_recv, cli_mux, srv_mux) = make_pair();
        let (trc, _tmp) = tracer().await;

        let rules = vec![repeatable_block_rule("a0")] ; // "a0" = CBOR empty map

        let (srv_result, all_blocks) = tokio::join!(
            execute_leios_fetch_script(srv_send, srv_recv, rules, &trc),
            async {
                let mut runner = Runner::<LeiosFetch>::new(Role::Client, cli_send, cli_recv);
                let mut blocks = Vec::new();
                for _ in 0..3 {
                    runner.send(&Message::MsgLeiosBlockRequest { point: origin_point() }).await.unwrap();
                    match runner.recv().await.unwrap() {
                        Message::MsgLeiosBlock { block } => blocks.push(block),
                        other => panic!("unexpected {other:?}"),
                    }
                }
                runner.send(&Message::MsgDone).await.unwrap();
                blocks
            }
        );

        assert_eq!(srv_result.unwrap().blocks_served, 3);
        assert_eq!(all_blocks.len(), 3);
        assert!(all_blocks.iter().all(|b| b == &[0xa0]), "all blocks should be identical");

        cli_mux.abort();
        srv_mux.abort();
    }

    /// Client sends MsgDone immediately: server exits with blocks_served = 0.
    #[tokio::test]
    async fn done_from_client_terminates_session() {
        let (cli_send, cli_recv, srv_send, srv_recv, cli_mux, srv_mux) = make_pair();
        let (trc, _tmp) = tracer().await;

        let (srv_result, ()) = tokio::join!(
            execute_leios_fetch_script(srv_send, srv_recv, vec![], &trc),
            async {
                let mut runner = Runner::<LeiosFetch>::new(Role::Client, cli_send, cli_recv);
                runner.send(&Message::MsgDone).await.unwrap();
            }
        );

        assert_eq!(srv_result.unwrap().blocks_served, 0);

        cli_mux.abort();
        srv_mux.abort();
    }

    /// When all rules are consumed the server returns an error.
    #[tokio::test]
    async fn exhausted_rules_return_error() {
        let (cli_send, cli_recv, srv_send, srv_recv, cli_mux, srv_mux) = make_pair();
        let (trc, _tmp) = tracer().await;

        let rules = vec![block_rule("a0")]; // only one rule, client sends two requests

        let (srv_result, _) = tokio::join!(
            execute_leios_fetch_script(srv_send, srv_recv, rules, &trc),
            async {
                let mut runner = Runner::<LeiosFetch>::new(Role::Client, cli_send, cli_recv);
                runner.send(&Message::MsgLeiosBlockRequest { point: origin_point() }).await.unwrap();
                let _ = runner.recv().await; // consume the first block
                // Send a second request — server has no rule for it
                let _ = runner.send(&Message::MsgLeiosBlockRequest { point: origin_point() }).await;
                // Server will error; client channel closes
                let _ = runner.recv().await;
            }
        );

        assert!(srv_result.is_err(), "server should error when rules are exhausted");

        cli_mux.abort();
        srv_mux.abort();
    }
}
