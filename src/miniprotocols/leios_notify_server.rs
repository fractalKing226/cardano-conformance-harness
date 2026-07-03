use std::time::{Duration, Instant};

use bytes::Bytes;
use net_core::mux::{CodecRecv, CodecSend};
use net_core::protocols::leios_notify::{LeiosNotify, Message};
use net_core::protocols::{Role, Runner};
use net_core::types::{Point as NcPoint, Vote, WrappedHeader};
use serde::Deserialize;
use serde_json::json;
use tracing::{info, warn};

use crate::trace::{Direction, EventKind, TraceEvent, Tracer};

pub const LEIOS_NOTIFY_PROTOCOL: u16 = net_core::protocols::leios_notify::PROTOCOL_ID;

const MINI_PROTOCOL: &str = "leios-notify";

// ── Action types ──────────────────────────────────────────────────────────────

/// One scripted action the server takes in response to `MsgLeiosNotificationRequestNext`.
#[derive(Debug, Clone, Deserialize)]
pub struct LeiosNotifyAction {
    pub kind: String,
    /// For "block_announcement": hex-encoded raw header bytes.
    pub header_bytes: Option<String>,
    /// For "block_offer" and "block_txs_offer": "slot:hex_hash" point.
    pub point: Option<String>,
    /// For "block_offer": size in bytes.
    pub eb_size: Option<u32>,
    /// For "votes": array of vote objects.
    pub votes: Option<Vec<VoteSpec>>,
    /// For "wait": seconds to sleep before responding.
    pub duration_secs: Option<u64>,
    /// For "raw_bytes": hex-encoded bytes to send verbatim.
    pub bytes: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct VoteSpec {
    pub slot: u64,
    pub eb_hash: String,
    pub voter_id: u16,
    pub signature: String,
}

// ── Summary ───────────────────────────────────────────────────────────────────

pub struct LeiosNotifyServerSummary {
    pub notifications_sent: u64,
    pub exit_reason: String,
    pub duration_ms: u64,
}

// ── Entry point ───────────────────────────────────────────────────────────────

/// Execute a scripted LeiosNotify server session.
///
/// For each `MsgLeiosNotificationRequestNext` from the client, the next action
/// from `actions` is executed. When the list is exhausted or `MsgDone` is
/// received, the session ends cleanly. Adversarial `raw_bytes` actions allow
/// sending malformed wire data.
pub async fn execute_leios_notify_script(
    codec_send: CodecSend,
    codec_recv: CodecRecv,
    actions: Vec<LeiosNotifyAction>,
    tracer: &Tracer,
) -> anyhow::Result<LeiosNotifyServerSummary> {
    let started_at = Instant::now();
    let mut notifications_sent: u64 = 0;
    let mut runner = Runner::<LeiosNotify>::new(Role::Server, codec_send, codec_recv);
    let mut action_iter = actions.into_iter();

    tracer
        .emit(
            TraceEvent::new(EventKind::ServerLeiosNotifyStarted, Direction::Internal, json!({}))
                .with_protocol(MINI_PROTOCOL),
        )
        .await?;

    loop {
        // Wait for a client message (RequestNext or Done).
        let msg = match runner.recv().await {
            Ok(m) => m,
            Err(e) => {
                warn!("LeiosNotify server recv error: {e}");
                break;
            }
        };

        match msg {
            Message::MsgDone => {
                tracer
                    .emit(
                        TraceEvent::new(EventKind::LeiosNotifyDone, Direction::Received, json!({}))
                            .with_protocol(MINI_PROTOCOL),
                    )
                    .await?;
                break;
            }
            Message::MsgLeiosNotificationRequestNext => {
                // Find the next action to execute.
                let action = match action_iter.next() {
                    Some(a) => a,
                    None => {
                        // No more actions — close the session.
                        info!("LeiosNotify server: actions exhausted, closing");
                        break;
                    }
                };

                let done = execute_action(&action, &mut runner, tracer, &mut notifications_sent).await?;
                if done {
                    break;
                }
            }
            other => {
                warn!("LeiosNotify server: unexpected message {other:?}");
            }
        }
    }

    let summary = LeiosNotifyServerSummary {
        notifications_sent,
        exit_reason: "completed".into(),
        duration_ms: started_at.elapsed().as_millis() as u64,
    };

    tracer
        .emit(
            TraceEvent::new(
                EventKind::ServerLeiosNotifyCompleted,
                Direction::Internal,
                json!({
                    "notifications_sent": summary.notifications_sent,
                    "exit_reason":        summary.exit_reason,
                    "duration_ms":        summary.duration_ms,
                }),
            )
            .with_protocol(MINI_PROTOCOL),
        )
        .await?;

    info!(
        notifications_sent = summary.notifications_sent,
        duration_ms = summary.duration_ms,
        "LeiosNotify server session complete"
    );

    Ok(summary)
}

// ── Action execution ──────────────────────────────────────────────────────────

/// Execute one scripted send action. Returns `true` if the session should end.
async fn execute_action(
    action: &LeiosNotifyAction,
    runner: &mut Runner<LeiosNotify>,
    tracer: &Tracer,
    notifications_sent: &mut u64,
) -> anyhow::Result<bool> {
    match action.kind.as_str() {
        "block_announcement" => {
            let raw = decode_hex_opt(&action.header_bytes, "header_bytes")?;
            let header = WrappedHeader::opaque(raw);
            runner
                .send(&Message::MsgLeiosBlockAnnouncement { header })
                .await
                .map_err(|e| anyhow::anyhow!("leios_notify_server send: {e}"))?;
            *notifications_sent += 1;
            tracer
                .emit(
                    TraceEvent::new(EventKind::LeiosNotifyBlockAnnouncement, Direction::Sent, json!({}))
                        .with_protocol(MINI_PROTOCOL),
                )
                .await?;
        }

        "block_offer" => {
            let point = parse_point_opt(&action.point, "point")?;
            let eb_size = action.eb_size.unwrap_or(0);
            runner
                .send(&Message::MsgLeiosBlockOffer { point, eb_size })
                .await
                .map_err(|e| anyhow::anyhow!("leios_notify_server send: {e}"))?;
            *notifications_sent += 1;
            tracer
                .emit(
                    TraceEvent::new(
                        EventKind::LeiosNotifyBlockOffer,
                        Direction::Sent,
                        json!({ "eb_size": eb_size }),
                    )
                    .with_protocol(MINI_PROTOCOL),
                )
                .await?;
        }

        "block_txs_offer" => {
            let point = parse_point_opt(&action.point, "point")?;
            runner
                .send(&Message::MsgLeiosBlockTxsOffer { point })
                .await
                .map_err(|e| anyhow::anyhow!("leios_notify_server send: {e}"))?;
            *notifications_sent += 1;
            tracer
                .emit(
                    TraceEvent::new(EventKind::LeiosNotifyBlockTxsOffer, Direction::Sent, json!({}))
                        .with_protocol(MINI_PROTOCOL),
                )
                .await?;
        }

        "votes" => {
            let votes = build_votes(action.votes.as_deref().unwrap_or(&[]))?;
            let count = votes.len();
            runner
                .send(&Message::MsgLeiosVotes { votes })
                .await
                .map_err(|e| anyhow::anyhow!("leios_notify_server send: {e}"))?;
            *notifications_sent += 1;
            tracer
                .emit(
                    TraceEvent::new(
                        EventKind::LeiosNotifyVotes,
                        Direction::Sent,
                        json!({ "count": count }),
                    )
                    .with_protocol(MINI_PROTOCOL),
                )
                .await?;
        }

        "wait" => {
            let secs = action.duration_secs.unwrap_or(0);
            if secs > 0 {
                tokio::time::sleep(Duration::from_secs(secs)).await;
            }
            // wait does not consume an action slot — caller loops back to recv
            return Ok(false);
        }

        "disconnect" => {
            info!("LeiosNotify server: script disconnect");
            return Ok(true);
        }

        "raw_bytes" => {
            let raw = decode_hex_opt(&action.bytes, "bytes")?;
            runner
                .send_raw(Bytes::from(raw))
                .await
                .map_err(|e| anyhow::anyhow!("leios_notify_server send_raw: {e}"))?;
            tracer
                .emit(
                    TraceEvent::new(
                        EventKind::Error,
                        Direction::Sent,
                        json!({ "phase": "raw_bytes", "kind": "leios_notify_server" }),
                    )
                    .with_protocol(MINI_PROTOCOL),
                )
                .await?;
        }

        other => {
            anyhow::bail!("leios_notify_server: unknown action kind \"{other}\"");
        }
    }
    Ok(false)
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn decode_hex_opt(opt: &Option<String>, field: &str) -> anyhow::Result<Vec<u8>> {
    let s = opt
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("leios_notify_server: missing {field}"))?;
    decode_hex(s).map_err(|e| anyhow::anyhow!("leios_notify_server: {field}: {e}"))
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

fn parse_point_opt(opt: &Option<String>, field: &str) -> anyhow::Result<NcPoint> {
    let s = opt
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("leios_notify_server: missing {field}"))?;
    parse_nc_point(s).map_err(|e| anyhow::anyhow!("leios_notify_server: {field}: {e}"))
}

fn parse_nc_point(s: &str) -> anyhow::Result<NcPoint> {
    if s == "origin" {
        return Ok(NcPoint::Origin);
    }
    let (slot_str, hash_str) = s
        .split_once(':')
        .ok_or_else(|| anyhow::anyhow!("expected \"origin\" or \"slot:hex_hash\""))?;
    let slot: u64 = slot_str.parse().map_err(|e| anyhow::anyhow!("invalid slot: {e}"))?;
    let hash_vec = decode_hex(hash_str)?;
    if hash_vec.len() != 32 {
        anyhow::bail!("point hash must be 32 bytes, got {}", hash_vec.len());
    }
    let mut hash = [0u8; 32];
    hash.copy_from_slice(&hash_vec);
    Ok(NcPoint::Specific { slot, hash })
}

#[cfg(test)]
mod tests {
    use super::*;
    use net_core::bearer::mem::MemBearer;
    use net_core::mux::scheduler::{AnyScheduler, TrafficClass};
    use net_core::mux::{Mux, MuxConfig, ProtocolConfig, RunningMux, MODE_INITIATOR, MODE_RESPONDER};
    use net_core::protocols::leios_notify::INGRESS_LIMIT;
    use tempfile::NamedTempFile;

    // ── Test helpers ──────────────────────────────────────────────────────────

    /// Creates an in-process mux pair: client (INITIATOR) and server (RESPONDER),
    /// both registered on the LeiosNotify protocol channel.
    fn make_pair() -> (CodecSend, CodecRecv, CodecSend, CodecRecv, RunningMux, RunningMux) {
        let (bearer_cli, bearer_srv) = MemBearer::pair();

        let proto = ProtocolConfig {
            id: LEIOS_NOTIFY_PROTOCOL,
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

    fn action(kind: &str) -> LeiosNotifyAction {
        LeiosNotifyAction {
            kind: kind.into(),
            // "f6" is CBOR null — WrappedHeader::encode writes raw bytes directly
            // into the CBOR stream, so the raw bytes must themselves be valid CBOR.
            header_bytes: if kind == "block_announcement" { Some("f6".into()) } else { None },
            point: if matches!(kind, "block_offer" | "block_txs_offer") {
                Some("1000:".to_string() + &"aa".repeat(32))
            } else { None },
            eb_size: if kind == "block_offer" { Some(512) } else { None },
            votes: None,
            duration_secs: None,
            bytes: None,
        }
    }

    async fn tracer() -> (Tracer, NamedTempFile) {
        let tmp = NamedTempFile::new().unwrap();
        let t = Tracer::open(tmp.path()).await.unwrap();
        (t, tmp)
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    /// One block_announcement action: client sends RequestNext, receives the announcement.
    #[tokio::test]
    async fn block_announcement_served_on_request_next() {
        let (cli_send, cli_recv, srv_send, srv_recv, cli_mux, srv_mux) = make_pair();
        let (trc, _tmp) = tracer().await;

        let actions = vec![action("block_announcement")];

        let (srv_result, cli_msg) = tokio::join!(
            execute_leios_notify_script(srv_send, srv_recv, actions, &trc),
            async {
                let mut runner = Runner::<LeiosNotify>::new(Role::Client, cli_send, cli_recv);
                runner.send(&Message::MsgLeiosNotificationRequestNext).await.unwrap();
                runner.recv().await.unwrap()
            }
        );

        let summary = srv_result.unwrap();
        assert_eq!(summary.notifications_sent, 1);

        match cli_msg {
            Message::MsgLeiosBlockAnnouncement { header } => {
                assert_eq!(header.raw, vec![0xf6]); // 0xf6 = CBOR null
            }
            other => panic!("expected MsgLeiosBlockAnnouncement, got {other:?}"),
        }

        cli_mux.abort();
        srv_mux.abort();
    }

    /// Three actions in sequence: announcement, offer, txs_offer.
    #[tokio::test]
    async fn multiple_actions_served_in_order() {
        let (cli_send, cli_recv, srv_send, srv_recv, cli_mux, srv_mux) = make_pair();
        let (trc, _tmp) = tracer().await;

        let actions = vec![
            action("block_announcement"),
            action("block_offer"),
            action("block_txs_offer"),
        ];

        let (srv_result, received) = tokio::join!(
            execute_leios_notify_script(srv_send, srv_recv, actions, &trc),
            async {
                let mut runner = Runner::<LeiosNotify>::new(Role::Client, cli_send, cli_recv);
                let mut msgs = Vec::new();
                for _ in 0..3 {
                    runner.send(&Message::MsgLeiosNotificationRequestNext).await.unwrap();
                    msgs.push(runner.recv().await.unwrap());
                }
                msgs
            }
        );

        assert_eq!(srv_result.unwrap().notifications_sent, 3);
        assert!(matches!(received[0], Message::MsgLeiosBlockAnnouncement { .. }));
        assert!(matches!(received[1], Message::MsgLeiosBlockOffer { .. }));
        assert!(matches!(received[2], Message::MsgLeiosBlockTxsOffer { .. }));

        cli_mux.abort();
        srv_mux.abort();
    }

    /// Client sends MsgDone immediately: server exits with notifications_sent = 0.
    #[tokio::test]
    async fn done_from_client_terminates_session() {
        let (cli_send, cli_recv, srv_send, srv_recv, cli_mux, srv_mux) = make_pair();
        let (trc, _tmp) = tracer().await;

        let (srv_result, ()) = tokio::join!(
            execute_leios_notify_script(srv_send, srv_recv, vec![], &trc),
            async {
                let mut runner = Runner::<LeiosNotify>::new(Role::Client, cli_send, cli_recv);
                runner.send(&Message::MsgDone).await.unwrap();
            }
        );

        assert_eq!(srv_result.unwrap().notifications_sent, 0);

        cli_mux.abort();
        srv_mux.abort();
    }

    /// Server sends block_offer with the correct point and size.
    #[tokio::test]
    async fn block_offer_fields_are_correct() {
        let (cli_send, cli_recv, srv_send, srv_recv, cli_mux, srv_mux) = make_pair();
        let (trc, _tmp) = tracer().await;

        let actions = vec![action("block_offer")];

        let (srv_result, cli_msg) = tokio::join!(
            execute_leios_notify_script(srv_send, srv_recv, actions, &trc),
            async {
                let mut runner = Runner::<LeiosNotify>::new(Role::Client, cli_send, cli_recv);
                runner.send(&Message::MsgLeiosNotificationRequestNext).await.unwrap();
                runner.recv().await.unwrap()
            }
        );

        srv_result.unwrap();
        match cli_msg {
            Message::MsgLeiosBlockOffer { point: _, eb_size } => {
                assert_eq!(eb_size, 512);
            }
            other => panic!("expected MsgLeiosBlockOffer, got {other:?}"),
        }

        cli_mux.abort();
        srv_mux.abort();
    }

    /// After actions are exhausted, the server closes cleanly and the client
    /// receives a channel-closed error on the next recv.
    #[tokio::test]
    async fn session_ends_when_actions_exhausted() {
        let (cli_send, cli_recv, srv_send, srv_recv, cli_mux, srv_mux) = make_pair();
        let (trc, _tmp) = tracer().await;

        let actions = vec![action("block_announcement")]; // only one action

        let (srv_result, (first_ok, second_err)) = tokio::join!(
            execute_leios_notify_script(srv_send, srv_recv, actions, &trc),
            async {
                let mut runner = Runner::<LeiosNotify>::new(Role::Client, cli_send, cli_recv);
                // First request: should succeed
                runner.send(&Message::MsgLeiosNotificationRequestNext).await.unwrap();
                let first = runner.recv().await;
                // Second request: server has exited, channel will close
                let _ = runner.send(&Message::MsgLeiosNotificationRequestNext).await;
                let second = runner.recv().await;
                (first.is_ok(), second.is_err())
            }
        );

        srv_result.unwrap();
        assert!(first_ok,  "first request should succeed");
        assert!(second_err, "second request should fail after server exits");

        cli_mux.abort();
        srv_mux.abort();
    }
}

fn build_votes(specs: &[VoteSpec]) -> anyhow::Result<Vec<Vote>> {
    specs
        .iter()
        .map(|s| {
            let eb_hash_bytes = decode_hex(&s.eb_hash)
                .map_err(|e| anyhow::anyhow!("vote eb_hash: {e}"))?;
            if eb_hash_bytes.len() != 32 {
                anyhow::bail!("vote eb_hash must be 32 bytes, got {}", eb_hash_bytes.len());
            }
            let mut eb_hash = [0u8; 32];
            eb_hash.copy_from_slice(&eb_hash_bytes);
            let vote_signature = decode_hex(&s.signature)
                .map_err(|e| anyhow::anyhow!("vote signature: {e}"))?;
            Ok(Vote { slot: s.slot, eb_hash, voter_id: s.voter_id, vote_signature })
        })
        .collect()
}
