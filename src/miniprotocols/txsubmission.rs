use net_core::mux::{CodecRecv, CodecSend};
use net_core::protocols::txsubmission::{Message, TxSubmission};
use net_core::protocols::{Role, Runner};
use serde_json::json;

use crate::trace::{Direction, EventKind, TraceEvent, Tracer};

pub const TX_SUBMISSION_PROTOCOL: u16 = net_core::protocols::txsubmission::PROTOCOL_ID;

const MINI_PROTOCOL: &str = "tx-submission";

/// Passive Tx-Submission producer.
///
/// In N2N, the TCP initiator (us) is the **producer**: we advertise available
/// transactions and the node (consumer) pulls them via `MsgRequestTxIds` /
/// `MsgRequestTxs`. Since the harness has no transactions to offer, we always
/// reply with empty lists.
///
/// The task runs until the channel closes (mux aborted on disconnect).
pub async fn run_tx_submission(codec_send: CodecSend, codec_recv: CodecRecv, tracer: Tracer) {
    let mut runner = Runner::<TxSubmission>::new(Role::Client, codec_send, codec_recv);

    // Send MsgInit to declare ourselves as a producer and move to Idle state.
    if let Err(e) = runner.send(&Message::MsgInit).await {
        tracing::debug!("tx-submission MsgInit failed: {e}");
        return;
    }
    let _ = tracer
        .emit(
            TraceEvent::new(
                EventKind::TxSubmissionMessage,
                Direction::Sent,
                json!({ "msg_kind": "init" }),
            )
            .with_protocol(MINI_PROTOCOL),
        )
        .await;

    loop {
        let msg = match runner.recv().await {
            Ok(m) => m,
            Err(e) => {
                tracing::debug!("tx-submission recv failed (connection closed): {e}");
                break;
            }
        };

        match msg {
            Message::MsgRequestTxIdsBlocking { ack, req } => {
                let _ = tracer
                    .emit(
                        TraceEvent::new(
                            EventKind::TxSubmissionMessage,
                            Direction::Received,
                            json!({ "msg_kind": "request_tx_ids", "blocking": true,
                                    "ack": ack, "req": req }),
                        )
                        .with_protocol(MINI_PROTOCOL),
                    )
                    .await;

                if let Err(e) = runner.send(&Message::MsgReplyTxIds { tx_ids: vec![] }).await {
                    tracing::debug!("tx-submission MsgReplyTxIds failed: {e}");
                    break;
                }
                let _ = tracer
                    .emit(
                        TraceEvent::new(
                            EventKind::TxSubmissionMessage,
                            Direction::Sent,
                            json!({ "msg_kind": "reply_tx_ids", "count": 0 }),
                        )
                        .with_protocol(MINI_PROTOCOL),
                    )
                    .await;
            }
            Message::MsgRequestTxIdsNonBlocking { ack, req } => {
                let _ = tracer
                    .emit(
                        TraceEvent::new(
                            EventKind::TxSubmissionMessage,
                            Direction::Received,
                            json!({ "msg_kind": "request_tx_ids", "blocking": false,
                                    "ack": ack, "req": req }),
                        )
                        .with_protocol(MINI_PROTOCOL),
                    )
                    .await;

                if let Err(e) = runner.send(&Message::MsgReplyTxIds { tx_ids: vec![] }).await {
                    tracing::debug!("tx-submission MsgReplyTxIds failed: {e}");
                    break;
                }
                let _ = tracer
                    .emit(
                        TraceEvent::new(
                            EventKind::TxSubmissionMessage,
                            Direction::Sent,
                            json!({ "msg_kind": "reply_tx_ids", "count": 0 }),
                        )
                        .with_protocol(MINI_PROTOCOL),
                    )
                    .await;
            }
            Message::MsgRequestTxs { tx_ids } => {
                let _ = tracer
                    .emit(
                        TraceEvent::new(
                            EventKind::TxSubmissionMessage,
                            Direction::Received,
                            json!({ "msg_kind": "request_txs", "count": tx_ids.len() }),
                        )
                        .with_protocol(MINI_PROTOCOL),
                    )
                    .await;

                if let Err(e) = runner.send(&Message::MsgReplyTxs { txs: vec![] }).await {
                    tracing::debug!("tx-submission MsgReplyTxs failed: {e}");
                    break;
                }
                let _ = tracer
                    .emit(
                        TraceEvent::new(
                            EventKind::TxSubmissionMessage,
                            Direction::Sent,
                            json!({ "msg_kind": "reply_txs", "count": 0 }),
                        )
                        .with_protocol(MINI_PROTOCOL),
                    )
                    .await;
            }
            // MsgDone from the remote — consumer is done pulling
            Message::MsgDone => {
                tracing::debug!("tx-submission: remote sent MsgDone");
                break;
            }
            other => {
                tracing::warn!("tx-submission: unexpected message {other:?}");
                break;
            }
        }
    }
}
