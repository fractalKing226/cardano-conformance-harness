use pallas_network::miniprotocols::txsubmission;
use pallas_network::multiplexer::AgentChannel;
use serde_json::json;

use crate::trace::{Direction, EventKind, TraceEvent, Tracer};

pub use pallas_network::miniprotocols::PROTOCOL_N2N_TX_SUBMISSION as TX_SUBMISSION_PROTOCOL;

const MINI_PROTOCOL: &str = "tx-submission";

/// Passive Tx-Submission producer.
///
/// In N2N, the TCP initiator (us) is the **producer**: we advertise available
/// transactions and the node (consumer) pulls them via `MsgRequestTxIds` /
/// `MsgRequestTxs`. Since the harness has no transactions to offer, we always
/// reply with empty lists. Every inbound request and outbound reply is logged
/// to the trace so the verifier can observe the node's pull behaviour.
///
/// The task runs until the channel closes (plexer aborted on disconnect).
///
/// TODO (scenario-driven tx-submission): the empty-list replies below are the
/// correct default but only one possible behavior. When scenarios need to test
/// transaction propagation, replace the hardcoded `vec![]` replies with content
/// drawn from a scenario-supplied "imaginary mempool" — a `Vec<EraTxBody>`
/// passed into this function (or behind a shared handle) that the scenario step
/// populates before the connection is opened. The default (no argument supplied
/// → empty mempool) must remain the fallback so existing scenarios are unaffected.
pub async fn run_tx_submission(channel: AgentChannel, tracer: Tracer) {
    let mut client = txsubmission::Client::new(channel);

    // Send MsgInit to declare ourselves as a producer and move to Idle state.
    if let Err(e) = client.send_init().await {
        tracing::debug!("tx-submission send_init failed: {e}");
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
        let request = match client.next_request().await {
            Ok(r) => r,
            Err(e) => {
                tracing::debug!("tx-submission next_request failed (connection closed): {e}");
                break;
            }
        };

        match request {
            txsubmission::Request::TxIds(ack, req) => {
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

                if let Err(e) = client.reply_tx_ids(vec![]).await {
                    tracing::debug!("tx-submission reply_tx_ids failed: {e}");
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
            txsubmission::Request::TxIdsNonBlocking(ack, req) => {
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

                if let Err(e) = client.reply_tx_ids(vec![]).await {
                    tracing::debug!("tx-submission reply_tx_ids failed: {e}");
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
            txsubmission::Request::Txs(ids) => {
                let _ = tracer
                    .emit(
                        TraceEvent::new(
                            EventKind::TxSubmissionMessage,
                            Direction::Received,
                            json!({ "msg_kind": "request_txs", "count": ids.len() }),
                        )
                        .with_protocol(MINI_PROTOCOL),
                    )
                    .await;

                if let Err(e) = client.reply_txs(vec![]).await {
                    tracing::debug!("tx-submission reply_txs failed: {e}");
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
        }
    }
}
