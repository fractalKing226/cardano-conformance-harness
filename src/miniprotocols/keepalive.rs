use std::time::Duration;

use net_core::mux::{CodecRecv, CodecSend};
use net_core::protocols::keepalive::{self, KeepAlive, Message};
use net_core::protocols::{Role, Runner};
use serde_json::json;

use crate::trace::{Direction, EventKind, TraceEvent, Tracer};

pub const KEEP_ALIVE_PROTOCOL: u16 = net_core::protocols::keepalive::PROTOCOL_ID;

const MINI_PROTOCOL: &str = "keep-alive";
pub const KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(20);

/// Background task that sends periodic `MsgKeepAlive` pings.
///
/// Runs until the mux shuts down (channels closed) or a send/recv error occurs.
pub async fn run_keepalive(
    codec_send: CodecSend,
    codec_recv: CodecRecv,
    tracer: Tracer,
    interval: Duration,
) {
    let mut runner = Runner::<KeepAlive>::new(Role::Client, codec_send, codec_recv);
    let mut cookie: u16 = 0;

    loop {
        tokio::time::sleep(interval).await;

        let result = keepalive::keep_alive(&mut runner, cookie).await;

        match result {
            Ok(_rtt) => {
                let _ = tracer
                    .emit(
                        TraceEvent::new(
                            EventKind::KeepAliveSent,
                            Direction::Sent,
                            json!({ "cookie": cookie }),
                        )
                        .with_protocol(MINI_PROTOCOL),
                    )
                    .await;
                let _ = tracer
                    .emit(
                        TraceEvent::new(
                            EventKind::KeepAliveReceived,
                            Direction::Received,
                            json!({ "cookie": cookie }),
                        )
                        .with_protocol(MINI_PROTOCOL),
                    )
                    .await;
                cookie = cookie.wrapping_add(1);
            }
            Err(e) => {
                tracing::debug!("keep-alive failed (connection likely closed): {e}");
                break;
            }
        }
    }
}

/// Server-side keep-alive: respond to MsgKeepAlive pings from the peer.
pub async fn run_keepalive_server(codec_send: CodecSend, codec_recv: CodecRecv) {
    let mut runner = Runner::<KeepAlive>::new(Role::Server, codec_send, codec_recv);
    loop {
        match runner.recv().await {
            Ok(Message::MsgKeepAlive { cookie }) => {
                if let Err(e) = runner.send(&Message::MsgKeepAliveResponse { cookie }).await {
                    tracing::debug!("keepalive server send failed (connection closed): {e}");
                    break;
                }
            }
            Ok(Message::MsgDone) => break,
            Ok(other) => {
                tracing::warn!("keepalive server: unexpected message {other:?}");
            }
            Err(e) => {
                tracing::debug!("keepalive server recv failed (connection closed): {e}");
                break;
            }
        }
    }
}
