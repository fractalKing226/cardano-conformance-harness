use std::time::Duration;

use net_core::mux::{CodecRecv, CodecSend};
use net_core::protocols::keepalive::{self, KeepAlive};
use net_core::protocols::{Role, Runner};
use pallas_network::miniprotocols::keepalive::Server as KaServer;
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
/// As the TCP acceptor (responder), we echo cookies back.
/// Uses pallas since the server-side connection is still pallas-based.
pub async fn run_keepalive_server(mut server: KaServer) {
    loop {
        if let Err(e) = server.recv_keepalive_request().await {
            tracing::debug!("keepalive server recv failed (connection closed): {e}");
            break;
        }
        if let Err(e) = server.send_keepalive_response().await {
            tracing::debug!("keepalive server send failed (connection closed): {e}");
            break;
        }
    }
}
