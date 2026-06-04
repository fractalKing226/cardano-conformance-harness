use std::time::Duration;

use pallas_network::miniprotocols::keepalive::{self, Server as KaServer};
use serde_json::json;

use crate::trace::{Direction, EventKind, TraceEvent, Tracer};

pub use pallas_network::miniprotocols::PROTOCOL_N2N_KEEP_ALIVE as KEEP_ALIVE_PROTOCOL;

const MINI_PROTOCOL: &str = "keep-alive";
pub const KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(20);

/// Background task that sends periodic `MsgKeepAlive` pings and logs both
/// sides of every exchange to the JSON trace file.
///
/// As the TCP initiator we have keep-alive client agency: we send pings and
/// the node (TCP responder) echoes them back. Without this loop the node will
/// eventually drop the connection.
///
/// `interval` controls how often pings are sent. Production code uses
/// `KEEP_ALIVE_INTERVAL`; tests may pass a shorter value.
///
/// The function runs until the channel is closed or a send/recv error occurs,
/// which normally happens when the plexer is aborted on `disconnect`.
pub async fn run_keepalive(mut client: keepalive::Client, tracer: Tracer, interval: Duration) {
    loop {
        tokio::time::sleep(interval).await;

        if let Err(e) = client.send_keepalive_request().await {
            tracing::debug!("keep-alive send failed (connection likely closed): {e}");
            break;
        }

        let cookie = match client.state() {
            keepalive::State::Server(c) => *c,
            _ => 0,
        };

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

        if let Err(e) = client.recv_keepalive_response().await {
            tracing::debug!("keep-alive recv failed (connection likely closed): {e}");
            break;
        }

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
    }
}

/// Server-side keep-alive: respond to MsgKeepAlive pings from the peer.
/// As the TCP acceptor (responder), we echo cookies back.
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
