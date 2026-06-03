use anyhow::Context;
use pallas_network::miniprotocols::handshake::n2n::{VersionData, VersionTable};
use pallas_network::miniprotocols::handshake::{Client, Confirmation};
use pallas_network::miniprotocols::PROTOCOL_N2N_HANDSHAKE;
use pallas_network::multiplexer::{AgentChannel, Bearer, Plexer};
use serde_json::json;
use tracing::{debug, info, warn};

use crate::trace::{Direction, EventKind, TraceEvent, Tracer};

/// Runs the NodeToNode Handshake on a pre-allocated multiplexer channel.
/// The caller is responsible for the Bearer, Plexer lifecycle, and the
/// ConnectionOpened / ConnectionClosed trace events.
pub async fn handshake_on_channel(
    channel: AgentChannel,
    magic: u64,
    tracer: &mut Tracer,
) -> anyhow::Result<u64> {
    tracer
        .emit(TraceEvent::new(
            EventKind::HandshakeStarted,
            Direction::Internal,
            json!({ "magic": magic }),
        ))
        .await?;

    let mut client: Client<VersionData> = Client::new(channel);

    let version_table = VersionTable::v7_and_above(magic);
    let mut proposed_versions: Vec<u64> = version_table.values.keys().cloned().collect();
    proposed_versions.sort();

    tracer
        .emit(TraceEvent::new(
            EventKind::HandshakeVersionProposed,
            Direction::Sent,
            json!({ "versions": proposed_versions, "magic": magic }),
        ))
        .await?;

    debug!(?proposed_versions, "Sending ProposeVersions");

    if let Err(e) = client.send_propose(version_table).await {
        let msg = e.to_string();
        let _ = tracer
            .emit(TraceEvent::new(
                EventKind::Error,
                Direction::Internal,
                json!({ "phase": "send_propose", "error": msg }),
            ))
            .await;
        return Err(anyhow::anyhow!("send_propose failed: {e}"));
    }

    debug!("Awaiting handshake confirmation");

    let confirmation = match client.recv_while_confirm().await {
        Ok(c) => c,
        Err(e) => {
            let msg = e.to_string();
            let _ = tracer
                .emit(TraceEvent::new(
                    EventKind::Error,
                    Direction::Internal,
                    json!({ "phase": "recv_while_confirm", "error": msg }),
                ))
                .await;
            return Err(anyhow::anyhow!("recv_while_confirm failed: {e}"));
        }
    };

    let negotiated = match confirmation {
        Confirmation::Accepted(version, ref data) => {
            info!(version, "Handshake accepted");
            tracer
                .emit(TraceEvent::new(
                    EventKind::HandshakeVersionAccepted,
                    Direction::Received,
                    json!({
                        "version": version,
                        "peer_data": format!("{data:?}"),
                    }),
                ))
                .await?;
            version
        }
        Confirmation::Rejected(ref reason) => {
            warn!(?reason, "Handshake rejected by peer");
            tracer
                .emit(TraceEvent::new(
                    EventKind::HandshakeVersionRejected,
                    Direction::Received,
                    json!({ "reason": format!("{reason:?}") }),
                ))
                .await?;
            return Err(anyhow::anyhow!("handshake rejected: {reason:?}"));
        }
        Confirmation::QueryReply(_) => {
            return Err(anyhow::anyhow!(
                "unexpected QueryReply during initiator handshake"
            ));
        }
    };

    tracer
        .emit(TraceEvent::new(
            EventKind::HandshakeCompleted,
            Direction::Internal,
            json!({ "negotiated_version": negotiated }),
        ))
        .await?;

    info!(negotiated, "Handshake complete");
    Ok(negotiated)
}

/// Standalone handshake probe: opens its own TCP connection, performs the
/// handshake, and closes cleanly. Used by unit tests and the handshake-only
/// code path.
pub async fn run_handshake(addr: &str, magic: u64, tracer: &mut Tracer) -> anyhow::Result<u64> {
    info!(%addr, "Opening TCP connection");

    let bearer = Bearer::connect_tcp(addr)
        .await
        .with_context(|| format!("failed to connect to {addr}"))?;

    tracer
        .emit(TraceEvent::new(
            EventKind::ConnectionOpened,
            Direction::Internal,
            json!({ "addr": addr }),
        ))
        .await?;

    let mut plexer = Plexer::new(bearer);
    let hs_channel = plexer.subscribe_client(PROTOCOL_N2N_HANDSHAKE);
    let plexer_handle = plexer.spawn();

    let version = match handshake_on_channel(hs_channel, magic, tracer).await {
        Ok(v) => v,
        Err(e) => {
            plexer_handle.abort().await;
            tracer
                .emit(TraceEvent::new(
                    EventKind::ConnectionClosed,
                    Direction::Internal,
                    json!({ "reason": "error" }),
                ))
                .await?;
            return Err(e);
        }
    };

    plexer_handle.abort().await;

    tracer
        .emit(TraceEvent::new(
            EventKind::ConnectionClosed,
            Direction::Internal,
            json!({}),
        ))
        .await?;

    Ok(version)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn fails_gracefully_on_refused_connection() {
        let tmp = NamedTempFile::new().unwrap();
        let mut tracer = crate::trace::Tracer::open(tmp.path()).await.unwrap();

        // Port 1 is reserved and will be refused on any standard OS.
        let result = run_handshake("127.0.0.1:1", 1, &mut tracer).await;

        assert!(result.is_err());

        for line in std::fs::read_to_string(tmp.path()).unwrap().lines() {
            serde_json::from_str::<serde_json::Value>(line)
                .expect("every trace line must be valid JSON");
        }
    }
}
