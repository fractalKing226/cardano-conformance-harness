use std::net::ToSocketAddrs;

use anyhow::Context;
use net_core::bearer::tcp::TcpBearer;
use net_core::mux::scheduler::{AnyScheduler, TrafficClass};
use net_core::mux::{CodecRecv, CodecSend, Mux, MuxConfig, ProtocolConfig, MODE_INITIATOR};
use net_core::protocols::handshake::{self, n2n, HandshakeResult};
use serde_json::json;
use tracing::{debug, info, warn};

use crate::trace::{Direction, EventKind, TraceEvent, Tracer};

/// Runs the NodeToNode Handshake on a pre-allocated codec pair.
/// Returns the negotiated version number.
pub async fn handshake_on_channels(
    codec_send: CodecSend,
    codec_recv: CodecRecv,
    magic: u64,
    tracer: &Tracer,
) -> anyhow::Result<u64> {
    tracer
        .emit(TraceEvent::new(
            EventKind::HandshakeStarted,
            Direction::Internal,
            json!({ "magic": magic }),
        ))
        .await?;

    let versions = n2n::version_table(&n2n::VersionData {
        network_magic: magic,
        initiator_only_diffusion_mode: true,
        peer_sharing: 1,
        query: false,
    });

    let mut proposed_versions: Vec<u64> = versions.keys().cloned().collect();
    proposed_versions.sort();

    tracer
        .emit(TraceEvent::new(
            EventKind::HandshakeVersionProposed,
            Direction::Sent,
            json!({ "versions": proposed_versions, "magic": magic }),
        ))
        .await?;

    debug!(?proposed_versions, "Sending ProposeVersions");

    let result = handshake::run_client(codec_send, codec_recv, versions)
        .await
        .map_err(|e| anyhow::anyhow!("handshake failed: {e}"))?;

    let negotiated = match result {
        HandshakeResult::Accepted { version_number, .. } => {
            info!(version_number, "Handshake accepted");
            tracer
                .emit(TraceEvent::new(
                    EventKind::HandshakeVersionAccepted,
                    Direction::Received,
                    json!({ "version": version_number }),
                ))
                .await?;
            version_number
        }
        HandshakeResult::Refused(reason) => {
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
        HandshakeResult::QueryReply(_) => {
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
/// handshake, and closes cleanly. Used by unit tests and the handshake-only code path.
pub async fn run_handshake(addr: &str, magic: u64, tracer: &Tracer) -> anyhow::Result<u64> {
    info!(%addr, "Opening TCP connection");

    let socket_addr = addr
        .to_socket_addrs()
        .with_context(|| format!("failed to resolve {addr}"))?
        .next()
        .ok_or_else(|| anyhow::anyhow!("no address resolved for {addr}"))?;

    let bearer = TcpBearer::connect(socket_addr)
        .await
        .with_context(|| format!("failed to connect to {addr}"))?;

    tracer
        .emit(TraceEvent::new(
            EventKind::ConnectionOpened,
            Direction::Internal,
            json!({ "addr": addr }),
        ))
        .await?;

    let hs_proto = ProtocolConfig {
        id: handshake::PROTOCOL_ID,
        traffic_class: TrafficClass::Priority,
        ingress_limit: handshake::SIZE_LIMIT,
        egress_queue_size: 4,
    };

    let mut mux = Mux::new(MuxConfig::default(), AnyScheduler::default(), MODE_INITIATOR);
    let (hs_send, hs_recv) = mux.register(&hs_proto);
    let _running = mux.run(bearer);

    let version = match handshake_on_channels(
        CodecSend::new(hs_send),
        CodecRecv::new(hs_recv),
        magic,
        tracer,
    )
    .await
    {
        Ok(v) => v,
        Err(e) => {
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
        let tracer = crate::trace::Tracer::open(tmp.path()).await.unwrap();

        // Port 1 is reserved and will be refused on any standard OS.
        let result = run_handshake("127.0.0.1:1", 1, &tracer).await;

        assert!(result.is_err());

        for line in std::fs::read_to_string(tmp.path()).unwrap().lines() {
            serde_json::from_str::<serde_json::Value>(line)
                .expect("every trace line must be valid JSON");
        }
    }
}
