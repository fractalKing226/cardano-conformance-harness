/// Peer-Sharing mini-protocol (N2N, version-conditional).
///
/// Peer-Sharing is available from N2N version 13 onwards. It allows peers to
/// exchange lists of known relay addresses to aid peer discovery. The node
/// sends `MsgShareRequest(amount)` and we would reply with `MsgSharePeers(addrs)`.
///
/// **Status: not yet implementable.** `pallas-network` 0.36.0 does not expose
/// a `PROTOCOL_N2N_PEER_SHARING` constant or message codec for this protocol.
/// When Pallas adds support the implementation here follows the same pattern as
/// `keepalive.rs` and `txsubmission.rs`: a background `run_peer_sharing` task
/// subscribed in `runner.rs` that logs every message and responds with an
/// empty peer list.
///
/// When ready, wire up in `runner.rs`:
/// ```text
/// // In connect step, after plexer.spawn():
/// if negotiated_version >= PEER_SHARING_MIN_VERSION {
///     let ps_channel = plexer.subscribe_client(PEER_SHARING_PROTOCOL);
///     state.ps_handle = Some(tokio::spawn(run_peer_sharing(ps_channel, tracer.clone())));
/// }
/// ```

/// Minimum N2N version that carries Peer-Sharing.
pub const PEER_SHARING_MIN_VERSION: u64 = 13;

// Once pallas-network exposes PROTOCOL_N2N_PEER_SHARING:
// pub use pallas_network::miniprotocols::PROTOCOL_N2N_PEER_SHARING as PEER_SHARING_PROTOCOL;
// pub use pallas_network::miniprotocols::peersharing;
//
// pub async fn run_peer_sharing(channel: AgentChannel, tracer: Tracer) {
//     // subscribe, log MsgShareRequest, reply with MsgSharePeers([])
// }
