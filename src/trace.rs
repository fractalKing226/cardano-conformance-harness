use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::Path;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt as _;
use tokio::sync::Mutex;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    // Connection lifecycle
    ConnectionOpened,
    ConnectionClosed,
    Error,

    // Handshake mini-protocol
    HandshakeStarted,
    HandshakeVersionProposed,
    HandshakeVersionAccepted,
    HandshakeVersionRejected,
    HandshakeCompleted,

    // Chain-Sync mini-protocol
    ChainSyncStarted,
    ChainSyncFindIntersect,
    ChainSyncIntersectFound,
    ChainSyncIntersectNotFound,
    ChainSyncRequestNext,
    ChainSyncRollForward,
    ChainSyncRollBackward,
    ChainSyncAwaitReply,
    ChainSyncDone,
    ChainSyncSessionSummary,

    // Block-Fetch mini-protocol
    BlockFetchStarted,
    BlockFetchRequestRange,
    BlockFetchNoBlocks,
    BlockFetchStartBatch,
    BlockFetchBlock,
    BlockFetchBatchDone,
    BlockFetchClientDone,
    BlockFetchSessionSummary,

    // Scenario runner
    ProtocolWorkersStarted,
    ScenarioStarted,
    QueryTipStarted,
    QueryTipCompleted,
    RepeatIterationStarted,
    RepeatIterationCompleted,
    VariableSet,
    VariableReferenced,
    ScenarioCompleted,
    StepStarted,
    StepCompleted,
    AssertionPassed,
    AssertionFailed,

    // Tx-Submission mini-protocol (passive — logged when peer sends messages)
    TxSubmissionMessage,

    // Keep-Alive mini-protocol
    KeepAliveSent,
    KeepAliveReceived,

    // Peer-Sharing mini-protocol (not yet supported in pallas-network 0.36.0)
    PeerSharingMessage,

    // Scripted response execution
    ResponseRuleApplied,

    // Server-side Block-Fetch session lifecycle
    ServerBlockFetchStarted,
    ServerBlockFetchCompleted,

    // Parallel step execution
    ParallelStarted,
    ParallelBranchStarted,
    ParallelBranchCompleted,
    ParallelBranchAborted,
    ParallelBranchFailed,
    ParallelCompleted,

    // Server-side lifecycle
    ServerBearerAccepted,
    ServerListenStarted,
    ServerListenStopped,
    ServerConnectionAccepted,
    ServerHandshakeAccepted,
    ServerChainSyncStarted,
    ServerChainSyncCompleted,

    // Network topology declaration and time evolution
    NetworkDeclared,
    SlotAdvanced,

    // Peer state lifecycle
    PeerStateInitialized,
    PeerChainExtended,

    // Peer-identity / imaginary-network events (emitted by emit_peer_event step)
    PeerProducedBlock,
    PeerCastVote,
    PeerForkedChain,
    PeerJoinedNetwork,
    PeerLeftNetwork,
    /// Catch-all for unknown event_kind strings passed to emit_peer_event.
    PeerNetworkEvent,
}

/// Direction of a trace event relative to the harness.
///
/// - `Sent` / `Received` correspond to actual wire messages.
/// - `Internal` is for harness-generated meta-events (session boundaries,
///   summaries, errors) that do not map to a single wire message.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    Sent,
    Received,
    Internal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceEvent {
    pub timestamp: String,
    pub kind: EventKind,
    pub direction: Direction,
    /// Name of the connection this event belongs to (e.g. `"default"`, `"peer_a"`).
    /// Present on all wire events and connection-lifecycle events; absent on
    /// scenario-level meta-events (ScenarioStarted, VariableSet, etc.).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connection: Option<String>,
    /// Peer identity label for trace attribution. Set when the connection (or
    /// emit_peer_event step) carries a peer_id. Independent of `connection` —
    /// the connection name is an internal handle; the peer_id is for downstream
    /// verifiers of the imaginary-network model.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub peer_id: Option<String>,
    /// Imaginary-network slot at the moment this event was emitted.
    /// Present whenever the scenario has a network declaration; absent otherwise.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub slot: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mini_protocol: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state_before: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state_after: Option<String>,
    pub payload: Value,
}

impl TraceEvent {
    pub fn new(kind: EventKind, direction: Direction, payload: Value) -> Self {
        Self {
            timestamp: Utc::now().to_rfc3339(),
            kind,
            direction,
            connection: None,
            peer_id: None,
            slot: None,
            mini_protocol: None,
            state_before: None,
            state_after: None,
            payload,
        }
    }

    /// Tags a wire-message event with its mini-protocol name and the state
    /// transition it caused. Use for `Sent` and `Received` events.
    pub fn with_states(
        mut self,
        mini_protocol: &'static str,
        state_before: impl Into<String>,
        state_after: impl Into<String>,
    ) -> Self {
        self.mini_protocol = Some(mini_protocol);
        self.state_before = Some(state_before.into());
        self.state_after = Some(state_after.into());
        self
    }

    /// Tags a meta/internal event with its mini-protocol name only.
    /// Use for `Internal` events (session starts, summaries, errors) where
    /// state-before/after fields are not meaningful.
    pub fn with_protocol(mut self, mini_protocol: &'static str) -> Self {
        self.mini_protocol = Some(mini_protocol);
        self
    }

    /// Tags a wire or connection-lifecycle event with the name of the connection
    /// it belongs to. Use for all events that are attributed to a specific
    /// named connection (both `"default"` and explicitly-named connections).
    pub fn with_connection(mut self, connection: impl Into<String>) -> Self {
        self.connection = Some(connection.into());
        self
    }

    /// Tags an event with a peer identity label for downstream trace verifiers.
    pub fn with_peer_id(mut self, peer_id: impl Into<String>) -> Self {
        self.peer_id = Some(peer_id.into());
        self
    }

    /// Explicitly sets the slot on an event (e.g. for SlotAdvanced where the
    /// to_slot is already known and should not be overwritten by the tracer).
    pub fn with_slot(mut self, slot: u64) -> Self {
        self.slot = Some(slot);
        self
    }
}

// ── Tracer ────────────────────────────────────────────────────────────────────

struct TracerInner {
    file: tokio::fs::File,
    /// In-memory buffer of events emitted since the last `drain_buffer` call.
    /// Used by the scenario runner to collect per-step events for assertions.
    event_buffer: Vec<Value>,
}

/// A cheaply cloneable, concurrency-safe trace file handle.
///
/// Multiple tasks may hold a `Tracer` clone and emit events concurrently.
/// The async mutex ensures each event is a single coherent JSON-lines write.
///
/// Each clone carries independent `peer_id` and `current_slot` context.
/// When `emit` is called, both are automatically stamped onto outgoing events
/// that don't already carry explicit values.
#[derive(Clone)]
pub struct Tracer {
    inner:        Arc<Mutex<TracerInner>>,
    /// Per-clone peer identity (set via `for_peer_opt`).
    peer_id:      Option<String>,
    /// Shared slot counter from RunnerState. Clones share the same Arc so
    /// slot changes in one branch are immediately visible to all tracers.
    current_slot: Option<Arc<AtomicU64>>,
}

impl Tracer {
    pub async fn open(path: &Path) -> anyhow::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)
            .await?;
        Ok(Self {
            inner: Arc::new(Mutex::new(TracerInner {
                file,
                event_buffer: Vec::new(),
            })),
            peer_id:      None,
            current_slot: None,
        })
    }

    /// Returns a clone tagged with `peer_id`, propagating the slot tracker.
    /// Passing `None` is a no-op for the peer_id field.
    pub fn for_peer_opt(&self, peer_id: Option<String>) -> Self {
        Self {
            inner:        Arc::clone(&self.inner),
            peer_id:      peer_id.or_else(|| self.peer_id.clone()),
            current_slot: self.current_slot.clone(),
        }
    }

    /// Returns a clone wired to the scenario's slot counter.
    /// All events emitted through the clone are automatically stamped with the
    /// current slot at emission time.
    pub fn with_slot_tracker(&self, current_slot: Option<Arc<AtomicU64>>) -> Self {
        Self {
            inner:        Arc::clone(&self.inner),
            peer_id:      self.peer_id.clone(),
            current_slot: current_slot.or_else(|| self.current_slot.clone()),
        }
    }

    pub async fn emit(&self, mut event: TraceEvent) -> anyhow::Result<()> {
        if event.peer_id.is_none() {
            event.peer_id = self.peer_id.clone();
        }
        // Stamp the current slot on events that don't carry an explicit value.
        // Relaxed ordering: slot is a logical counter for trace attribution, not a
        // memory-ordering barrier. A reader seeing the previous slot on an event
        // emitted nanoseconds before a slot advance is fine — the SlotAdvanced
        // event provides the authoritative record of when the change occurred.
        if event.slot.is_none() {
            event.slot = self.current_slot.as_ref().map(|a| a.load(Ordering::Relaxed));
        }
        // Serialise outside the lock to minimise hold time.
        let v = serde_json::to_value(&event)?;
        let mut line = serde_json::to_string(&v)?;
        line.push('\n');
        let mut inner = self.inner.lock().await;
        inner.event_buffer.push(v);
        inner.file.write_all(line.as_bytes()).await?;
        inner.file.flush().await?;
        Ok(())
    }

    /// Returns all events buffered since the last call, clearing the buffer.
    pub async fn drain_buffer(&self) -> Vec<Value> {
        std::mem::take(&mut self.inner.lock().await.event_buffer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn event_serialises_with_snake_case_fields() {
        let event = TraceEvent::new(
            EventKind::ConnectionOpened,
            Direction::Internal,
            json!({ "addr": "localhost:3001" }),
        );

        let parsed: serde_json::Value =
            serde_json::from_str(&serde_json::to_string(&event).unwrap()).unwrap();

        assert_eq!(parsed["kind"], "connection_opened");
        assert_eq!(parsed["direction"], "internal");
        assert!(parsed["timestamp"].is_string());
        assert_eq!(parsed["payload"]["addr"], "localhost:3001");
    }

    #[tokio::test]
    async fn protocol_fields_are_omitted_when_none() {
        let event = TraceEvent::new(
            EventKind::HandshakeStarted,
            Direction::Internal,
            json!({}),
        );
        let json = serde_json::to_string(&event).unwrap();
        assert!(!json.contains("mini_protocol"));
        assert!(!json.contains("state_before"));
        assert!(!json.contains("state_after"));
    }

    #[tokio::test]
    async fn with_states_populates_optional_fields() {
        let event = TraceEvent::new(
            EventKind::ChainSyncRequestNext,
            Direction::Sent,
            json!({}),
        )
        .with_states("chain-sync", "Idle", "CanAwait");

        let parsed: serde_json::Value =
            serde_json::from_str(&serde_json::to_string(&event).unwrap()).unwrap();

        assert_eq!(parsed["mini_protocol"], "chain-sync");
        assert_eq!(parsed["state_before"], "Idle");
        assert_eq!(parsed["state_after"], "CanAwait");
    }

    #[tokio::test]
    async fn with_protocol_sets_only_mini_protocol() {
        let event = TraceEvent::new(
            EventKind::ChainSyncStarted,
            Direction::Internal,
            json!({}),
        )
        .with_protocol("chain-sync");

        let parsed: serde_json::Value =
            serde_json::from_str(&serde_json::to_string(&event).unwrap()).unwrap();

        assert_eq!(parsed["mini_protocol"], "chain-sync");
        assert!(parsed.get("state_before").is_none() || parsed["state_before"].is_null());
    }

    #[tokio::test]
    async fn emit_writes_one_json_line_per_call() {
        let tmp = NamedTempFile::new().unwrap();
        let tracer = Tracer::open(tmp.path()).await.unwrap();

        tracer
            .emit(TraceEvent::new(
                EventKind::HandshakeStarted,
                Direction::Internal,
                json!({ "magic": 1_u64 }),
            ))
            .await
            .unwrap();
        tracer
            .emit(TraceEvent::new(
                EventKind::HandshakeCompleted,
                Direction::Internal,
                json!({ "negotiated_version": 13_u64 }),
            ))
            .await
            .unwrap();

        let contents = std::fs::read_to_string(tmp.path()).unwrap();
        let lines: Vec<&str> = contents.lines().collect();
        assert_eq!(lines.len(), 2);

        let first: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(first["kind"], "handshake_started");
        assert_eq!(first["payload"]["magic"], 1);

        let second: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(second["kind"], "handshake_completed");
        assert_eq!(second["payload"]["negotiated_version"], 13);
    }

    #[tokio::test]
    async fn concurrent_emits_all_appear_in_file() {
        let tmp = NamedTempFile::new().unwrap();
        let tracer = Tracer::open(tmp.path()).await.unwrap();

        let t1 = tracer.clone();
        let t2 = tracer.clone();
        let (r1, r2) = tokio::join!(
            t1.emit(TraceEvent::new(EventKind::KeepAliveSent, Direction::Sent, json!({ "cookie": 1 }))),
            t2.emit(TraceEvent::new(EventKind::KeepAliveReceived, Direction::Received, json!({ "cookie": 1 }))),
        );
        r1.unwrap();
        r2.unwrap();

        let lines: Vec<_> = std::fs::read_to_string(tmp.path())
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str::<serde_json::Value>(l).unwrap())
            .collect();
        assert_eq!(lines.len(), 2);
    }
}
