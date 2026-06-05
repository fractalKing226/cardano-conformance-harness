# Network Declarations

Network declarations are an optional top-level field in a scenario file that give the
imaginary network of peers a vocabulary. Scenarios can then reference peers by identity
in protocol-serving steps rather than by fixture paths. This is the first slice of a
multi-part arc toward **automatic content generation** — future slices will replace
fixture paths with computed content (block production, vote casting, fork choices),
but the peer vocabulary introduced here does not change.

---

## The `network` block

```json
{
  "network": {
    "peers": [
      {
        "id": "honest_peer",
        "chain_sync_fixture":  "fixtures/chain_a.jsonl",
        "block_fetch_fixture": "fixtures/chain_a_blocks.jsonl",
        "description": "The canonical chain producer"
      },
      {
        "id": "adversary",
        "chain_sync_fixture": "fixtures/chain_b.jsonl",
        "description": "An adversary holding a divergent chain"
      }
    ]
  }
}
```

### Peer fields

| Field | Required | Description |
|-------|----------|-------------|
| `id` | yes | Unique string identifying this peer within the scenario |
| `chain_sync_fixture` | at least one | Path to a chain-sync JSONL fixture this peer serves |
| `block_fetch_fixture` | at least one | Path to a block-fetch JSONL fixture this peer serves |
| `description` | no | Human-readable description of this peer's role |

**Validation rules:**

- Peer `id`s must be unique within the `network` block.
- Each peer must have at least one fixture (`chain_sync_fixture` or `block_fetch_fixture`);
  a peer with neither is rejected at parse time.
- `chain_sync_fixture` and `block_fetch_fixture` are independent — a peer can have one,
  both, or neither (though at least one is required). They are resolved independently by
  the protocol step that uses them.

---

## The `as_peer` step parameter

`serve_chain_sync` and `serve_block_fetch` accept an optional `as_peer: "<id>"` parameter.
When present:

1. The step resolves the peer's protocol-specific fixture:
   - `serve_chain_sync as_peer:` uses `peer.chain_sync_fixture`. If the peer has no
     `chain_sync_fixture`, the step errors at runtime with a clear message.
   - `serve_block_fetch as_peer:` uses `peer.block_fetch_fixture`. If the peer has no
     `block_fetch_fixture`, the step errors at runtime.
   - Each protocol looks only at its own fixture field and ignores the other.

2. The serving connection's `peer_id` is set to `peer.id`, **overriding** any `peer_id`
   previously set at `accept_handshake` time. This means all wire events emitted by the
   serve step are attributed to the declared peer in the trace.

3. `as_peer` and the corresponding inline fixture path are **mutually exclusive**:
   - `serve_chain_sync`: cannot have both `fixture_path` and `as_peer`
   - `serve_block_fetch`: cannot have both `block_fetch_fixture_path` and `as_peer`
   - The validator rejects both at parse time.

4. `as_peer` references must name a peer declared in the scenario's `network` block.
   Using `as_peer` without a `network` block, or referencing an undeclared peer, is a
   parse-time validation error.

### `as_peer` + `responses` (hybrid mode)

`as_peer` and `responses` are compatible. When both are present, `responses` drives the
script and the peer's fixture acts as a header/block source for `header_from_fixture` or
`block_from_fixture` references within those responses.

---

## Peer-id precedence

The `peer_id` on a connection can be set in two ways:

| Where set | When it applies |
|-----------|-----------------|
| `accept_handshake peer_id:` | At connection time (before any serve step runs) |
| `serve_chain_sync as_peer:` or `serve_block_fetch as_peer:` | At serve time (overrides the accept-time value) |

`as_peer` takes precedence: it writes `peer.id` to the connection's `peer_id` field
just before the tracer is created for that serve step. This design is deliberate —
the `as_peer` declaration is the most specific statement of which peer is serving
this content. The `accept_handshake peer_id:` sets identity at the transport level;
`as_peer` sets it at the content level, which is what the trace verifier cares about.

---

## Trace events

When a scenario with a `network` block starts, the harness emits a single
`network_declared` event immediately after `scenario_started`:

```json
{
  "kind": "network_declared",
  "direction": "internal",
  "payload": {
    "peers": [
      { "id": "honest_peer", "chain_sync_fixture": "fixtures/chain_a.jsonl" },
      { "id": "adversary",   "chain_sync_fixture": "fixtures/chain_b.jsonl" }
    ]
  }
}
```

This event records the imaginary topology at scenario start so a trace reader (or
verifier) knows which peers existed and what chains they held, without having to
reconstruct it from the step definitions.

Wire events emitted during `serve_chain_sync` or `serve_block_fetch` with `as_peer`
carry the peer identity in the `peer_id` field:

```json
{
  "kind": "chain_sync_roll_forward",
  "direction": "sent",
  "connection": "client_a",
  "peer_id": "honest_peer",
  "mini_protocol": "chain-sync",
  ...
}
```

---

## Example scenario

```json
{
  "name": "two_peers_different_chains",
  "network_magic": 42,
  "trace_output_path": "trace.jsonl",
  "network": {
    "peers": [
      { "id": "honest_peer", "chain_sync_fixture": "fixtures/chain_a.jsonl" },
      { "id": "adversary",   "chain_sync_fixture": "fixtures/chain_b.jsonl" }
    ]
  },
  "steps": [
    { "kind": "listen", "bind_address": "0.0.0.0:3020" },
    {
      "kind": "parallel",
      "branches": [
        [
          { "kind": "accept_handshake", "as": "client_a" },
          { "kind": "serve_chain_sync", "on": "client_a", "as_peer": "honest_peer" }
        ],
        [
          { "kind": "accept_handshake", "as": "client_b" },
          { "kind": "serve_chain_sync", "on": "client_b", "as_peer": "adversary" }
        ]
      ]
    },
    { "kind": "close_listener" }
  ]
}
```

The harness serves `chain_a.jsonl` to `client_a` (attributed to `honest_peer`) and
`chain_b.jsonl` to `client_b` (attributed to `adversary`) concurrently. The trace
is fully attributed — a verifier can tell exactly which peer served which content to
which connection without any inference.

---

---

## Slot evolution

The imaginary network has a clock. Scenarios advance it explicitly; the harness
does not tick it automatically.

### Time fields in the `network` block

| Field | Default | Description |
|-------|---------|-------------|
| `start_slot` | `0` | The slot the network begins at when the scenario starts |
| `slot_length_ms` | `1000` | Wall-clock ms per slot — vocabulary reserved for future use; not acted on in this slice |

### `advance_to_slot`

Sets the current slot to an absolute value.

```json
{ "kind": "advance_to_slot", "slot": 200 }
```

- Requires a `network` block (errors otherwise).
- `slot` must be **strictly greater than** the current slot. Same-slot and rewind
  advances are rejected at runtime: `"advance_to_slot: target slot N is not greater
  than current slot M"`. This is intentional — a same-slot advance is almost always
  a scenario author mistake.

### `tick_slots`

Advances the current slot by a relative number of positions.

```json
{ "kind": "tick_slots", "count": 50 }
```

- `count` must be at least 1 (validated at parse time). Zero is degenerate.
- The operation is atomic (`fetch_add`), so it is safe in parallel branches.

Both steps work in client-mode and server-mode scenarios, and are legal inside
`repeat` and `parallel` bodies.

### `SlotAdvanced` trace event

Every slot change emits a `slot_advanced` event:

```json
{
  "kind": "slot_advanced",
  "direction": "internal",
  "slot": 200,
  "payload": { "from_slot": 100, "to_slot": 200, "reason": "advance_to_slot" }
}
```

`reason` is `"advance_to_slot"` or `"tick_slots"`. The top-level `slot` field equals
`to_slot` — the event is pinned to the slot it just established.

### Slot context on all events

When a scenario has a `network` declaration, **every trace event** automatically
carries a `slot` field reflecting the current imaginary-network slot at the moment
of emission:

```json
{ "kind": "chain_sync_roll_forward", "slot": 250, "direction": "sent", ... }
```

This applies to wire events, internal meta-events, and `SlotAdvanced` itself.
Events in scenarios without a `network` block omit the `slot` field entirely —
the field is never present as `null`, only absent.

**Ordering note.** The slot is read with `Relaxed` ordering: it is a logical
counter for trace attribution, not a memory-ordering barrier. Events emitted
nanoseconds before a slot change may carry the previous slot; the `SlotAdvanced`
event is the authoritative record of when the transition occurred.

---

---

## Peer state (slice 3)

Each declared peer gains **runtime state** — an in-memory chain that persists across steps and can be extended during scenario execution. Serve steps consult the peer's current state rather than re-reading the fixture file.

### How state is initialized

At scenario start, for each declared peer:

1. If `chain_sync_fixture` is set, load it into the peer's `chain_entries` (one `ChainEntry` per fixture line).
2. If `block_fetch_fixture` is set, load it into the peer's `block_store` (slot+hash → body bytes).
3. Emit `peer_state_initialized { peer_id, chain_entries_loaded, blocks_loaded }` — emitted for every peer including those with no fixtures (`entries_loaded: 0`).

The fixture is read once at startup. Subsequent `serve_chain_sync as_peer:` steps use the in-memory state, not the file.

### `peer_extends_chain`

Appends a new header (and optionally a block body) to a named peer's chain. Any `serve_chain_sync as_peer:` step that runs afterwards sees the extended chain.

| Parameter | Required | Description |
|-----------|----------|-------------|
| `peer_id` | yes | Which peer to extend |
| `slot` | yes | Slot of the new block (explicit — not parsed from CBOR) |
| `block_number` | yes | Block height |
| `block_hash` | yes | 64-char hex (32 bytes) |
| `header_cbor` | yes | Hex-encoded header bytes |
| `variant` | no | Era variant byte (default: `DEFAULT_HEADER_VARIANT`) |
| `block_body_cbor` | no | Hex-encoded block body — if present, added to block_store |

**Monotonic slot check.** The new entry's slot must be strictly greater than the current chain tip slot. When the chain is empty, any slot is accepted (no "tip slot 0" error). Error message: `"peer_extends_chain: slot N is not greater than current chain tip slot M"`.

The parameter set mirrors the fixture JSONL format — scenario authors who've captured a fixture can lift an entry verbatim.

```json
{
  "kind": "peer_extends_chain",
  "peer_id": "block_producer",
  "slot": 158,
  "block_number": 20,
  "block_hash": "0000...0158",
  "header_cbor": "828a...",
  "variant": 6
}
```

### Slot-aware serving

When the scenario has a `current_slot` (from `network.start_slot` + `advance_to_slot`/`tick_slots`), `serve_chain_sync as_peer:` only exposes entries with `slot <= current_slot`. Entries in the peer's chain with higher slots are invisible until time advances.

```
fixture: 20 entries at slots 138-157
advance_to_slot: 145
serve_chain_sync: sees only 8 entries (slots 138-145)
```

If the scenario has no `network` block, or `current_slot` is not set, all entries are exposed — identical to today's behavior. Slot-aware serving is purely opt-in.

### Trace events

| Kind | When | Payload fields |
|------|------|----------------|
| `peer_state_initialized` | Once per peer at scenario start | `peer_id`, `chain_entries_loaded`, `blocks_loaded` |
| `peer_chain_extended` | On each `peer_extends_chain` step | `peer_id`, `slot`, `block_hash`, `block_number`, `source` |

### Locking

Peer state lives in `RunnerState.peers: Arc<Mutex<HashMap<String, PeerState>>>`. Serve steps take a brief lock, clone the relevant entries, and release before any long-running I/O. `peer_extends_chain` takes a brief lock to push one entry and releases immediately. Parallel branches can read the same peer's chain simultaneously (unlike connections, which are exclusively checked out).

---

## What future slices will change

This slice introduces the vocabulary without changing the content model. In subsequent
slices:

- `chain_sync_fixture` and `block_fetch_fixture` will become optional when the peer has
  a **chain model** (slot leader schedule, block production parameters). The harness will
  compute header and block content on the fly.
- A peer's `chain_sync_fixture` path will be replaceable by an inline chain description
  (genesis parameters + slot assignments) that the harness expands to wire-level headers.
- Cross-peer relationships ("peer B votes on peer A's block at slot N") will be
  expressible once the content model exists.

Scenarios written with `network` + `as_peer` today will continue to work as-is when
those future slices land — the vocabulary is stable, only the content source changes.
