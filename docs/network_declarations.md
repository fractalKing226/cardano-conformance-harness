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
