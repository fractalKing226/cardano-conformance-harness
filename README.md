# cardano-conformance-harness

A network-level conformance testing harness for Cardano nodes, written in Rust.

The harness speaks Cardano mini-protocols to a node under test, drives scripted
scenarios, and captures the full exchange into a JSON-lines trace file. A
separate verifier will check those traces against Agda specifications.

## What this version does

Reads a scenario JSON file at startup and executes its steps in order. Supports:

- **Client mode** — connect, handshake, chain-sync, block-fetch, query-tip, sleep, disconnect
- **Server mode** — listen, accept incoming connections, serve scripted Chain-Sync and Block-Fetch responses (honest or adversarial)
- **Parallel execution** — run branches of steps concurrently; abort all if any branch fails
- **Named connections** — multiple outgoing or accepted connections in the same scenario, each with an explicit name
- **Peer identity** — connections carry an optional `peer_id` label that propagates to every wire event for trace attribution
- **Variables** — steps can write outputs to named variables that later steps reference with `$name` syntax
- **Adversarial serving** — scripted response rules send spec-incorrect messages (wrong state, truncated batches, malformed CBOR) to produce conformance evidence
- **Fixture capture** — `--capture-fixture` and `--capture-block-fixture` record real-node traffic into replayable JSONL files

## Prerequisites

- Rust toolchain (stable, edition 2024)
- Docker + Docker Compose (for the local devnet)

## Build

```sh
cargo build --release
```

## Local devnet (Docker)

The quickest way to get a real node to test against is the single-node private
devnet derived from the [Hydra project](https://github.com/cardano-scaling/hydra).
It starts forging blocks within seconds, uses **network magic 42**, and needs no
internet connectivity after the first `prepare-devnet.sh` run.

```sh
# 1. Download genesis / key files and stamp the start time
./scripts/prepare-devnet.sh

# 2. Start the node (port 3001 on localhost)
docker compose up

# 3. In a separate terminal, run the default scenario
cargo run -- --scenario scenarios/default.json
```

Re-run `prepare-devnet.sh` whenever you want a fresh chain.

The `devnet/` directory is git-ignored — it contains generated state and key
material that should not be committed.

## Run against preprod testnet

Create a scenario file pointing at a preprod relay and run it:

```sh
cargo run -- --scenario my_preprod_scenario.json
```

## CLI

```
cardano-conformance-harness --scenario <PATH> [--capture-fixture <PATH>] [--capture-block-fixture <PATH>]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--scenario` | `scenarios/default.json` | Path to the scenario JSON file |
| `--capture-fixture` | — | Write every `RollForward` header from `chain_sync` steps to this JSONL file for later use as a `serve_chain_sync` fixture |
| `--capture-block-fixture` | — | Write every block body from `block_fetch` steps (when `batch_size: 1`) to this JSONL file for later use as a `serve_block_fetch` fixture |

### Logging

```sh
RUST_LOG=debug cargo run -- --scenario scenarios/default.json
```

## Scenario file format

A scenario is a JSON file that describes a sequence of steps to execute.

### Minimal example

```json
{
  "name": "chain_sync_then_block_fetch",
  "description": "Sync 10 headers from genesis, then fetch their bodies",
  "target_address": "localhost:3001",
  "network_magic": 42,
  "trace_output_path": "trace.jsonl",
  "steps": [
    { "kind": "connect" },
    { "kind": "handshake" },
    { "kind": "chain_sync", "count": 10 },
    { "kind": "block_fetch", "points": "from_chain_sync" },
    { "kind": "disconnect" }
  ]
}
```

### Header fields

| Field | Required | Description |
|-------|----------|-------------|
| `name` | yes | Human-readable name, emitted in `scenario_started` / `scenario_completed` |
| `description` | no | Free-text description |
| `target_address` | conditional | Node address as `host:port`. Required if any `connect` step omits its own `target_address`; may be absent when every `connect` step specifies its own |
| `network_magic` | yes | Cardano network magic number |
| `trace_output_path` | yes | Path for the JSON-lines trace output |
| `expected_outcome` | no | Informational string logged in `scenario_completed` (e.g. `"success"`) |

### Named connections (`as` and `on`)

Steps that open a connection or listener accept an optional **`as: <name>`** field to give it a name. Steps that act on an existing connection accept an optional **`on: <name>`** field to select which connection to use. Both default to `"default"` — existing single-connection scenarios work unchanged.

```json
{ "kind": "connect",    "as": "peer_a" }
{ "kind": "connect",    "as": "peer_b" }
{ "kind": "handshake",  "on": "peer_a" }
{ "kind": "handshake",  "on": "peer_b" }
{ "kind": "chain_sync", "on": "peer_a", "count": 5  }
{ "kind": "chain_sync", "on": "peer_b", "count": 10 }
```

Every wire event in the trace includes a `"connection": "<name>"` field.

**Validation rules:**
- `as` names must be unique within a scenario.
- `on` must refer to a connection opened by an earlier step.
- The validator reports forward references and duplicates at parse time.

### Peer identity (`peer_id`)

`connect` and `accept_handshake` steps accept an optional **`peer_id: <string>`** field. The label propagates to every wire event emitted on that connection as `"peer_id": "<value>"` in the trace, alongside the `connection` name.

`peer_id` is for external attribution (verifiers, imaginary-network models); `as`/`on` names are internal handles. A connection can have either, both, or neither.

```json
{ "kind": "connect", "as": "conn_a", "peer_id": "alice" }
{ "kind": "connect", "as": "conn_b", "peer_id": "bob" }
```

Multiple connections may share the same `peer_id` (e.g. a peer that reconnects).

### Variables

Steps that produce data can store it in a named variable using the `output` field.
Later steps reference variables using a `$name` prefix.

```json
{ "kind": "query_tip", "output": "tip" }
{ "kind": "chain_sync", "intersection_points": ["$tip.point"], "count": 5 }
```

| Form | Meaning |
|------|---------|
| `$name` | The whole variable value |
| `$name.field` | A field of a record variable |
| `$name[i]` | An element of a list variable |

Variable substitution happens at parameter-binding time, just before each step executes. A `VariableReferenced` trace event is emitted for every substitution; `VariableSet` is emitted when a step stores a result. Unresolved references produce a clear error at that point.

### Step kinds and parameters

> **Note on channel reuse.** Each `chain_sync` and `block_fetch` step consumes its
> multiplexer channel. A second step of the same kind on the same connection will fail
> with "no channel". Use a `disconnect` / `connect` / `handshake` cycle to open a fresh session.

#### `connect`

Opens a TCP connection and subscribes the full N2N mini-protocol suite.

| Parameter | Default | Description |
|-----------|---------|-------------|
| `target_address` | scenario-level `target_address` | Override the scenario-level address for this connection only — useful in parallel scenarios where branches connect to different hosts |
| `peer_id` | — | Peer identity label propagated to all wire events on this connection |

| Protocol | ID | Status |
|----------|----|--------|
| Handshake | 0 | Active — required before any other protocol |
| Chain-Sync | 2 | Active — `chain_sync` step |
| Block-Fetch | 3 | Active — `block_fetch` step |
| Tx-Submission | 4 | Subscribed, idle |
| Keep-Alive | 8 | Active — background loop sends periodic pings |

#### `handshake`

Runs the NodeToNode Handshake mini-protocol. No parameters.

#### `chain_sync`

| Parameter | Default | Description |
|-----------|---------|-------------|
| `intersection_points` | `["origin"]` | Points to intersect at; each is `"origin"` or `"slot:hex_hash"` |
| `count` | `10` | Headers to consume before `MsgDone` |
| `await_timeout_secs` | `30` | Seconds to wait in MustReply state |

When `output` is set, stores collected points as a list variable for a later `block_fetch`.

#### `block_fetch`

| Parameter | Default | Description |
|-----------|---------|-------------|
| `points` | `"from_chain_sync"` | `"from_chain_sync"` (deprecated), a `"$varname"` reference, or an array of `"slot:hex_hash"` strings |
| `batch_size` | `1` | Points per `MsgRequestRange` |

Prefer the explicit variable pattern over `from_chain_sync`:

```json
{ "kind": "chain_sync", "count": 5, "output": "pts" }
{ "kind": "block_fetch", "points": "$pts" }
```

**Fixture capture:** when `--capture-block-fixture` is active and `batch_size: 1`, each received block body is appended to the fixture file with its slot and hash taken from the requested point. `batch_size > 1` does not capture (per-block slot/hash is indeterminate without CBOR parsing).

#### `query_tip`

Opens a temporary TCP connection, performs a handshake, and does a minimal Chain-Sync round-trip to obtain the current chain tip.

| Output variable shape | Fields |
|-----------------------|--------|
| `tip` record | `point` (string), `block_number` (integer) |

```json
{ "kind": "query_tip", "output": "tip" }
```

#### `sleep`

| Parameter | Required | Description |
|-----------|----------|-------------|
| `duration_secs` | yes | Seconds to sleep (literal or `$varname`) |

#### `repeat`

| Parameter | Description |
|-----------|-------------|
| `times` | Number of iterations — a literal integer or a `$varname` reference |
| `body` | Array of steps to execute each iteration |

Variables written inside `body` accumulate to the outer scope. `RepeatIterationStarted` / `RepeatIterationCompleted` events bracket each iteration.

#### `parallel`

Runs multiple branches of steps concurrently. All branches share the same connection and variable stores (via Arc-wrapped state). Each branch gets an independent clone of the runner state at the start.

| Parameter | Description |
|-----------|-------------|
| `branches` | Array of step arrays; each inner array is one branch |

```json
{
  "kind": "parallel",
  "branches": [
    [
      { "kind": "connect", "as": "conn_a" },
      { "kind": "handshake", "on": "conn_a" },
      { "kind": "chain_sync", "on": "conn_a", "count": 5 }
    ],
    [
      { "kind": "connect", "as": "conn_b" },
      { "kind": "handshake", "on": "conn_b" },
      { "kind": "chain_sync", "on": "conn_b", "count": 5 }
    ]
  ]
}
```

All branches run concurrently on the same async executor (cooperative, not OS-thread parallel). If any branch fails, the remaining branches are dropped and the `parallel` step fails. Branches must use disjoint connection names.

Trace events: `parallel_started`, `parallel_branch_started`, `parallel_branch_completed` / `parallel_branch_failed` / `parallel_branch_aborted` (with `last_step` index for aborted branches), `parallel_completed`.

#### `emit_peer_event`

Emits a peer-identity network-state event into the trace without using any connection. Models imaginary-network activity (block production, votes, fork choices) as first-class trace entries independent of wire connections.

| Parameter | Required | Description |
|-----------|----------|-------------|
| `peer_id` | yes | Which peer performed this action |
| `event_kind` | yes | Type of event: `peer_produced_block`, `peer_cast_vote`, `peer_forked_chain`, `peer_joined_network`, `peer_left_network`. Unknown values emit a warning and use `peer_network_event` |
| `payload` | no | Arbitrary JSON object with event-specific data (slot, block_hash, etc.) — carried verbatim in the trace |

```json
{
  "kind": "emit_peer_event",
  "peer_id": "honest_peer",
  "event_kind": "peer_produced_block",
  "payload": { "slot": 1234, "block_hash": "deadbeef" }
}
```

#### `disconnect`

Closes the TCP connection cleanly. No parameters.

---

### Server-side step kinds

A scenario is either **client-mode** or **server-mode**. The validator rejects scenarios that mix the two at load time.

#### `listen`

Binds a TCP listener.

| Parameter | Default | Description |
|-----------|---------|-------------|
| `bind_address` | `"0.0.0.0:3001"` | Address and port to bind |

The listener's `Arc<TcpListener>` is stored so multiple `accept_handshake` steps in parallel branches can accept concurrently.

#### `accept_handshake`

Accepts the next TCP connection and completes the NodeToNode Handshake as the **responder**.

| Parameter | Default | Description |
|-----------|---------|-------------|
| `peer_id` | — | Peer identity label for the accepted connection; propagated to all subsequent wire events on this connection |

Emits `server_bearer_accepted` immediately after the TCP accept (before handshake begins), then handshake wire events with directions reversed (received = peer→harness, sent = harness→peer).

#### `serve_chain_sync`

Executes a response script against the client's Chain-Sync session.

| Parameter | Form | Description |
|-----------|------|-------------|
| `fixture_path` | auto | Path to a chain-sync JSONL fixture. Auto-generates an honest script. |
| `responses` | explicit | Ordered list of response rules (see below). |
| `fixture_path` + `responses` | hybrid | `responses` drives; `fixture_path` provides header sources for `header_from_fixture` references. |
| `await_at_tip_secs` | auto only | Seconds to hold in MustReply at tip (default 30, max 300). |

#### `serve_block_fetch`

Executes a response script against the client's Block-Fetch session.

| Parameter | Form | Description |
|-----------|------|-------------|
| `block_fetch_fixture_path` | — | Path to a block-fetch JSONL fixture. Required if any rule references `block_from_fixture` or `stream_batch` with `block_count_from_request`. |
| `responses` | explicit | Ordered list of response rules (see below). |
| `no_blocks_default` | auto | If `true` and `responses` is absent, auto-generate a script that declines every range with `NoBlocks`. |

At least one of `block_fetch_fixture_path` (with auto-generation) or `responses` is required.

#### `close_listener`

Stops the TCP listener. Active server connections remain open until their `serve_*` step completes. No parameters.

---

### Response rules

Both `serve_chain_sync` and `serve_block_fetch` accept an explicit `responses` list. Each rule has an `on` field and a `send` field. Rules are consumed in order; the first unconsumed rule matching the incoming message fires.

**`on` values:** `request_next`, `find_intersect`, `request_range`, `done`, `any`

**`send` kinds:**

| Kind | Key fields | Notes |
|------|-----------|-------|
| `roll_forward` | `header_from_fixture: N` or `header_cbor: <hex>` + optional `variant: u8` | Chain-Sync only |
| `roll_backward` | `point: "origin"\|"slot:hash"` | Chain-Sync only |
| `intersect_found` | `point` | Chain-Sync only |
| `intersect_not_found` | — | Chain-Sync only |
| `await_reply` | `hold_secs: N` (default 0) | Chain-Sync only; puts client in MustReply |
| `start_batch` | — | Block-Fetch only; MsgStartBatch |
| `block` | `block_from_fixture: N` or `block_cbor: <hex>` | Block-Fetch only; MsgBlock |
| `batch_done` | — | Block-Fetch only; MsgBatchDone |
| `no_blocks` | — | Block-Fetch only; MsgNoBlocks |
| `stream_batch` | `block_indices: [N,…]` or `block_count_from_request: true` | Block-Fetch only; convenience — emits StartBatch + N×Block + BatchDone for one RequestRange without returning to the receive loop |
| `send_sequence` | `sends: [<send>,…]` | Any protocol; emits multiple sends for **one incoming message** without returning to the receive loop between them. Nesting a `send_sequence` inside another is rejected at parse time. |
| `wait` | `duration_secs: N` | Pause without sending |
| `disconnect` | — | Drop the TCP connection immediately |
| `raw_bytes` | `hex: <hex>` | Send arbitrary bytes — malformed CBOR, truncated frames, etc. |

Optional `tip` object on any Chain-Sync send kind overrides the included tip.
Optional `repeatable: true` on any rule makes it refire on every subsequent matching message (useful for auto-generated scripts).

The execution loop never validates responses against the current protocol state. The trace records the state the harness *tracked* before and after each send, so a verifier can see exactly which state the violation occurred in.

#### `send_sequence` — producer-driven streaming

Block-Fetch's streaming model is one-way: after `RequestRange`, the server pushes blocks without client acknowledgment. Rules with a single `send` fire once per received message, which means a `start_batch` rule would wait for a second client message that never arrives in Streaming state. `send_sequence` solves this by emitting all its sub-sends for the **same** incoming message:

```json
{
  "on": "request_range",
  "send": {
    "kind": "send_sequence",
    "sends": [
      { "kind": "start_batch" },
      { "kind": "block", "block_cbor": "a0" },
      { "kind": "block", "block_cbor": "a0" },
      { "kind": "disconnect" }
    ]
  }
}
```

`stream_batch` is the honest variant of this pattern (always ends with `BatchDone`). `send_sequence` covers adversarial variants such as disconnect mid-batch or `NoBlocks` after `StartBatch`.

#### Adversarial example: out-of-state IntersectFound

```json
{
  "kind": "serve_chain_sync",
  "fixture_path": "fixtures/devnet_genesis.jsonl",
  "responses": [
    { "on": "find_intersect", "send": { "kind": "intersect_found", "point": "origin" } },
    { "on": "request_next",   "send": { "kind": "roll_forward", "header_from_fixture": 0 } },
    { "on": "request_next",   "send": { "kind": "intersect_found", "point": "origin" } },
    { "on": "any",            "send": { "kind": "disconnect" } }
  ]
}
```

---

### Fixture formats

#### Chain-Sync fixture (JSONL)

First line is the **anchor**; remaining lines are block headers in chain order.

```jsonl
{"anchor": true}
{"slot":62,"block_hash":"5a3d778e...","block_number":1,"cbor_hex":"828a00183e..."}
{"slot":63,"block_hash":"14051e8b...","block_number":2,"cbor_hex":"828a01183f..."}
```

Capture from a real node:

```sh
cargo run -- \
  --scenario scenarios/capture_chain_for_fixture.json \
  --capture-fixture fixtures/devnet_genesis.jsonl
```

#### Block-Fetch fixture (JSONL)

Same structure. First line is the anchor; remaining lines are block bodies.

```jsonl
{"anchor": true}
{"slot":62,"block_hash":"5a3d778e...","block_cbor_hex":"820a82..."}
{"slot":63,"block_hash":"14051e8b...","block_cbor_hex":"820a83..."}
```

Capture using `batch_size: 1` so each block's slot and hash are known from the request:

```sh
cargo run -- \
  --scenario scenarios/capture_blocks_for_fixture.json \
  --capture-block-fixture fixtures/devnet_blocks.jsonl
```

---

### Pointing a real cardano-node at the harness

Add the harness to the node's topology as an outgoing connection:

```json
{
  "localRoots": [{
    "accessPoints": [{ "address": "127.0.0.1", "port": 3001 }],
    "advertise": false, "trustable": true, "valency": 1
  }],
  "publicRoots": [], "useLedgerAfterSlot": -1
}
```

The harness must be running in server mode before the node starts.

---

### Assertions (`expect`)

Any step can have an optional `expect` object. Failures emit `assertion_failed` and abort the scenario.

```json
{
  "kind": "chain_sync",
  "count": 3,
  "expect": {
    "min_events": 3,
    "must_contain_kind": ["chain_sync_roll_forward"],
    "must_not_contain_kind": ["error"]
  }
}
```

| Field | Description |
|-------|-------------|
| `min_events` | Step must emit at least this many trace events |
| `must_contain_kind` | Each listed kind must appear at least once |
| `must_not_contain_kind` | None of the listed kinds may appear |

---

### Network magic quick-reference

| Network | Magic |
|---------|-------|
| Local devnet | `42` |
| Mainnet | `764824073` |
| Preprod | `1` |
| Preview | `2` |
| SanchoNet | `4` |

---

### Included scenarios

#### Client-side

| File | Description |
|------|-------------|
| `default.json` | Chain-Sync 10 headers + Block-Fetch |
| `chain_sync_only.json` | Chain-Sync 5 headers |
| `chain_sync_then_block_fetch.json` | Chain-Sync 10 + Block-Fetch |
| `assertion_demo.json` | Demonstrates `expect` clauses |
| `query_tip_chain_sync.json` | Query tip, then Chain-Sync from that tip |
| `repeat_demo.json` | Repeat 4×: sync 3 headers + fetch bodies |
| `combined_variables.json` | Variables, query_tip, repeat together |
| `capture_chain_for_fixture.json` | Capture 20 headers to a chain-sync fixture |
| `capture_blocks_for_fixture.json` | Capture 20 block bodies to a block-fetch fixture |
| `multi_peer_two_outgoing.json` | Two named outgoing connections, chain_sync on each |
| `multi_peer_consistency_check.json` | Same range synced from two connections, output to named variables |
| `client_block_fetch_one_range.json` | Minimal block-fetch client for adversarial server tests |

#### Parallel

| File | Description |
|------|-------------|
| `parallel_two_clients.json` | Two client connections opened and synced concurrently |
| `parallel_server_two_chains.json` | Two incoming connections served in parallel from the same listener |
| `parallel_with_error.json` | One branch uses a valid address; the other uses an invalid one — tests abort-on-first-error with per-step `target_address` |

#### Peer identity

| File | Description |
|------|-------------|
| `peer_identity_basic.json` | Two connections with distinct `peer_id`s; trace events carry the labels |
| `peer_identity_with_emit.json` | `emit_peer_event` for `peer_produced_block` interleaved with wire events |
| `peer_identity_anonymous_connection.json` | Connection without `peer_id` — confirms `peer_id` field is absent in trace |

#### Server-side — honest

| File | Description |
|------|-------------|
| `serve_chain_to_one_client.json` | Fixture auto-script, serve until client sends Done |
| `serve_chain_long_session.json` | Fixture auto-script, hold 60 s at tip |
| `scripted_honest_serve.json` | Explicit responses, honest behaviour |
| `scripted_honest_block_fetch.json` | Honest Block-Fetch auto-script from fixture |
| `scripted_no_blocks_decline.json` | Decline every range with NoBlocks |
| `multi_peer_server_two_clients.json` | One listener, two sequential clients served from the same fixture |

#### Server-side — adversarial Chain-Sync

| File | Description |
|------|-------------|
| `scripted_stall_at_tip.json` | 3 headers then AwaitReply for 60 s |
| `scripted_out_of_state_intersect.json` | IntersectFound sent from CanAwait state |

#### Server-side — adversarial Block-Fetch

Each scenario is paired with `client_block_fetch_one_range.json` in the integration tests.

| File | Port | Violation | `peer_id` |
|------|------|-----------|-----------|
| `block_fetch_mid_batch_disconnect.json` | 3010 | StartBatch + 2 blocks + Disconnect (no BatchDone) | `mid_batch_dropper` |
| `block_fetch_block_outside_batch.json` | 3011 | Block sent from Busy state (no preceding StartBatch) | `busy_state_block_sender` |
| `block_fetch_batch_done_without_start.json` | 3012 | BatchDone sent from Busy state (no StartBatch) | `premature_batch_done` |
| `block_fetch_excessive_blocks.json` | 3013 | 10 blocks served for a 1-block request | `block_overflower` |
| `block_fetch_malformed_block.json` | 3014 | Raw `0x82 0xff` CBOR sent in Busy state | `cbor_poisoner` |
| `block_fetch_no_blocks_after_start.json` | 3015 | StartBatch then NoBlocks (invalid in Streaming) | `no_blocks_mid_stream` |

---

## Trace file format

Each line is a self-contained JSON object.

### Top-level fields

| Field | Always present | Description |
|-------|---------------|-------------|
| `timestamp` | yes | RFC 3339 timestamp |
| `kind` | yes | Event kind (snake_case) |
| `direction` | yes | `sent`, `received`, or `internal` |
| `connection` | wire + lifecycle events | Connection name (`"default"` or explicit `as` name) |
| `peer_id` | when set on connection | Peer identity label from `connect`/`accept_handshake` `peer_id` parameter, or from `emit_peer_event` |
| `mini_protocol` | wire events | `"chain-sync"`, `"block-fetch"`, etc. |
| `state_before` | wire events | Protocol state before the message |
| `state_after` | wire events | Protocol state after the message |
| `payload` | yes | Event-specific JSON object |

`sent`/`received` events carry `state_before` and `state_after`. `internal` events carry only `mini_protocol` (when applicable).

### Selected `kind` values

| Value | Direction | Meaning |
|-------|-----------|---------|
| `scenario_started` | internal | Scenario execution begins |
| `scenario_completed` | internal | Scenario finished; `payload.outcome` |
| `step_started` | internal | Step about to execute |
| `step_completed` | internal | Step finished; `payload.outcome` |
| `assertion_passed` / `assertion_failed` | internal | `expect` clause result |
| `variable_set` / `variable_referenced` | internal | Variable store activity |
| `connection_opened` / `connection_closed` | internal | TCP lifecycle |
| `error` | internal | Unexpected error; `payload.error` + `payload.phase` |
| `handshake_version_proposed` | sent | MsgProposeVersions |
| `handshake_version_accepted` | received | MsgAcceptVersion |
| `handshake_completed` | internal | Handshake done |
| `chain_sync_find_intersect` | sent | MsgFindIntersect |
| `chain_sync_intersect_found` | received | MsgIntersectFound |
| `chain_sync_roll_forward` | received | MsgRollForward |
| `chain_sync_roll_backward` | received | MsgRollBackward |
| `chain_sync_await_reply` | received | MsgAwaitReply |
| `chain_sync_session_summary` | internal | Chain-Sync statistics |
| `block_fetch_request_range` | sent | MsgRequestRange |
| `block_fetch_start_batch` | received | MsgStartBatch |
| `block_fetch_block` | received | MsgBlock (full body) |
| `block_fetch_batch_done` | received | MsgBatchDone |
| `block_fetch_no_blocks` | received | MsgNoBlocks |
| `block_fetch_client_done` | sent | MsgClientDone |
| `block_fetch_session_summary` | internal | Block-Fetch statistics |
| `parallel_started` | internal | `parallel` step begins |
| `parallel_branch_started` | internal | One branch starts |
| `parallel_branch_completed` | internal | Branch finished normally |
| `parallel_branch_failed` | internal | Branch returned an error; `payload.step`, `payload.error` |
| `parallel_branch_aborted` | internal | Branch dropped by parent after sibling failure; `payload.last_step` |
| `parallel_completed` | internal | All branches done; `payload.outcome` |
| `peer_produced_block` | internal | `emit_peer_event` with `event_kind: "peer_produced_block"` |
| `peer_cast_vote` | internal | `emit_peer_event` with `event_kind: "peer_cast_vote"` |
| `peer_forked_chain` | internal | `emit_peer_event` with `event_kind: "peer_forked_chain"` |
| `peer_joined_network` | internal | `emit_peer_event` with `event_kind: "peer_joined_network"` |
| `peer_left_network` | internal | `emit_peer_event` with `event_kind: "peer_left_network"` |
| `peer_network_event` | internal | `emit_peer_event` with an unrecognised `event_kind` |
| `server_bearer_accepted` | internal | TCP accept succeeded |
| `server_handshake_accepted` | internal | Responder-side handshake complete |
| `server_block_fetch_started` | internal | Block-Fetch script begins |
| `server_block_fetch_completed` | internal | Block-Fetch script ends; `payload.blocks_served`, `payload.exit_reason` |
| `response_rule_applied` | internal | One script rule fired; `payload.rule_index`, `payload.on`, `payload.send` |

---

## Tests

### Unit tests

```sh
cargo test
```

90 unit tests covering:

- `trace::tests` — serialisation, `direction`, `with_protocol`/`with_states`, `peer_id` propagation, concurrent emit
- `scenario::tests` — parsing, validation, `parse_point`, per-step `target_address`, `peer_id` placement rules
- `scenario::fixture::tests` — load/save round-trip, cursor operations, anchor regression
- `scenario::block_fixture::tests` — load, range lookup, round-trip
- `scenario::vars::tests` — `resolve_ref` (all forms + error cases), `substitute_in_value`, `point_to_str`
- `scenario::runner::tests` — assertion evaluator, `subscribed_protocols` suite, worker spawn lifecycle
- `scenario::response_rules::tests` — rule parsing, `rule_def_to_script` resolution, `stream_batch` generation
- `miniprotocols::handshake::tests` — graceful error on refused connection
- `miniprotocols::chainsync::tests` — hex encoding, point extraction, payload fields
- `miniprotocols::blockfetch::tests` — payload fields, summary fields, **capture_blocks_populated_with_batch_size_one**, **capture_blocks_empty_when_batch_size_gt_one**

### Integration tests

#### Devnet tests — require `docker compose up`

```sh
cargo test --test live_node -- --ignored
```

Tests handshake, chain-sync, block-fetch, keep-alive, Tx-Submission, and variable-driven scenarios against the local devnet.

#### Block-Fetch adversarial tests — require free TCP ports 3010–3015

```sh
cargo test --test live_node -- --ignored block_fetch_adversarial
```

Six tests, each pairing an adversarial server scenario with the `client_block_fetch_one_range.json` client. All have a 10-second timeout: if the client hangs (doesn't detect the violation within 10 s) the test records that as a conformance finding rather than a hard failure — except for `mid_batch_disconnect`, where hanging is itself an expected outcome.

See `docs/block_fetch_conformance_results.md` for the results table.

---

## Project structure

```
src/
  lib.rs                          — library root; module declarations, DEVNET_MAGIC
  main.rs                         — CLI: load scenario, run ScenarioRunner
  trace.rs                        — TraceEvent, EventKind, Direction, Tracer        [unit tests]
  miniprotocols/
    mod.rs
    handshake.rs                  — NodeToNode Handshake                            [unit tests]
    chainsync.rs                  — Chain-Sync N2N client                           [unit tests]
    chainsync_server.rs           — Chain-Sync response script executor
    blockfetch.rs                 — Block-Fetch N2N client + fixture capture        [unit tests]
    blockfetch_server.rs          — Block-Fetch response script executor
    keepalive.rs                  — Keep-Alive client + server
    txsubmission.rs               — Tx-Submission (passive receive)
  scenario/
    mod.rs                        — Scenario, StepDef, StepKind, validation         [unit tests]
    runner.rs                     — ScenarioRunner, step handlers, CheckedOut guard  [unit tests]
    vars.rs                       — variable store, substitution                    [unit tests]
    fixture.rs                    — chain-sync fixture load/save/cursor             [unit tests]
    block_fixture.rs              — block-fetch fixture load/save/range lookup      [unit tests]
    response_rules.rs             — response rule types, send_sequence, conversion  [unit tests]
scenarios/
  *.json                          — see scenario catalogue above
fixtures/
  devnet_genesis.jsonl            — 20 chain-sync headers captured from local devnet
  devnet_blocks.jsonl             — 20 block bodies (produced by capture_blocks_for_fixture.json)
docs/
  block_fetch_conformance_results.md — adversarial test findings (TBD columns after first run)
tests/
  live_node.rs                    — integration tests (devnet + adversarial)
scripts/
  prepare-devnet.sh               — downloads Hydra devnet config, stamps genesis time
```

## What's next

- Parse slot/hash/block_number from Block-Fetch block body CBOR to enable fixture capture with `batch_size > 1`
- Peer-Sharing mini-protocol (once pallas-network exposes it; version ≥ 13)
- Tx-Submission step handler
- Agda specification verifier integration — consume trace files and check against formal spec
- Populate `docs/block_fetch_conformance_results.md` Observed column after first adversarial run
