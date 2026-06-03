# cardano-conformance-harness

A network-level conformance testing harness for Cardano nodes, written in Rust.

The harness speaks Cardano mini-protocols to a node under test, drives scripted
scenarios, and captures the full exchange into a JSON-lines trace file. A
separate verifier will check those traces against Agda specifications.

## What this version does

Reads a scenario JSON file at startup and executes its steps in order. Each step
corresponds to a protocol action: connect, handshake, chain-sync, block-fetch,
sleep, or disconnect. Every message sent and received across all mini-protocols
is appended to a JSON-lines trace file.

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
cardano-conformance-harness --scenario <PATH>
```

| Flag | Default | Description |
|------|---------|-------------|
| `--scenario` | `scenarios/default.json` | Path to the scenario JSON file |

### Logging

```sh
RUST_LOG=debug cargo run -- --scenario scenarios/default.json
```

## Scenario file format

A scenario is a JSON file that describes a sequence of steps to execute.

### Minimal example (28 lines)

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
| `target_address` | yes | Node address as `host:port` |
| `network_magic` | yes | Cardano network magic number |
| `trace_output_path` | yes | Path for the JSON-lines trace output |
| `expected_outcome` | no | Informational string logged in `scenario_completed` (e.g. `"success"`) |

### Step kinds and parameters

#### `connect`

Opens a TCP connection to `target_address`. Subscribes all currently-supported
protocols on the multiplexer (Handshake=0, Chain-Sync=2, Block-Fetch=3).
Unused channels sit idle at negligible cost. No parameters.

A scenario may contain multiple `connect` / `disconnect` cycles in any order.

#### `handshake`

Runs the NodeToNode Handshake mini-protocol. No parameters.

#### `chain_sync`

| Parameter | Default | Description |
|-----------|---------|-------------|
| `intersection_points` | `["origin"]` | Points to intersect at; each is `"origin"` or `"slot:hex_hash"` |
| `count` | `10` | Headers to consume before `MsgDone` |
| `await_timeout_secs` | `30` | Seconds to wait in MustReply state |

Collects the point of each `RollForward` header (slot + blake2b-256 of header
bytes) for use by a subsequent `block_fetch` step.

#### `block_fetch`

| Parameter | Default | Description |
|-----------|---------|-------------|
| `points` | `"from_chain_sync"` | `"from_chain_sync"` uses points from the most recent `chain_sync`; or an array of `"slot:hex_hash"` strings |
| `batch_size` | `1` | Points per `MsgRequestRange` |

#### `disconnect`

Closes the TCP connection cleanly. No parameters.

#### `sleep`

| Parameter | Required | Description |
|-----------|----------|-------------|
| `duration_secs` | yes | Seconds to sleep |

### Assertions (`expect`)

Any step can have an optional `expect` object. Assertions are evaluated against
the events emitted by that step. Failures emit `assertion_failed` and abort the
scenario with a non-zero exit code.

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

### Network magic quick-reference

| Network | Magic |
|---------|-------|
| Local devnet | `42` |
| Mainnet | `764824073` |
| Preprod | `1` |
| Preview | `2` |
| SanchoNet | `4` |

### Included scenarios

| File | Description |
|------|-------------|
| `scenarios/default.json` | Chain-Sync 10 headers + Block-Fetch (the default) |
| `scenarios/chain_sync_only.json` | Chain-Sync 5 headers, no block fetch |
| `scenarios/chain_sync_then_block_fetch.json` | Chain-Sync 10 + Block-Fetch |
| `scenarios/assertion_demo.json` | Demonstrates `expect` clauses |

## Trace file format

Each line is a self-contained JSON object.

### Direction values

| Value | Meaning |
|-------|---------|
| `sent` | Wire message sent by the harness |
| `received` | Wire message received from the node |
| `internal` | Harness-generated meta-event (session start/end, summary, assertion, error) — not a single wire message |

`sent` and `received` events carry `state_before` and `state_after`.
`internal` events carry only `mini_protocol` (when applicable) and `payload`.

### Scenario-level events

```json
{"kind":"scenario_started","direction":"internal","payload":{"name":"chain_sync_only","steps":4,...}}
{"kind":"step_started","direction":"internal","payload":{"index":0,"kind":"connect"}}
{"kind":"step_completed","direction":"internal","payload":{"index":0,"outcome":"ok"}}
{"kind":"assertion_passed","direction":"internal","payload":{"step_index":1,"assertion":"must_contain_kind:handshake_completed","message":"found event with kind \"handshake_completed\""}}
{"kind":"scenario_completed","direction":"internal","payload":{"name":"chain_sync_only","steps_passed":4,"steps_failed":0,"duration_ms":87,"outcome":"completed"}}
```

### Handshake events

```json
{"kind":"connection_opened","direction":"internal","payload":{"addr":"localhost:3001"}}
{"kind":"handshake_started","direction":"internal","payload":{"magic":42}}
{"kind":"handshake_version_proposed","direction":"sent","payload":{"magic":42,"versions":[7,8,9,10,11,12,13,14]}}
{"kind":"handshake_version_accepted","direction":"received","payload":{"peer_data":"VersionData { ... }","version":14}}
{"kind":"handshake_completed","direction":"internal","payload":{"negotiated_version":14}}
```

### Chain-Sync events

```json
{"kind":"chain_sync_started","direction":"internal","mini_protocol":"chain-sync","payload":{"target_headers":10}}
{"kind":"chain_sync_find_intersect","direction":"sent","mini_protocol":"chain-sync","state_before":"Idle","state_after":"Intersect","payload":{"points":["origin"]}}
{"kind":"chain_sync_intersect_found","direction":"received","mini_protocol":"chain-sync","state_before":"Intersect","state_after":"Idle","payload":{"point":"origin","tip":{...}}}
{"kind":"chain_sync_roll_forward","direction":"received","mini_protocol":"chain-sync","state_before":"CanAwait","state_after":"Idle","payload":{"variant":6,"cbor_hex":"828a00...","cbor_len":815,"tip":{...}}}
{"kind":"chain_sync_session_summary","direction":"internal","mini_protocol":"chain-sync","payload":{"headers_received":10,"collected_points":10,...}}
```

### Block-Fetch events

```json
{"kind":"block_fetch_started","direction":"internal","mini_protocol":"block-fetch","payload":{"total_points":10,"batch_size":1}}
{"kind":"block_fetch_request_range","direction":"sent","mini_protocol":"block-fetch","state_before":"Idle","state_after":"Busy","payload":{"from":{"slot":62,"hash":"5a3d77..."},"to":{"slot":62,...},"batch_len":1}}
{"kind":"block_fetch_block","direction":"received","mini_protocol":"block-fetch","state_before":"Streaming","state_after":"Streaming","payload":{"cbor_hex":"820a82...","cbor_len":1234}}
{"kind":"block_fetch_session_summary","direction":"internal","mini_protocol":"block-fetch","payload":{"blocks_received":10,"total_bytes":12340,...}}
```

### `kind` values

| Value | Direction | Meaning |
|-------|-----------|---------|
| `scenario_started` | internal | Scenario execution begins |
| `scenario_completed` | internal | Scenario finished; see `payload.outcome` |
| `step_started` | internal | A step is about to execute |
| `step_completed` | internal | A step finished; see `payload.outcome` |
| `assertion_passed` | internal | An `expect` clause passed |
| `assertion_failed` | internal | An `expect` clause failed (scenario aborted) |
| `connection_opened` | internal | TCP connection established |
| `connection_closed` | internal | Connection torn down |
| `error` | internal | Unexpected error; see `payload.error` and `payload.phase` |
| `handshake_started` | internal | Handshake mini-protocol initiated |
| `handshake_version_proposed` | sent | MsgProposeVersions |
| `handshake_version_accepted` | received | MsgAcceptVersion |
| `handshake_version_rejected` | received | MsgRefuse |
| `handshake_completed` | internal | Handshake Done |
| `chain_sync_started` | internal | Chain-Sync session opened |
| `chain_sync_find_intersect` | sent | MsgFindIntersect |
| `chain_sync_intersect_found` | received | MsgIntersectFound |
| `chain_sync_intersect_not_found` | received | MsgIntersectNotFound |
| `chain_sync_request_next` | sent | MsgRequestNext |
| `chain_sync_roll_forward` | received | MsgRollForward (header) |
| `chain_sync_roll_backward` | received | MsgRollBackward |
| `chain_sync_await_reply` | received | MsgAwaitReply |
| `chain_sync_done` | sent | MsgDone |
| `chain_sync_session_summary` | internal | Chain-Sync statistics |
| `block_fetch_started` | internal | Block-Fetch session opened |
| `block_fetch_request_range` | sent | MsgRequestRange |
| `block_fetch_no_blocks` | received | MsgNoBlocks |
| `block_fetch_start_batch` | received | MsgStartBatch |
| `block_fetch_block` | received | MsgBlock (full body) |
| `block_fetch_batch_done` | received | MsgBatchDone |
| `block_fetch_client_done` | sent | MsgClientDone |
| `block_fetch_session_summary` | internal | Block-Fetch statistics |

### MustReply

`chain_sync_await_reply` appears when the harness catches up to the node's tip.
The `await_timeout_secs` scenario parameter (default 30 s) controls how long
the harness waits. On the local devnet this almost never triggers (blocks every
0.1 s). Against preprod (20 s average block time) it is common if `count` is
large enough to reach the tip.

## Tests

### Unit tests

```sh
cargo test
```

- `trace::tests` — serialisation, `Direction::Internal`, `with_protocol`/`with_states`
- `scenario::tests` — parsing, validation, `parse_point`
- `scenario::runner::tests` — assertion evaluator (all eight cases)
- `miniprotocols::handshake::tests` — graceful error on refused connection
- `miniprotocols::chainsync::tests` — hex encoding, point extraction, payload fields
- `miniprotocols::blockfetch::tests` — payload fields, summary fields

### Integration tests

Require the devnet (`docker compose up`):

```sh
cargo test --test live_node -- --ignored
```

| Test | What it checks |
|------|---------------|
| `handshake_completes_against_devnet` | Full handshake, version negotiation, trace sequence |
| `handshake_rejected_with_wrong_magic` | Node rejects unknown magic; trace stays valid |
| `chain_sync_receives_n_headers_from_devnet` | 5 headers; event sequence, payload fields, summary |
| `chain_sync_intersect_found_at_genesis` | `Point::Origin` resolves to `"origin"` |
| `block_fetch_fetches_blocks_from_devnet` | 5 blocks; event sequence, `cbor_len > 0`, summary |

## Project structure

```
src/
  lib.rs                    — library root; module declarations, DEVNET_MAGIC
  main.rs                   — CLI: load scenario file, run ScenarioRunner
  trace.rs                  — TraceEvent, EventKind, Direction, Tracer        [unit tests inline]
  miniprotocols/
    mod.rs
    handshake.rs            — NodeToNode Handshake                            [unit tests inline]
    chainsync.rs            — Chain-Sync N2N (headers + point extraction)     [unit tests inline]
    blockfetch.rs           — Block-Fetch N2N (block bodies)                  [unit tests inline]
  scenario/
    mod.rs                  — Scenario, StepDef, Assertions types + validation [unit tests inline]
    runner.rs               — ScenarioRunner, step handlers, assertions        [unit tests inline]
scenarios/
  default.json              — default scenario (chain-sync + block-fetch)
  chain_sync_only.json
  chain_sync_then_block_fetch.json
  assertion_demo.json
tests/
  live_node.rs              — integration tests against the Docker devnet
scripts/
  prepare-devnet.sh         — downloads Hydra devnet config and stamps genesis time
```

## What's next

- Tx-Submission mini-protocol
- Parse slot/hash/block_number from Block-Fetch block body CBOR
- Named connection handles for multi-connection scenarios
- Version-conditional protocol subscriptions (e.g. Peer-Sharing on N2N v13+)
- Agda specification verifier integration
