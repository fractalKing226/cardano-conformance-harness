# Block-Fetch Conformance Evidence: Pallas 0.36.0

This document records what Pallas's Block-Fetch client does in response to six
specific server-side protocol violations. The harness has no opinion about
correctness — it executes a scripted server, records the wire exchange, and this
document summarises what the client trace shows.

**How to read the table.** Each row is one adversarial scenario. The Expected
column states what a spec-conformant client would do (or a range of acceptable
outcomes where the spec is ambiguous). The Observed column records the actual Pallas
0.36.0 behaviour from a single run against the local devnet.

**How to add to this document.** When running against a different implementation
(e.g. Amaru) or a newer Pallas version, add a new column — do not overwrite the
existing Observed column. If you add a new scenario, add a new row following the
same structure. If the Observed behaviour differs from Expected in a way that
suggests a spec ambiguity, add a note to the Notes section at the bottom.

**How to regenerate.** Requires `fixtures/devnet_blocks.jsonl` (≥10 entries) and
free TCP ports 3010–3015:

```
cargo test --test live_node block_fetch_adversarial -- --ignored
```

---

## Results — Pallas 0.36.0

| Scenario | Port | Protocol Rule Violated | Server Script | Expected Client Reaction | Observed — Pallas 0.36.0 |
|---|---|---|---|---|---|
| `block_fetch_mid_batch_disconnect` | 3010 | BatchDone required to close a batch | StartBatch + 2×Block + Disconnect (no BatchDone) | Error; `exit_reason ≠ completed`; partial block count | Test hit 10-second timeout. Client received both blocks the server sent (2 `block_fetch_block` events) then made no further progress. Pallas's Block-Fetch client does not detect peer disconnect mid-batch; it waits indefinitely for the next Block or BatchDone. No session summary was produced before the timeout fired. |
| `block_fetch_block_outside_batch` | 3011 | Block valid only in Streaming state | Block sent from Busy state (no StartBatch) | State-machine error; `exit_reason ≠ completed`; `blocks_received = 0` | Client errored within 1 ms with message `"inbound message is not valid for current state"` (phase: `request_range`). Session summary: `exit_reason: error`, `blocks_received: 0`. Clean detection with no buffering of the illegal Block message. |
| `block_fetch_batch_done_without_start` | 3012 | BatchDone valid only in Streaming state | BatchDone sent from Busy state (no StartBatch) | State-machine error; `exit_reason ≠ completed`; `blocks_received = 0` | Client errored within 1 ms with message `"inbound message is not valid for current state"` (phase: `request_range`). Session summary: `exit_reason: error`, `blocks_received: 0`. Same fast path as `block_outside_batch` — Pallas's Busy-state guard catches both violations identically. |
| `block_fetch_excessive_blocks` | 3013 | Spec silent on over-delivery | 10 blocks served for a 1-block request | Implementation-dependent: accept, truncate, or error | Client completed normally within 7 ms accepting all 10 blocks. Session summary: `exit_reason: completed`, `blocks_received: 10`. Pallas does not validate the response against the requested range — it streams until BatchDone regardless of how many blocks arrive. See Notes. |
| `block_fetch_malformed_block` | 3014 | CBOR framing must be valid | Raw `0x82 0xff` bytes where StartBatch/NoBlocks expected | CBOR decode error; `exit_reason ≠ completed` | Client errored within 1 ms with message `"error while sending or receiving data through the multiplexer"` (phase: `request_range`). Session summary: `exit_reason: error`, `blocks_received: 0`. The error surfaces at the multiplexer transport layer rather than as a CBOR decode error; see Notes. |
| `block_fetch_no_blocks_after_start` | 3015 | NoBlocks valid only in Busy state | StartBatch then NoBlocks (invalid in Streaming) | State-machine error; `exit_reason ≠ completed`; `blocks_received = 0` | Client errored within 1 ms with message `"inbound message is not valid for current state"` (phase: `recv_while_streaming`). Session summary: `exit_reason: error`, `blocks_received: 0`. Unlike the Busy-state violations, Pallas catches this during the streaming receive loop rather than during range request handling — confirming the state machine guards both transitions. |

---

## Notes

### `block_fetch_mid_batch_disconnect` — client hangs, does not recover

Pallas 0.36.0 has no timeout on the streaming receive loop. After `StartBatch`,
the client blocks on the next message from the server indefinitely. When the server
drops the TCP connection, the underlying socket error is not surfaced through the
Block-Fetch protocol layer within any reasonable window. This is a conformance gap:
a client that cannot detect a dead connection will deadlock any pipeline that depends
on an in-progress block-fetch completing.

A future slice could add a `recv_while_streaming` timeout to the harness client
(mirroring `await_timeout_secs` on Chain-Sync) to produce a bounded diagnostic
rather than a hard hang.

### `block_fetch_excessive_blocks` — Pallas accepts over-delivery

The spec (CIP-0035, §5.2) specifies `RequestRange(from, to)` as requesting "a batch
of blocks starting at `from` and ending at `to`" but does not state that the server
MUST send exactly the blocks between those points, nor that the client MUST reject
extras. Pallas treats the range as a hint and streams until `BatchDone`.

This is a genuine ambiguity worth tracking. If a future version of the spec
tightens the requirement to "exactly the requested range", Pallas would need to add
a range-validation step.

### `block_fetch_malformed_block` — error at multiplexer layer, not CBOR layer

The scenario description says this tests malformed CBOR, but the error Pallas
reports is `"error while sending or receiving data through the multiplexer"`, not a
CBOR decode error. This is because the raw bytes (`0x82ff`) reach the client as a
Block-Fetch channel payload, and the multiplexer's framing machinery raises the
error before the CBOR decoder attempts to parse the message body. The distinction
matters for verifier classification: the violation is detected, but by the wrong
layer. A stricter test would inject the malformed bytes as the body of an otherwise
valid `MsgBlock` frame (i.e., inside an already-started batch), which would require
a `send_sequence` variant that sends `StartBatch` followed by `raw_bytes`. That test
is noted in the scenario file as a future extension.
