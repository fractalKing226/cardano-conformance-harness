# Block-Fetch Conformance Evidence: Pallas 0.36.0

This document records the harness's observations of Pallas's Block-Fetch client behaviour
in response to six specific server-side protocol violations. Each row corresponds to one
adversarial scenario in `scenarios/`.

The harness has no opinion about correctness — it executes a scripted server, records the
wire exchange, and this document summarises what the trace shows. Future verifier work
will interpret these observations against the formal spec.

Run all six tests manually (requires free TCP ports 3010–3015):

```
cargo test --test live_node -- --ignored block_fetch_adversarial
```

---

## Results table

| Scenario | Port | Protocol Rule Violated | Server Script | Expected Client Reaction | Observed — Pallas 0.36.0 |
|---|---|---|---|---|---|
| `block_fetch_mid_batch_disconnect` | 3010 | BatchDone required to close a batch (§5.2) | StartBatch + 2×Block + Disconnect (no BatchDone) | Error; `exit_reason ≠ completed`; partial block count in summary | TBD |
| `block_fetch_block_outside_batch` | 3011 | Block valid only in Streaming state | Block sent from Busy state (no StartBatch) | State-machine error; `exit_reason ≠ completed`; `blocks_received = 0` | TBD |
| `block_fetch_batch_done_without_start` | 3012 | BatchDone valid only in Streaming state | BatchDone sent from Busy state (no StartBatch) | State-machine error; `exit_reason ≠ completed`; `blocks_received = 0` | TBD |
| `block_fetch_excessive_blocks` | 3013 | Spec silent; clients may validate range | 10 blocks served for a 1-block request | Implementation-dependent: accept, truncate, or error | TBD |
| `block_fetch_malformed_block` | 3014 | CBOR framing must be valid | Raw `0x82 0xff` bytes where StartBatch/NoBlocks expected | CBOR decode error; `exit_reason ≠ completed` | TBD |
| `block_fetch_no_blocks_after_start` | 3015 | NoBlocks valid only in Busy state | StartBatch then NoBlocks (invalid in Streaming) | State-machine error; `exit_reason ≠ completed`; `blocks_received = 0` | TBD |

---

## Notes

**Fixture dependency.** `block_fetch_excessive_blocks` requires
`fixtures/devnet_blocks.jsonl` with at least 10 entries. Capture this by running
the `capture_chain_for_fixture` scenario against a live devnet, then a corresponding
block-fetch capture scenario.

**`send_sequence` primitive.** Scenarios `mid_batch_disconnect` and
`no_blocks_after_start` use the `send_sequence` send rule, which emits an ordered list
of sub-sends for a single incoming message without returning to the receive loop. This
is the natural primitive for Block-Fetch's producer-driven streaming model, where the
server pushes multiple messages after one `RequestRange` without client acknowledgment
between them.

**Malformed block reframing.** `block_fetch_malformed_block` sends garbage CBOR in
**Busy** state (where StartBatch/NoBlocks is expected), not in Streaming state after
StartBatch. The stricter mid-stream variant (StartBatch + malformed block bytes) is a
valid future extension expressible as a `send_sequence` once the integration is confirmed
working.

**Populating the Observed column.** After running the tests, update each TBD with the
actual `exit_reason` string and `blocks_received` count from the client trace. Example:

```
Observed: exit_reason="channel_recv_error", blocks_received=1, state_at_exit="Streaming"
```
