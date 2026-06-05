# Adversarial Production Evidence: Pallas 0.36.0

This document records what Pallas's Chain-Sync client does when served chains produced by adversarial production rules. Slice 5 introduced three new `ProductionRule` variants that generate specific chain-level defects without requiring cryptographic invalidity.

**What this document represents.** Each row is one adversarial scenario. The Expected column describes what a fully spec-conformant client would do (or the range of conformant outcomes where the spec is ambiguous). The Observed column records what the implementation under test actually did. Both expected and observed behavior are evidence — unexpected behavior is not a documentation error, it is a conformance finding.

**How to use this document.** When testing a different implementation (e.g., Amaru, Haskell node), add a new "Observed — Impl X.Y" column; do not overwrite the Pallas column. When adding a new adversarial production rule, add a new row. When a finding in the Observed column contradicts Expected, add a note to the "Findings of Interest" section at the bottom rather than adjusting Expected.

**How to read the table.** Each row is one adversarial scenario. The "Defect" column names the protocol-layer defect introduced. The "Expected Reaction" column states what a fully-validating client would do. "Observed" records actual Pallas behavior.

**How to add to this document.** Run the integration tests with `--ignored` after ensuring the harness is built. Add an "Observed — Pallas X.Y.Z" column for each version tested. Do not overwrite existing columns.

**How to regenerate.** Requires free TCP ports 3027–3029:

```
cargo test --test live_node -- --ignored adversarial_production
```

---

## Adversarial production rules

### `forked_from_slot`

Two peers share `first_slot`, `interval`, and `fork_slot` but have different `fork_markers`. Before `fork_slot` their chains are byte-for-byte identical. From `fork_slot` onward, each peer's block hashes diverge — two valid-looking but incompatible continuations of the same history.

Adversarial intent: equivocation evidence. The harness can present two conflicting continuations to a client and observe whether the client detects the inconsistency or blindly follows one chain.

Trace: `peer_chain_extended` events carry `defect_kind: "fork_divergence"` on all post-fork blocks.

### `skips_slots`

Like `every_n_slots` but specific scheduled slots are omitted. Block numbers remain sequential; slot values have gaps. This is a legitimate scenario in Cardano (slot leaders who miss their slot), but combined with a known gap pattern it lets the test verify the client's slot-continuity assumptions.

Adversarial intent: non-contiguous slot sequences. Tests whether a client assumes consecutive slots or handles gaps correctly.

Trace: all produced blocks carry `defect_kind: "sparse_chain"` regardless of which slots were skipped (the production itself is adversarial even if individual blocks are valid).

### `broken_prev_hash`

Produces blocks normally until `break_at_slot`, then substitutes `wrong_hash` as the `prev_hash` field in subsequent blocks. Individual block CBOR is syntactically valid; the chain's hash-link is not.

Adversarial intent: hash-chain integrity violation. Tests whether the client validates the backward hash chain during chain-sync or defers that check to later.

Trace: `defect_kind: "broken_prev_hash"` on all blocks at or after `break_at_slot`.

---

## Results — Pallas 0.36.0

| Scenario | Port | Defect | Expected Reaction | Observed — Pallas 0.36.0 |
|---|---|---|---|---|
| `conflicting_forks` | 3027 | Two peers produce identical pre-fork histories, divergent post-fork hashes | Each client accepts its peer's chain; a validating node would detect equivocation if it received both | Client accepted all 7 headers from each peer without error. Pre-fork blocks (slots 100–110) carry identical hashes across both peers confirming shared history; post-fork blocks (slots 115–130) have distinct hashes, confirmed by `defect_kind: "fork_divergence"` on each. Pallas's chain-sync client is passive — it does not cross-check hashes between connections. |
| `sparse_chain` | 3028 | Chain has sequential block_numbers but non-contiguous slot values (105, 115 skipped) | Client accepts — sparse slots are valid in Cardano | Client accepted all 5 headers without error. Slots served: 100, 110, 120, 125, 130 (slots 105 and 115 absent). Block numbers were sequential 0–4. Pallas raises no error for non-contiguous slot sequences; chain-sync does not require consecutive slots. |
| `broken_chain_integrity` | 3029 | `prev_hash` field is wrong from slot 115 onward | Validating client errors at slot 115; non-validating client accepts silently | **Client accepted all 7 headers without error.** Pallas's chain-sync client does not validate the backward hash chain during ingestion. All 7 `chain_sync_roll_forward` events were sent by the server; the chain_sync session completed normally. See "Findings of Interest" below. |

---

## Notes

### Hash-chain validation is the key conformance question

`broken_chain_integrity` is the most diagnostic test in this set. Cardano's chain-sync protocol delivers raw header bytes; whether clients validate the backward hash chain during ingestion (rather than deferring to consensus) is a meaningful implementation choice that affects security under eclipse attacks.

A Pallas client that accepts `broken_chain_integrity` without error is not necessarily wrong — the protocol spec does not require real-time hash-chain validation. But it's a property worth documenting.

### Fork detection is out of scope for chain-sync clients

`conflicting_forks` tests that both chains are servable, not that the client detects the equivocation. A single chain-sync client connecting to one peer cannot detect a fork — it only sees one chain. The scenario is useful for testing that the harness correctly attributes divergent histories to distinct peers, which is infrastructure for future multi-peer equivocation scenarios.

---

## Findings of Interest

### `broken_chain_integrity` — Pallas does not validate hash-chain linkage during chain-sync

Pallas 0.36.0 accepted all 7 headers from a chain whose `prev_hash` was deliberately wrong from slot 115 onward. This is the most significant finding from slice 5:

**Implication.** An eclipse-attacking node can serve an arbitrary chain to a Pallas client during the chain-sync phase. The client will accept blocks whose `prev_hash` values do not chain back to a legitimate genesis, as long as the CBOR is syntactically valid and the chain-sync state machine is followed. Hash-chain validation presumably occurs at a later stage (ledger application, block validation), but this means the chain-sync boundary provides no protection against forged chain content.

**This is not necessarily a bug.** The Cardano chain-sync protocol specification does not require clients to validate hash chains in real time. The finding documents a property of the implementation, not a violation of the spec. A future verifier integrating this harness should distinguish between "wire-protocol conformance" (what this harness tests) and "consensus conformance" (what block validation tests).

**Reproducibility.** Run `cargo test --test live_node broken_chain -- --ignored --nocapture` to see the raw count. As of Pallas 0.36.0 the eprintln output reads: `broken_chain_integrity: Pallas accepted 7 roll_forwards before the chain_sync ended`.

---

### Populating the Observed column for new versions

Run the adversarial integration tests manually:

```
cargo test --test live_node -- --ignored adversarial_production
```

For `broken_chain_integrity`, the test prints how many `roll_forward` messages were accepted before chain-sync ended. Record that value plus `chain_sync_session_summary.exit_reason` if available in the client trace.
