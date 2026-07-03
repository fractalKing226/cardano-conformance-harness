//! Runtime state for a declared imaginary-network peer.
//!
//! Each peer in the scenario's `network` block gains a `PeerState` at scenario
//! start, initialized from its declared fixtures. Serve steps and
//! `peer_extends_chain` steps mutate or read this state rather than touching
//! the fixture files directly.

use std::collections::{BTreeMap, HashMap};

use pallas_network::miniprotocols::Point;

use crate::scenario::block_fixture::BlockFixtureChain;
use crate::scenario::fixture::{self, FixtureChain, FixtureEntry, DEFAULT_HEADER_VARIANT};
use crate::scenario::ProductionRule;

// ── ChainEntry ─────────────────────────────────────────────────────────────────

/// One block header in a peer's chain — the in-memory form of a `FixtureEntry`.
///
/// Uses raw bytes rather than hex strings for efficient in-memory manipulation.
/// Conversion to/from `FixtureEntry` (which uses hex) happens at the fixture-file
/// and serve-step boundaries.
#[derive(Debug, Clone)]
pub struct ChainEntry {
    pub slot:         u64,
    pub block_hash:   Vec<u8>,   // 32-byte blake2b hash
    pub block_number: u64,
    pub header_cbor:  Vec<u8>,   // era-specific header bytes
    pub variant:      u8,
}

impl ChainEntry {
    pub fn from_fixture_entry(e: &FixtureEntry) -> Self {
        Self {
            slot:         e.slot,
            block_hash:   decode_hex_unchecked(&e.block_hash),
            block_number: e.block_number,
            header_cbor:  decode_hex_unchecked(&e.cbor_hex),
            variant:      e.variant,
        }
    }
}

// ── PeerState ─────────────────────────────────────────────────────────────────

/// Runtime state for one declared peer.
///
/// `chain_entries` is the peer's current chain in chronological order.
/// `block_store` maps `(slot, hash)` → raw block body bytes; populated from
/// `block_fetch_fixture` at init and by `peer_extends_chain`/production at runtime.
/// `BTreeMap` is used instead of `HashMap` so iteration is slot-ordered, which
/// is required for `BlockFixtureChain` conversion.
#[derive(Debug, Default)]
pub struct PeerState {
    pub chain_entries:   Vec<ChainEntry>,
    pub block_store:     BTreeMap<(u64, Vec<u8>), Vec<u8>>,
    /// Production rule for this peer, copied from the Peer declaration at init.
    /// `None` or `Some(ProductionRule::None)` means no automatic production.
    pub production_rule: Option<ProductionRule>,
}

// ── Automatic production ──────────────────────────────────────────────────────

/// Result of evaluating a production rule for one (peer, slot) pair.
/// Emitted as `production_rule_fired` in the trace regardless of `skipped`.
pub struct ProductionEvent {
    pub peer_id:       String,
    pub slot:          u64,
    pub rule_kind:     &'static str,
    /// `true` when the peer's chain tip already covers this slot (an explicit
    /// `peer_extends_chain` step ran first). The rule fired but produced nothing.
    pub skipped:       bool,
    /// Populated only when `!skipped`.
    pub block_number:  u64,
    pub block_hash_hex: String,
    /// Describes the specific adversarial defect this block carries, if any.
    /// `None` for honest production; present for adversarial rule variants.
    /// Emitted on `peer_chain_extended` events to help verifiers identify
    /// adversarial blocks without decoding rule parameters.
    pub defect_kind:   Option<&'static str>,
}

/// Internal per-rule context carried from the immutable phase-1 scan into
/// phase-2 block production. Captures only what's needed to modify `synthetic_hash`
/// arguments — keeps phase-2 free of borrow conflicts.
#[derive(Clone)]
enum RuleContext {
    Honest,
    Forked   {
        fork_slot:      u64,
        fork_marker:    String,
        /// Canonical identifier for pre-fork hash computation — derived from the
        /// rule's timing params so two ForkedFromSlot rules with the same
        /// first_slot/interval/fork_slot produce identical pre-fork chains.
        canonical_root: String,
    },
    Broken   { break_at_slot: u64, wrong_hash_bytes: Vec<u8> },
    Sparse,  // SkipsSlots: blocks are honest individually; defect_kind="sparse_chain"
}

/// Evaluate every peer's production rule for the slot range `(old_slot, new_slot]`
/// and append blocks to chains that fire. Returns one `ProductionEvent` per
/// (peer, slot) evaluation (skipped or not) for trace emission.
///
/// Peer order within the HashMap is arbitrary; all peers are evaluated for each
/// advancing slot independently.
pub fn apply_production_rules(
    peers: &mut HashMap<String, PeerState>,
    old_slot: u64,
    new_slot: u64,
) -> Vec<ProductionEvent> {
    // Phase 1: immutable scan — collect firing slots and rule context per peer.
    let tasks: Vec<(String, &'static str, Vec<u64>, RuleContext)> = peers
        .iter()
        .filter_map(|(id, ps)| {
            let rule = ps.production_rule.as_ref()?;
            if matches!(rule, ProductionRule::None) { return None; }
            let slots = rule.slots_in_range(old_slot + 1, new_slot);
            if slots.is_empty() { return None; }
            let ctx = match rule {
                ProductionRule::ForkedFromSlot { first_slot, interval, fork_slot, fork_marker } => {
                    // Canonical root encodes the timing params so two peers with the
                    // same rule shape (but different fork_markers) produce identical
                    // pre-fork chains — enabling the diverging-forks test pattern.
                    let canonical_root = format!("{first_slot}:{interval}:{fork_slot}");
                    RuleContext::Forked { fork_slot: *fork_slot, fork_marker: fork_marker.clone(), canonical_root }
                }
                ProductionRule::BrokenPrevHash { break_at_slot, wrong_hash, .. } => {
                    // decode_hex is infallible here — validation guarantees valid hex.
                    let bytes = decode_hex(wrong_hash).unwrap_or_else(|_| vec![0u8; 32]);
                    RuleContext::Broken { break_at_slot: *break_at_slot, wrong_hash_bytes: bytes }
                }
                ProductionRule::SkipsSlots { .. } => RuleContext::Sparse,
                _ => RuleContext::Honest,
            };
            Some((id.clone(), rule.kind_str(), slots, ctx))
        })
        .collect();

    // Phase 2: mutable — append entries to each peer's chain.
    let mut events = Vec::new();
    for (peer_id, rule_kind, slots, ctx) in tasks {
        let ps = peers.get_mut(&peer_id).expect("peer present from phase 1");
        for slot in slots {
            // Skip if the chain tip already covers this slot (explicit extension took priority).
            let skipped = ps.chain_tip_slot().map_or(false, |tip| tip >= slot);
            let (block_number, block_hash_hex, defect_kind) = if skipped {
                (0, String::new(), None)
            } else {
                let actual_prev = ps.chain_entries.last()
                    .map(|e| e.block_hash.clone())
                    .unwrap_or_else(|| vec![0u8; 32]);
                let bn = ps.chain_entries.last().map(|e| e.block_number + 1).unwrap_or(0);

                // Rule context determines hash inputs and defect attribution.
                let (hash_peer_id, hash_prev, defect) = match &ctx {
                    // Pre-fork: use the canonical_root so two peers with the same
                    // timing params produce identical chains before the fork point.
                    RuleContext::Forked { fork_slot, canonical_root, .. } if slot < *fork_slot => {
                        (canonical_root.clone(), actual_prev, None)
                    }
                    // Post-fork: fork_marker distinguishes chains; null-byte prevents
                    // "foo"+"bar" == "fo"+"obar" collisions with canonical_root.
                    RuleContext::Forked { fork_marker, .. } => {
                        (format!("\x00{fork_marker}"), actual_prev, Some("fork_divergence"))
                    }
                    RuleContext::Broken { break_at_slot, wrong_hash_bytes } if slot >= *break_at_slot => {
                        (peer_id.clone(), wrong_hash_bytes.clone(), Some("broken_prev_hash"))
                    }
                    RuleContext::Sparse => {
                        (peer_id.clone(), actual_prev, Some("sparse_chain"))
                    }
                    _ => (peer_id.clone(), actual_prev, None),
                };

                let hash = synthetic_hash(&hash_peer_id, slot, &hash_prev);
                let hash_hex = fixture::encode_hex(&hash);
                let header_cbor = synthetic_header_cbor(bn, slot);
                ps.chain_entries.push(ChainEntry {
                    slot,
                    block_hash:   hash.clone(),
                    block_number: bn,
                    header_cbor,
                    variant: DEFAULT_HEADER_VARIANT,
                });
                ps.block_store.insert((slot, hash.clone()), vec![0xa0]);
                (bn, hash_hex, defect)
            };
            events.push(ProductionEvent {
                peer_id: peer_id.clone(), slot, rule_kind, skipped,
                block_number, block_hash_hex, defect_kind,
            });
        }
    }
    events
}

// ── Synthetic block content ───────────────────────────────────────────────────

/// Deterministic 32-byte block hash: Blake2b-256 of `peer_id || slot_be || prev_hash`.
fn synthetic_hash(peer_id: &str, slot: u64, prev_hash: &[u8]) -> Vec<u8> {
    let mut input = Vec::new();
    input.extend_from_slice(peer_id.as_bytes());
    input.extend_from_slice(&slot.to_be_bytes());
    input.extend_from_slice(prev_hash);
    blake2b_simd::Params::new().hash_length(32).hash(&input).as_bytes().to_vec()
}

/// Minimal valid Conway header CBOR for wire consumption.
///
/// Shape: `array(2)[ array(2)[block_number, slot], null ]`
///
/// This matches exactly what `extract_header_fields` in `chainsync.rs` expects:
/// outer array, inner array with block_number first then slot, then anything
/// for the signature position. Real VRF proofs and KES signatures are slice 6+.
fn synthetic_header_cbor(block_number: u64, slot: u64) -> Vec<u8> {
    let mut cbor = vec![0x82u8, 0x82]; // array(2) outer, array(2) inner
    cbor.extend_from_slice(&cbor_uint(block_number));
    cbor.extend_from_slice(&cbor_uint(slot));
    cbor.push(0xf6); // null — signature placeholder
    cbor
}

/// Minimal CBOR unsigned integer encoding (same logic as live_node.rs tests).
fn cbor_uint(v: u64) -> Vec<u8> {
    if v <= 23           { vec![v as u8] }
    else if v <= 0xFF    { vec![0x18, v as u8] }
    else if v <= 0xFFFF  { vec![0x19, (v >> 8) as u8, (v & 0xFF) as u8] }
    else if v <= 0xFFFF_FFFF {
        vec![0x1a, (v >> 24) as u8, ((v >> 16) & 0xFF) as u8,
             ((v >> 8) & 0xFF) as u8, (v & 0xFF) as u8]
    } else {
        vec![0x1b, (v >> 56) as u8, ((v >> 48) & 0xFF) as u8, ((v >> 40) & 0xFF) as u8,
             ((v >> 32) & 0xFF) as u8, ((v >> 24) & 0xFF) as u8, ((v >> 16) & 0xFF) as u8,
             ((v >> 8) & 0xFF) as u8, (v & 0xFF) as u8]
    }
}

impl PeerState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Slot of the last chain entry, or `None` if the chain is empty.
    pub fn chain_tip_slot(&self) -> Option<u64> {
        self.chain_entries.last().map(|e| e.slot)
    }

    /// Build a `FixtureChain` from entries with `slot <= current_slot`.
    /// When `current_slot` is `None`, all entries are included.
    ///
    /// # Hex round-trip note
    /// `ChainEntry` stores raw bytes; `FixtureEntry` requires hex strings. This
    /// method re-encodes bytes → hex on every serve step. The inefficiency is
    /// acceptable for this slice — a future refactor could change `FixtureChain`
    /// to store raw bytes internally and defer hex conversion to serialization
    /// boundaries (fixture file read/write only).
    pub fn filtered_fixture_chain(&self, current_slot: Option<u64>) -> FixtureChain {
        let entries: Vec<FixtureEntry> = self.chain_entries.iter()
            .filter(|e| current_slot.map_or(true, |s| e.slot <= s))
            .map(|e| FixtureEntry {
                slot:         e.slot,
                block_hash:   fixture::encode_hex(&e.block_hash),
                block_number: e.block_number,
                cbor_hex:     fixture::encode_hex(&e.header_cbor),
                variant:      e.variant,
            })
            .collect();
        FixtureChain { anchor: Point::Origin, entries }
    }

    /// Build a `BlockFixtureChain` from the peer's block_store for use in
    /// `serve_block_fetch` range resolution.
    pub fn to_block_fixture_chain(&self) -> BlockFixtureChain {
        BlockFixtureChain::from_block_store(&self.block_store)
    }

    /// Initialize chain_entries from a loaded `FixtureChain`.
    pub fn from_fixture_chain(chain: &FixtureChain) -> Self {
        Self {
            chain_entries:   chain.entries.iter().map(ChainEntry::from_fixture_entry).collect(),
            block_store:     BTreeMap::new(),
            production_rule: None, // set by the runner after construction
        }
    }

    /// Add block bodies from a `BlockFixtureChain` into the block_store.
    pub fn extend_from_block_fixture_chain(&mut self, chain: &BlockFixtureChain) {
        for entry in &chain.entries {
            let hash = decode_hex_unchecked(&entry.block_hash);
            let body = entry.body_bytes();
            self.block_store.insert((entry.slot, hash), body);
        }
    }
}

// ── Hex helpers ───────────────────────────────────────────────────────────────

/// Decode hex to bytes, returning an empty vec on any error.
/// Only used for fixture data that has already been validated on load.
fn decode_hex_unchecked(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .filter_map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok())
        .collect()
}

/// Decode hex to bytes with proper error reporting. Used for user-supplied hex
/// parameters in `peer_extends_chain` steps.
pub fn decode_hex(s: &str) -> anyhow::Result<Vec<u8>> {
    anyhow::ensure!(s.len() % 2 == 0, "odd-length hex string");
    (0..s.len())
        .step_by(2)
        .map(|i| {
            u8::from_str_radix(&s[i..i + 2], 16)
                .map_err(|e| anyhow::anyhow!("invalid hex byte at offset {i}: {e}"))
        })
        .collect()
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    // ── ProductionRule.slots_in_range ─────────────────────────────────────────

    #[test]
    fn every_n_slots_fires_in_range() {
        let rule = ProductionRule::EveryNSlots { first_slot: 100, interval: 5 };
        // (99, 130] → 100, 105, 110, 115, 120, 125, 130 = 7
        assert_eq!(rule.slots_in_range(100, 130), vec![100, 105, 110, 115, 120, 125, 130]);
        // (100, 130] → 105, 110, 115, 120, 125, 130 = 6
        assert_eq!(rule.slots_in_range(101, 130), vec![105, 110, 115, 120, 125, 130]);
        // (130, 130] = empty
        assert_eq!(rule.slots_in_range(131, 130), Vec::<u64>::new());
        // first_slot > to → empty
        assert_eq!(rule.slots_in_range(50, 99), Vec::<u64>::new());
    }

    #[test]
    fn at_slots_fires_correctly() {
        let rule = ProductionRule::AtSlots { slots: vec![102, 107, 117] };
        assert_eq!(rule.slots_in_range(100, 120), vec![102, 107, 117]);
        assert_eq!(rule.slots_in_range(103, 120), vec![107, 117]);
        assert_eq!(rule.slots_in_range(118, 200), Vec::<u64>::new());
    }

    #[test]
    fn none_rule_never_fires() {
        assert_eq!(ProductionRule::None.slots_in_range(0, u64::MAX), Vec::<u64>::new());
    }

    // ── apply_production_rules ────────────────────────────────────────────────

    fn peer_with_rule(rule: ProductionRule) -> PeerState {
        PeerState { production_rule: Some(rule), ..Default::default() }
    }

    #[test]
    fn apply_production_extends_chain_sequentially() {
        let mut peers: HashMap<String, PeerState> = HashMap::new();
        peers.insert("p".into(), peer_with_rule(
            ProductionRule::EveryNSlots { first_slot: 10, interval: 10 }
        ));

        let events = apply_production_rules(&mut peers, 0, 30);
        // Fires at 10, 20, 30 = 3 blocks
        assert_eq!(events.len(), 3);
        assert!(events.iter().all(|e| !e.skipped));
        assert_eq!(events[0].slot, 10);
        assert_eq!(events[1].slot, 20);
        assert_eq!(events[2].slot, 30);

        let ps = peers.get("p").unwrap();
        assert_eq!(ps.chain_entries.len(), 3);
        assert_eq!(ps.chain_entries[0].block_number, 0);
        assert_eq!(ps.chain_entries[1].block_number, 1);
        assert_eq!(ps.chain_entries[2].block_number, 2);
    }

    #[test]
    fn apply_production_skips_when_tip_already_covers_slot() {
        let mut ps = peer_with_rule(ProductionRule::EveryNSlots { first_slot: 10, interval: 10 });
        // Pre-extend chain to slot 10 explicitly (simulates peer_extends_chain)
        ps.chain_entries.push(ChainEntry {
            slot: 10, block_hash: vec![0; 32], block_number: 0,
            header_cbor: vec![0x82, 0x82, 0x00, 0x0a, 0xf6], variant: 6,
        });
        let mut peers = HashMap::new();
        peers.insert("p".into(), ps);

        let events = apply_production_rules(&mut peers, 0, 20);
        // Slot 10: skipped (tip=10 >= 10); Slot 20: produced
        assert_eq!(events.len(), 2);
        assert!(events[0].skipped);
        assert_eq!(events[0].slot, 10);
        assert!(!events[1].skipped);
        assert_eq!(events[1].slot, 20);
        // Chain should have 2 entries: the pre-existing + the produced one at 20
        assert_eq!(peers.get("p").unwrap().chain_entries.len(), 2);
    }

    #[test]
    fn apply_production_fires_at_new_slot_inclusive() {
        // The range is (old_slot, new_slot], so new_slot itself must fire.
        let mut peers: HashMap<String, PeerState> = HashMap::new();
        peers.insert("p".into(), peer_with_rule(
            ProductionRule::EveryNSlots { first_slot: 50, interval: 50 }
        ));
        let events = apply_production_rules(&mut peers, 0, 50);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].slot, 50);
        assert!(!events[0].skipped);
    }

    #[test]
    fn block_number_increments_after_initial_fixture_entries() {
        let mut ps = peer_with_rule(ProductionRule::EveryNSlots { first_slot: 200, interval: 100 });
        // Simulate 3 fixture-loaded entries
        for (i, slot) in [10u64, 20, 30].iter().enumerate() {
            ps.chain_entries.push(ChainEntry {
                slot: *slot, block_hash: vec![i as u8; 32], block_number: i as u64,
                header_cbor: vec![0x82, 0x82, i as u8, *slot as u8, 0xf6], variant: 6,
            });
        }
        let mut peers = HashMap::new();
        peers.insert("p".into(), ps);

        let events = apply_production_rules(&mut peers, 100, 200);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].block_number, 3); // continues from bn=2
    }

    fn make_entry(slot: u64, block_number: u64) -> ChainEntry {
        ChainEntry {
            slot,
            block_hash:   vec![slot as u8; 32],
            block_number,
            header_cbor:  vec![0xaa, 0xbb],
            variant:      6,
        }
    }

    #[test]
    fn new_peer_state_is_empty() {
        let ps = PeerState::new();
        assert!(ps.chain_entries.is_empty());
        assert!(ps.block_store.is_empty());
        assert_eq!(ps.chain_tip_slot(), None);
    }

    #[test]
    fn filtered_fixture_chain_respects_slot_bound() {
        let mut ps = PeerState::new();
        ps.chain_entries.push(make_entry(10, 1));
        ps.chain_entries.push(make_entry(20, 2));
        ps.chain_entries.push(make_entry(30, 3));

        // No filter — all three visible.
        assert_eq!(ps.filtered_fixture_chain(None).entries.len(), 3);

        // current_slot = 20 → entries at 10 and 20 visible, 30 hidden.
        let chain = ps.filtered_fixture_chain(Some(20));
        assert_eq!(chain.entries.len(), 2);
        assert_eq!(chain.entries[1].slot, 20);

        // current_slot = 5 → none visible.
        assert_eq!(ps.filtered_fixture_chain(Some(5)).entries.len(), 0);
    }

    #[test]
    fn chain_tip_slot_returns_last_entry() {
        let mut ps = PeerState::new();
        ps.chain_entries.push(make_entry(10, 1));
        ps.chain_entries.push(make_entry(20, 2));
        assert_eq!(ps.chain_tip_slot(), Some(20));
    }

    // ── Adversarial rule tests ────────────────────────────────────────────────

    #[test]
    fn forked_slots_match_every_n_slots() {
        let forked = ProductionRule::ForkedFromSlot {
            first_slot: 100, interval: 5, fork_slot: 115, fork_marker: "x".into()
        };
        let honest = ProductionRule::EveryNSlots { first_slot: 100, interval: 5 };
        assert_eq!(forked.slots_in_range(100, 130), honest.slots_in_range(100, 130),
            "ForkedFromSlot fires at the same slots as EveryNSlots");
    }

    #[test]
    fn pre_fork_hashes_match_between_same_timing_different_markers() {
        // Two ForkedFromSlot peers with identical first_slot/interval/fork_slot but
        // different fork_markers must produce identical pre-fork chains. This is the
        // invariant that makes the conflicting-forks scenario meaningful: both chains
        // share the same history up to fork_slot and diverge afterward.
        let mut peers: HashMap<String, PeerState> = HashMap::new();
        peers.insert("peer_a".into(), PeerState {
            production_rule: Some(ProductionRule::ForkedFromSlot {
                first_slot: 100, interval: 10, fork_slot: 130, fork_marker: "chain_a".into()
            }), ..Default::default()
        });
        peers.insert("peer_b".into(), PeerState {
            production_rule: Some(ProductionRule::ForkedFromSlot {
                first_slot: 100, interval: 10, fork_slot: 130, fork_marker: "chain_b".into()
            }), ..Default::default()
        });
        apply_production_rules(&mut peers, 99, 120);
        let a_hashes: Vec<_> = peers["peer_a"].chain_entries.iter().map(|e| e.block_hash.clone()).collect();
        let b_hashes: Vec<_> = peers["peer_b"].chain_entries.iter().map(|e| e.block_hash.clone()).collect();
        assert_eq!(a_hashes, b_hashes, "pre-fork hashes must be identical across peers with same timing");
    }

    #[test]
    fn post_fork_hashes_differ_with_different_markers() {
        let mut make_peers = |marker: &str| {
            let mut peers: HashMap<String, PeerState> = HashMap::new();
            peers.insert("p".into(), PeerState {
                production_rule: Some(ProductionRule::ForkedFromSlot {
                    first_slot: 100, interval: 10, fork_slot: 110, fork_marker: marker.into()
                }), ..Default::default()
            });
            apply_production_rules(&mut peers, 99, 130);
            peers
        };
        let chain_a = make_peers("A");
        let chain_b = make_peers("B");
        // Slot 100: pre-fork, hashes must match
        assert_eq!(chain_a["p"].chain_entries[0].block_hash, chain_b["p"].chain_entries[0].block_hash,
            "pre-fork hash must be identical");
        // Slot 110+: post-fork, hashes must differ
        assert_ne!(chain_a["p"].chain_entries[1].block_hash, chain_b["p"].chain_entries[1].block_hash,
            "post-fork hashes must diverge with different markers");
    }

    #[test]
    fn skips_slots_omits_specified_indices() {
        let mut peers: HashMap<String, PeerState> = HashMap::new();
        peers.insert("p".into(), PeerState {
            production_rule: Some(ProductionRule::SkipsSlots {
                first_slot: 100, interval: 5, skip_indices: vec![1, 3]
                // skips slot 105 (idx=1) and slot 115 (idx=3)
            }), ..Default::default()
        });
        apply_production_rules(&mut peers, 99, 130);
        let slots: Vec<u64> = peers["p"].chain_entries.iter().map(|e| e.slot).collect();
        assert!(!slots.contains(&105), "slot 105 (idx=1) must be skipped");
        assert!(!slots.contains(&115), "slot 115 (idx=3) must be skipped");
        assert!(slots.contains(&100), "slot 100 (idx=0) must be present");
        assert!(slots.contains(&110), "slot 110 (idx=2) must be present");
    }

    #[test]
    fn skips_slots_block_numbers_are_sequential() {
        let mut peers: HashMap<String, PeerState> = HashMap::new();
        peers.insert("p".into(), PeerState {
            production_rule: Some(ProductionRule::SkipsSlots {
                first_slot: 100, interval: 5, skip_indices: vec![1, 3]
            }), ..Default::default()
        });
        apply_production_rules(&mut peers, 99, 130);
        let bns: Vec<u64> = peers["p"].chain_entries.iter().map(|e| e.block_number).collect();
        let expected: Vec<u64> = (0..bns.len() as u64).collect();
        assert_eq!(bns, expected, "block_numbers must be sequential despite slot gaps");
    }

    #[test]
    fn broken_prev_hash_correct_before_break() {
        let mut peers: HashMap<String, PeerState> = HashMap::new();
        let wrong = "abcd".repeat(16); // 64 chars
        peers.insert("p".into(), PeerState {
            production_rule: Some(ProductionRule::BrokenPrevHash {
                first_slot: 100, interval: 10, break_at_slot: 120, wrong_hash: wrong.clone()
            }), ..Default::default()
        });
        apply_production_rules(&mut peers, 99, 130);
        let entries = &peers["p"].chain_entries;
        // Block at slot 100: prev = zeros (empty chain), hash = correct synthetic
        let expected_hash_100 = synthetic_hash("p", 100, &vec![0u8; 32]);
        assert_eq!(entries[0].block_hash, expected_hash_100, "pre-break hash must be correct");
    }

    #[test]
    fn broken_prev_hash_wrong_after_break() {
        let mut peers: HashMap<String, PeerState> = HashMap::new();
        let wrong = "abcd".repeat(16); // 64 chars hex
        peers.insert("p".into(), PeerState {
            production_rule: Some(ProductionRule::BrokenPrevHash {
                first_slot: 100, interval: 10, break_at_slot: 110, wrong_hash: wrong.clone()
            }), ..Default::default()
        });
        apply_production_rules(&mut peers, 99, 130);
        let entries = &peers["p"].chain_entries;
        // Block at slot 110 (break_at_slot): prev should be wrong_hash bytes
        let wrong_bytes = decode_hex(&wrong).unwrap();
        let expected_hash_110 = synthetic_hash("p", 110, &wrong_bytes);
        assert_eq!(entries[1].block_hash, expected_hash_110,
            "post-break hash must use the wrong_hash as prev");
    }

    #[test]
    fn defect_kind_populated_for_adversarial_rules() {
        let mut peers: HashMap<String, PeerState> = HashMap::new();
        peers.insert("forked".into(), PeerState {
            production_rule: Some(ProductionRule::ForkedFromSlot {
                first_slot: 100, interval: 10, fork_slot: 110, fork_marker: "f".into()
            }), ..Default::default()
        });
        peers.insert("sparse".into(), PeerState {
            production_rule: Some(ProductionRule::SkipsSlots {
                first_slot: 100, interval: 10, skip_indices: vec![1]
            }), ..Default::default()
        });
        let events = apply_production_rules(&mut peers, 99, 120);
        let forked_events: Vec<_> = events.iter().filter(|e| e.peer_id == "forked").collect();
        // Slot 100 (pre-fork): no defect
        assert_eq!(forked_events[0].defect_kind, None);
        // Slot 110 (at fork): defect_kind = "fork_divergence"
        assert_eq!(forked_events[1].defect_kind, Some("fork_divergence"));
        // SkipsSlots produced block: defect_kind = "sparse_chain"
        let sparse_events: Vec<_> = events.iter().filter(|e| e.peer_id == "sparse" && !e.skipped).collect();
        assert!(sparse_events.iter().all(|e| e.defect_kind == Some("sparse_chain")));
    }

    #[test]
    fn decode_hex_rejects_odd_length() {
        assert!(decode_hex("abc").is_err());
    }

    #[test]
    fn decode_hex_round_trips() {
        let bytes = vec![0xde, 0xad, 0xbe, 0xef];
        let hex = fixture::encode_hex(&bytes);
        assert_eq!(decode_hex(&hex).unwrap(), bytes);
    }
}
