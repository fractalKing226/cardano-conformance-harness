//! Runtime state for a declared imaginary-network peer.
//!
//! Each peer in the scenario's `network` block gains a `PeerState` at scenario
//! start, initialized from its declared fixtures. Serve steps and
//! `peer_extends_chain` steps mutate or read this state rather than touching
//! the fixture files directly.

use std::collections::BTreeMap;

use pallas_network::miniprotocols::Point;

use crate::scenario::block_fixture::BlockFixtureChain;
use crate::scenario::fixture::{self, FixtureChain, FixtureEntry};

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
/// `block_fetch_fixture` at init and by `peer_extends_chain` steps at runtime.
/// `BTreeMap` is used instead of `HashMap` so iteration is slot-ordered, which
/// is required for `BlockFixtureChain` conversion.
#[derive(Debug, Default)]
pub struct PeerState {
    pub chain_entries: Vec<ChainEntry>,
    pub block_store:   BTreeMap<(u64, Vec<u8>), Vec<u8>>,
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
            chain_entries: chain.entries.iter().map(ChainEntry::from_fixture_entry).collect(),
            block_store:   BTreeMap::new(),
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
