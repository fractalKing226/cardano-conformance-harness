//! Block-Fetch fixture format for server-side block-body replay.
//!
//! JSONL file, anchor first, then one block-body entry per line.
//!
//! ```text
//! {"anchor": true}
//! {"slot":62,"block_hash":"5a3d77...","block_cbor_hex":"820a82..."}
//! {"slot":63,"block_hash":"14051e...","block_cbor_hex":"820a82..."}
//! ```
//!
//! No `variant` field (Block-Fetch bodies are era-agnostic at the protocol
//! level). No `block_number` (not part of the Block-Fetch wire exchange).

use std::io::{BufRead, Write};
use std::path::Path;

use anyhow::Context as _;
use pallas_network::miniprotocols::Point;
use serde::{Deserialize, Serialize};

// ── Types ─────────────────────────────────────────────────────────────────────

/// One block-body record in a Block-Fetch fixture file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockFixtureEntry {
    pub slot: u64,
    pub block_hash: String,  // lowercase hex
    pub block_cbor_hex: String,
}

impl BlockFixtureEntry {
    /// Return the block body as raw bytes.
    pub fn body_bytes(&self) -> Vec<u8> {
        decode_hex_unchecked(&self.block_cbor_hex)
    }

    /// Return the block's `Point`.
    pub fn point(&self) -> Point {
        Point::Specific(self.slot, decode_hex_unchecked(&self.block_hash))
    }
}

/// A loaded Block-Fetch fixture: an anchor point plus an ordered list of blocks.
#[derive(Debug, Clone)]
pub struct BlockFixtureChain {
    pub anchor: Point,
    pub entries: Vec<BlockFixtureEntry>,
}

impl BlockFixtureChain {
    /// Build a `BlockFixtureChain` from a peer's block_store.
    ///
    /// `BTreeMap<(slot, hash), body>` iterates in slot order, which is the order
    /// a Block-Fetch server serves blocks in.
    pub fn from_block_store(store: &std::collections::BTreeMap<(u64, Vec<u8>), Vec<u8>>) -> Self {
        let entries = store
            .iter()
            .map(|((slot, hash), body)| BlockFixtureEntry {
                slot:          *slot,
                block_hash:    encode_hex(hash),
                block_cbor_hex: encode_hex(body),
            })
            .collect();
        Self { anchor: pallas_network::miniprotocols::Point::Origin, entries }
    }

    /// Find all entries whose points fall in `[from, to]` (inclusive, in chain
    /// order). Returns `None` if either endpoint is missing from the fixture or
    /// `to` comes before `from` in chain order.
    ///
    /// Mid-range discontinuities are not detected — the fixture is assumed to
    /// be contiguous between its endpoints. Use explicit `stream_batch` rules
    /// in scenarios that need gappy responses.
    pub fn find_range(&self, from: &Point, to: &Point) -> Option<Vec<&BlockFixtureEntry>> {
        let from_idx = self.find_entry_idx(from)?;
        let to_idx   = self.find_entry_idx(to)?;
        if to_idx < from_idx {
            return None;  // reverse order → unsatisfiable
        }
        Some(self.entries[from_idx..=to_idx].iter().collect())
    }

    fn find_entry_idx(&self, point: &Point) -> Option<usize> {
        match point {
            Point::Origin => None,  // origin is not a block in a BF fixture
            Point::Specific(slot, hash) => self
                .entries
                .iter()
                .position(|e| e.slot == *slot && decode_hex_unchecked(&e.block_hash) == *hash),
        }
    }
}

// ── Load / save ───────────────────────────────────────────────────────────────

/// Load a Block-Fetch fixture from a JSONL file.
pub fn load(path: &Path) -> anyhow::Result<BlockFixtureChain> {
    let file = std::fs::File::open(path)
        .with_context(|| format!("opening block fixture {}", path.display()))?;
    let mut lines = std::io::BufReader::new(file).lines();

    let anchor_line = lines
        .next()
        .ok_or_else(|| anyhow::anyhow!("block fixture is empty: {}", path.display()))?
        .context("reading anchor line")?;
    let anchor = parse_anchor(&anchor_line)
        .with_context(|| format!("parsing anchor in {}", path.display()))?;

    let mut entries = Vec::new();
    for (idx, line) in lines.enumerate() {
        let line = line.with_context(|| format!("reading line {} of block fixture", idx + 2))?;
        if line.trim().is_empty() { continue; }
        let entry: BlockFixtureEntry = serde_json::from_str(&line)
            .with_context(|| format!("parsing line {} of block fixture", idx + 2))?;
        entries.push(entry);
    }

    Ok(BlockFixtureChain { anchor, entries })
}

fn parse_anchor(line: &str) -> anyhow::Result<Point> {
    let v: serde_json::Value = serde_json::from_str(line)?;
    anyhow::ensure!(
        v.get("anchor").and_then(|b| b.as_bool()) == Some(true),
        "first block-fixture line must have anchor: true"
    );
    Ok(Point::Origin)  // Block-Fetch fixtures always anchor at origin
}

/// Write the anchor line, truncating any existing content.
pub fn write_anchor(path: &Path) -> anyhow::Result<()> {
    let mut f = std::fs::OpenOptions::new()
        .create(true).write(true).truncate(true)
        .open(path)
        .with_context(|| format!("creating block fixture {}", path.display()))?;
    writeln!(f, r#"{{"anchor":true}}"#)?;
    Ok(())
}

/// Append one block-body entry.
pub fn append_entry(path: &Path, entry: &BlockFixtureEntry) -> anyhow::Result<()> {
    let mut f = std::fs::OpenOptions::new()
        .create(true).append(true)
        .open(path)
        .with_context(|| format!("appending to block fixture {}", path.display()))?;
    writeln!(f, "{}", serde_json::to_string(entry)?)?;
    Ok(())
}

// ── Hex helpers ───────────────────────────────────────────────────────────────

pub fn encode_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn decode_hex_unchecked(s: &str) -> Vec<u8> {
    (0..s.len()).step_by(2)
        .filter_map(|i| u8::from_str_radix(&s[i..i+2], 16).ok())
        .collect()
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    fn make_chain(n: u64) -> BlockFixtureChain {
        BlockFixtureChain {
            anchor: Point::Origin,
            entries: (0..n).map(|i| BlockFixtureEntry {
                slot: 10 * (i + 1),
                block_hash: format!("{:064x}", i),
                block_cbor_hex: format!("deadbeef{i:02x}"),
            }).collect(),
        }
    }

    #[test]
    fn find_range_exact_endpoints() {
        let chain = make_chain(5);
        let from = chain.entries[1].point();
        let to   = chain.entries[3].point();
        let result = chain.find_range(&from, &to).unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].slot, 20);
        assert_eq!(result[2].slot, 40);
    }

    #[test]
    fn find_range_single_block() {
        let chain = make_chain(3);
        let pt = chain.entries[1].point();
        let result = chain.find_range(&pt, &pt).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].slot, 20);
    }

    #[test]
    fn find_range_missing_endpoint_returns_none() {
        let chain = make_chain(3);
        let good  = chain.entries[0].point();
        let bad   = Point::Specific(99, vec![0xff; 32]);
        assert!(chain.find_range(&good, &bad).is_none());
        assert!(chain.find_range(&bad, &good).is_none());
    }

    #[test]
    fn find_range_reversed_returns_none() {
        let chain = make_chain(3);
        let from = chain.entries[2].point();
        let to   = chain.entries[0].point();
        assert!(chain.find_range(&from, &to).is_none());
    }

    #[test]
    fn fixture_round_trips() {
        let chain = make_chain(3);
        let tmp = NamedTempFile::new().unwrap();
        write_anchor(tmp.path()).unwrap();
        for e in &chain.entries { append_entry(tmp.path(), e).unwrap(); }

        let loaded = load(tmp.path()).unwrap();
        assert_eq!(loaded.entries.len(), 3);
        assert_eq!(loaded.entries[0].slot, 10);
        assert_eq!(loaded.entries[2].slot, 30);
    }

    #[test]
    fn load_anchor_only() {
        let tmp = NamedTempFile::new().unwrap();
        write_anchor(tmp.path()).unwrap();
        let chain = load(tmp.path()).unwrap();
        assert_eq!(chain.entries.len(), 0);
    }
}
