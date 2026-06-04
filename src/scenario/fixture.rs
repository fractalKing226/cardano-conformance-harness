//! Chain fixture format for the server-side Chain-Sync handler.
//!
//! A fixture is a JSONL file: one JSON object per line. The first line is the
//! **anchor** (`{"anchor": true}` for genesis, or a slot+hash for mid-chain
//! segments). Remaining lines are block headers in chain order.
//!
//! Example:
//! ```text
//! {"anchor": true}
//! {"slot":62,"block_hash":"5a3d77...","block_number":1,"cbor_hex":"828a00..."}
//! {"slot":63,"block_hash":"14051e...","block_number":2,"cbor_hex":"828a01..."}
//! ```

use std::io::{BufRead, Write};
use std::path::Path;

use anyhow::Context as _;
use pallas_network::miniprotocols::chainsync::Tip;
use pallas_network::miniprotocols::Point;
use serde::{Deserialize, Serialize};

// ── Types ─────────────────────────────────────────────────────────────────────

/// Era variant byte used when no `variant` field is present in a legacy fixture.
/// Update this constant when Cardano introduces a new era.
pub const DEFAULT_HEADER_VARIANT: u8 = 6; // Conway

fn default_header_variant() -> u8 {
    DEFAULT_HEADER_VARIANT
}

/// One captured block header — the wire-level record in a fixture file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FixtureEntry {
    pub slot: u64,
    pub block_hash: String,  // lowercase hex, 64 chars for a 32-byte hash
    pub block_number: u64,
    pub cbor_hex: String,
    /// Era variant byte (0=Byron, 6=Conway). Defaults to `DEFAULT_HEADER_VARIANT`
    /// when loading older fixtures that predate this field.
    #[serde(default = "default_header_variant")]
    pub variant: u8,
}

/// Internal representation of one fixture line during load/save.
#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
#[allow(dead_code)]
enum FixtureLine {
    Anchor { anchor: bool },
    Block(FixtureEntry),
}

/// A loaded fixture chain: an anchor point plus an ordered sequence of headers.
#[derive(Debug, Clone)]
pub struct FixtureChain {
    /// Point before the first header (typically `Point::Origin`).
    pub anchor: Point,
    /// Headers in chain order.
    pub entries: Vec<FixtureEntry>,
}

impl FixtureChain {
    /// The chain tip (last entry as a Point + block_number pair).
    pub fn tip(&self) -> Tip {
        match self.entries.last() {
            None => Tip(self.anchor.clone(), 0),
            Some(e) => Tip(
                Point::Specific(e.slot, decode_hex_unchecked(&e.block_hash)),
                e.block_number,
            ),
        }
    }
}

// ── Load / save ───────────────────────────────────────────────────────────────

/// Load a fixture from a JSONL file.
pub fn load(path: &Path) -> anyhow::Result<FixtureChain> {
    let file = std::fs::File::open(path)
        .with_context(|| format!("opening fixture file {}", path.display()))?;
    let reader = std::io::BufReader::new(file);
    let mut lines = reader.lines();

    // First line must be the anchor.
    let anchor_line = lines
        .next()
        .ok_or_else(|| anyhow::anyhow!("fixture file is empty: {}", path.display()))?
        .context("reading anchor line")?;
    let anchor = parse_anchor(&anchor_line)
        .with_context(|| format!("parsing anchor in {}", path.display()))?;

    let mut entries = Vec::new();
    for (idx, line) in lines.enumerate() {
        let line = line.with_context(|| format!("reading line {} of fixture", idx + 2))?;
        if line.trim().is_empty() {
            continue;
        }
        let entry: FixtureEntry = serde_json::from_str(&line)
            .with_context(|| format!("parsing line {} of fixture", idx + 2))?;
        entries.push(entry);
    }

    Ok(FixtureChain { anchor, entries })
}

fn parse_anchor(line: &str) -> anyhow::Result<Point> {
    let v: serde_json::Value = serde_json::from_str(line)?;
    if v.get("anchor").and_then(|b| b.as_bool()) == Some(true) {
        // Check for optional slot/hash for non-genesis anchors
        match (v.get("slot"), v.get("block_hash")) {
            (Some(s), Some(h)) if !h.as_str().unwrap_or("").is_empty() => {
                let slot = s.as_u64().context("anchor slot must be u64")?;
                let hash = decode_hex(h.as_str().context("anchor block_hash must be string")?)
                    .context("anchor block_hash hex")?;
                Ok(Point::Specific(slot, hash))
            }
            _ => Ok(Point::Origin),
        }
    } else {
        anyhow::bail!("first fixture line must have anchor: true")
    }
}

/// Write the anchor line to a new fixture file, truncating any existing content.
pub fn write_anchor(path: &Path, anchor: &Point) -> anyhow::Result<()> {
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)
        .with_context(|| format!("creating fixture file {}", path.display()))?;
    match anchor {
        Point::Origin => writeln!(f, r#"{{"anchor":true}}"#)?,
        Point::Specific(slot, hash) => {
            let line = serde_json::json!({
                "anchor": true,
                "slot": slot,
                "block_hash": encode_hex(hash),
            });
            writeln!(f, "{}", serde_json::to_string(&line)?)?;
        }
    }
    Ok(())
}

/// Append one header entry to a fixture file.
pub fn append_entry(path: &Path, entry: &FixtureEntry) -> anyhow::Result<()> {
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("opening fixture file for append: {}", path.display()))?;
    writeln!(f, "{}", serde_json::to_string(entry)?)?;
    Ok(())
}

// ── Cursor ────────────────────────────────────────────────────────────────────

/// A traversal cursor over a `FixtureChain`.
///
/// Position semantics:
/// - `pos = -1`: before the anchor (initial state, nothing served yet)
/// - `pos = 0`: at the anchor
/// - `pos = k (k >= 1)`: at `entries[k-1]`
pub struct Cursor<'a> {
    chain: &'a FixtureChain,
    /// Current position. -1 means "before start"; 0..=entries.len() means "at entry".
    pos: isize,
}

impl<'a> Cursor<'a> {
    /// Create a cursor positioned before the anchor.
    pub fn new(chain: &'a FixtureChain) -> Self {
        Self { chain, pos: -1 }
    }

    /// Current tip of the fixture (the last block, or the anchor if empty).
    pub fn tip(&self) -> Tip {
        self.chain.tip()
    }

    /// Find the best intersection for the given client points.
    ///
    /// Returns the position of the **first** candidate that exists in the
    /// fixture, preserving the order the client supplied. Chain-Sync requires
    /// this: the client encodes priority through ordering (most-recent-first in
    /// practice), and the server must respect that ordering rather than
    /// independently choosing the highest slot it knows about.
    pub fn find_intersect(&self, points: &[Point]) -> Option<isize> {
        points.iter().find_map(|p| self.point_to_pos(p))
    }

    fn point_to_pos(&self, point: &Point) -> Option<isize> {
        match point {
            Point::Origin => {
                if matches!(self.chain.anchor, Point::Origin) {
                    Some(0)
                } else {
                    None
                }
            }
            Point::Specific(slot, hash) => {
                // Check anchor
                if let Point::Specific(a_slot, ref a_hash) = self.chain.anchor {
                    if *slot == a_slot && *hash == *a_hash {
                        return Some(0);
                    }
                }
                // Check blocks
                for (i, e) in self.chain.entries.iter().enumerate() {
                    if *slot == e.slot && *hash == decode_hex_unchecked(&e.block_hash) {
                        return Some((i + 1) as isize);
                    }
                }
                None
            }
        }
    }

    /// Move the cursor to `new_pos` (result of `find_intersect`).
    pub fn set_pos(&mut self, new_pos: isize) {
        self.pos = new_pos;
    }

    /// Point at the current cursor position.
    pub fn current_point(&self) -> Point {
        match self.pos {
            p if p <= 0 => self.chain.anchor.clone(),
            p => {
                let e = &self.chain.entries[(p - 1) as usize];
                Point::Specific(e.slot, decode_hex_unchecked(&e.block_hash))
            }
        }
    }

    /// Advance by one and return the new entry, or `None` if already at tip.
    pub fn advance(&mut self) -> Option<&FixtureEntry> {
        let next = self.pos + 1;
        if next as usize > self.chain.entries.len() {
            None // already at tip
        } else {
            self.pos = next;
            if self.pos == 0 {
                // moved to anchor — no header to return
                None
            } else {
                Some(&self.chain.entries[(self.pos - 1) as usize])
            }
        }
    }

    /// Number of entries remaining past the current position.
    pub fn remaining(&self) -> usize {
        let next = (self.pos + 1).max(0) as usize;
        self.chain.entries.len().saturating_sub(next)
    }
}

// ── Hex helpers ───────────────────────────────────────────────────────────────

pub fn encode_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn decode_hex(s: &str) -> anyhow::Result<Vec<u8>> {
    anyhow::ensure!(s.len() % 2 == 0, "odd-length hex string");
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).map_err(|e| anyhow::anyhow!("{e}")))
        .collect()
}

fn decode_hex_unchecked(s: &str) -> Vec<u8> {
    decode_hex(s).unwrap_or_default()
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    fn make_chain() -> FixtureChain {
        FixtureChain {
            anchor: Point::Origin,
            entries: vec![
                FixtureEntry {
                    slot: 10,
                    block_hash: "aa".repeat(32),
                    block_number: 1,
                    cbor_hex: "deadbeef".into(),
                    variant: DEFAULT_HEADER_VARIANT,
                },
                FixtureEntry {
                    slot: 20,
                    block_hash: "bb".repeat(32),
                    block_number: 2,
                    cbor_hex: "cafebabe".into(),
                    variant: DEFAULT_HEADER_VARIANT,
                },
                FixtureEntry {
                    slot: 30,
                    block_hash: "cc".repeat(32),
                    block_number: 3,
                    cbor_hex: "f00df00d".into(),
                    variant: DEFAULT_HEADER_VARIANT,
                },
            ],
        }
    }

    // ── load / save round-trip ────────────────────────────────────────────────

    #[test]
    fn fixture_round_trips_through_file() {
        let chain = make_chain();
        let tmp = NamedTempFile::new().unwrap();
        write_anchor(tmp.path(), &chain.anchor).unwrap();
        for e in &chain.entries {
            append_entry(tmp.path(), e).unwrap();
        }

        let loaded = load(tmp.path()).unwrap();
        assert!(matches!(loaded.anchor, Point::Origin));
        assert_eq!(loaded.entries.len(), 3);
        assert_eq!(loaded.entries[0].slot, 10);
        assert_eq!(loaded.entries[2].block_number, 3);
    }

    #[test]
    fn load_empty_file_errors() {
        let tmp = NamedTempFile::new().unwrap();
        assert!(load(tmp.path()).is_err());
    }

    #[test]
    fn load_anchor_only_gives_empty_entries() {
        let tmp = NamedTempFile::new().unwrap();
        write_anchor(tmp.path(), &Point::Origin).unwrap();
        let chain = load(tmp.path()).unwrap();
        assert_eq!(chain.entries.len(), 0);
    }

    #[test]
    fn write_anchor_truncates_on_second_capture_keeping_exactly_one_anchor() {
        // Invariant: write_anchor always truncates, so a second capture run on the
        // same path produces a well-formed file (one anchor, M entries) not a
        // concatenation of two runs (two anchors, N+M entries).
        let tmp = NamedTempFile::new().unwrap();
        let entry = FixtureEntry { slot: 1, block_hash: "aa".repeat(32), block_number: 1, cbor_hex: "deadbeef".into(), variant: DEFAULT_HEADER_VARIANT };

        write_anchor(tmp.path(), &Point::Origin).unwrap();
        append_entry(tmp.path(), &entry).unwrap(); // first run: 1 anchor + 1 entry

        write_anchor(tmp.path(), &Point::Origin).unwrap();
        append_entry(tmp.path(), &entry).unwrap();
        append_entry(tmp.path(), &entry).unwrap(); // second run: truncated, 1 anchor + 2 entries

        let chain = load(tmp.path()).unwrap();
        assert_eq!(chain.entries.len(), 2, "second capture should have replaced the first, not appended to it");
    }

    // ── Cursor: find_intersect ────────────────────────────────────────────────

    #[test]
    fn cursor_finds_origin() {
        let chain = make_chain();
        let cursor = Cursor::new(&chain);
        let pos = cursor.find_intersect(&[Point::Origin]);
        assert_eq!(pos, Some(0));
    }

    #[test]
    fn cursor_finds_specific_block() {
        let chain = make_chain();
        let cursor = Cursor::new(&chain);
        let hash = decode_hex_unchecked(&"bb".repeat(32));
        let pos = cursor.find_intersect(&[Point::Specific(20, hash)]);
        assert_eq!(pos, Some(2));
    }

    #[test]
    fn cursor_returns_first_match_in_candidate_order() {
        // Clients send most-recent-first; the server must pick the first candidate
        // that exists, not the highest slot. Here slot 30 is listed first so it wins
        // even though slot 10 is also present.
        let chain = make_chain();
        let cursor = Cursor::new(&chain);
        let h1 = decode_hex_unchecked(&"aa".repeat(32));
        let h2 = decode_hex_unchecked(&"cc".repeat(32));

        // Most-recent first — slot 30 should win.
        let pos = cursor.find_intersect(&[
            Point::Specific(30, h2.clone()),
            Point::Specific(10, h1.clone()),
        ]);
        assert_eq!(pos, Some(3)); // slot 30 = block index 3

        // Least-recent first — slot 10 wins now.
        let pos2 = cursor.find_intersect(&[
            Point::Specific(10, h1),
            Point::Specific(30, h2),
        ]);
        assert_eq!(pos2, Some(1)); // slot 10 = block index 1
    }

    #[test]
    fn cursor_returns_none_for_no_match() {
        let chain = make_chain();
        let cursor = Cursor::new(&chain);
        let pos = cursor.find_intersect(&[Point::Specific(99, vec![0xff; 32])]);
        assert_eq!(pos, None);
    }

    // ── Cursor: advance ───────────────────────────────────────────────────────

    #[test]
    fn cursor_advance_through_chain() {
        let chain = make_chain();
        let mut cursor = Cursor::new(&chain);
        cursor.set_pos(0); // at anchor

        let e1 = cursor.advance().unwrap();
        assert_eq!(e1.slot, 10);
        assert_eq!(cursor.remaining(), 1);

        let e2 = cursor.advance().unwrap();
        assert_eq!(e2.slot, 20);

        let e3 = cursor.advance().unwrap();
        assert_eq!(e3.slot, 30);

        assert!(cursor.advance().is_none(), "advance past tip should return None");
    }

    #[test]
    fn cursor_advance_from_initial_pos_skips_anchor() {
        let chain = make_chain();
        let mut cursor = Cursor::new(&chain);
        // Initial pos = -1; advancing to 0 lands at anchor, no header yet
        // Advancing again gives the first block.
        // Actually advance() from -1: next=0, pos=0, return None (anchor)
        let at_anchor = cursor.advance();
        assert!(at_anchor.is_none()); // moved to anchor, no header
        let first = cursor.advance().unwrap();
        assert_eq!(first.slot, 10);
    }

    #[test]
    fn cursor_tip_reflects_last_entry() {
        let chain = make_chain();
        let cursor = Cursor::new(&chain);
        let Tip(tip_point, block_num) = cursor.tip();
        assert_eq!(block_num, 3);
        assert!(matches!(tip_point, Point::Specific(30, _)));
    }
}
