//! Response rule types for scripted Chain-Sync and Block-Fetch serving.
//!
//! A `serve_chain_sync` or `serve_block_fetch` step can specify its behaviour
//! either via a fixture path (auto-generated honest script) or an explicit
//! `responses` list. Both paths converge on `Vec<ScriptRule>` at execution time.

use serde::Deserialize;

use crate::scenario::block_fixture::{BlockFixtureChain, BlockFixtureEntry};
use crate::scenario::fixture::{FixtureChain, FixtureEntry, DEFAULT_HEADER_VARIANT};

// ── User-facing JSON types ────────────────────────────────────────────────────

/// Which incoming message kind a rule matches.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum On {
    // Chain-Sync
    RequestNext,
    FindIntersect,
    // Block-Fetch
    RequestRange,
    // Both protocols (session-end message)
    Done,
    // Matches any incoming message in either protocol
    Any,
}

/// Optional tip to include in a Chain-Sync response.
#[derive(Debug, Clone, Deserialize)]
pub struct TipSpec {
    pub point: String,
    pub block_number: u64,
}

/// The response action specified in a scenario JSON `send` object.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SendDef {
    // ── Chain-Sync sends ──────────────────────────────────────────────────────
    RollForward {
        header_from_fixture: Option<usize>,
        header_cbor: Option<String>,
        /// Note: era-specific default. Update `DEFAULT_HEADER_VARIANT` in
        /// fixture.rs when Cardano introduces new eras.
        variant: Option<u8>,
        tip: Option<TipSpec>,
    },
    RollBackward  { point: String, tip: Option<TipSpec> },
    IntersectFound    { point: String, tip: Option<TipSpec> },
    IntersectNotFound { tip: Option<TipSpec> },
    AwaitReply { #[serde(default)] hold_secs: u64 },

    // ── Block-Fetch sends ─────────────────────────────────────────────────────
    /// MsgStartBatch — begins a streaming batch.
    StartBatch,
    /// MsgBlock — one block body within a batch.
    Block {
        block_from_fixture: Option<usize>,
        block_cbor: Option<String>,
    },
    /// MsgBatchDone — ends the current batch.
    BatchDone,
    /// MsgNoBlocks — decline the range request.
    NoBlocks,
    /// Convenience: expands to StartBatch + N×Block + BatchDone.
    StreamBatch {
        /// Explicit fixture indices; all are served regardless of the request.
        block_indices: Option<Vec<usize>>,
        /// If true, serve the entries matching the client's requested range from
        /// the fixture. Produces NoBlocks if the range is unsatisfiable.
        block_count_from_request: Option<bool>,
    },

    // ── Protocol-agnostic sends ───────────────────────────────────────────────
    Wait       { duration_secs: u64 },
    Disconnect,
    RawBytes   { hex: String },
    /// Emit an ordered list of sends for a single incoming message without
    /// returning to the receive loop between them. Natural for Block-Fetch's
    /// producer-driven streaming (e.g. StartBatch + blocks + Disconnect).
    /// Nesting a send_sequence inside another send_sequence is rejected at
    /// parse time.
    SendSequence { sends: Vec<SendDef> },
}

/// One response rule as written in a scenario file.
#[derive(Debug, Clone, Deserialize)]
pub struct ResponseRuleDef {
    pub on: On,
    pub send: SendDef,
}

// ── Internal runtime types ────────────────────────────────────────────────────

/// Runtime header source for Chain-Sync RollForward.
#[derive(Debug, Clone)]
pub enum HeaderSource {
    FixtureEntry(FixtureEntry),
    Literal { cbor: Vec<u8>, variant: u8 },
}

/// Runtime block body source for Block-Fetch Block.
#[derive(Debug, Clone)]
pub enum BlockSource {
    FixtureEntry(BlockFixtureEntry),
    Literal(Vec<u8>),
}

/// Runtime send action.
#[derive(Debug, Clone)]
pub enum ScriptSend {
    // Chain-Sync
    RollForward { source: HeaderSource, tip: Option<TipSpec> },
    RollBackward  { point: String, tip: Option<TipSpec> },
    IntersectFound    { point: String, tip: Option<TipSpec> },
    IntersectNotFound { tip: Option<TipSpec> },
    AwaitReply { hold_secs: u64 },
    // Block-Fetch
    StartBatch,
    Block { source: BlockSource },
    BatchDone,
    NoBlocks,
    StreamBatch { sources: StreamBatchSources },
    // Both
    Wait { duration_secs: u64 },
    Disconnect,
    RawBytes { bytes: Vec<u8> },
    /// Ordered sub-sends emitted for one incoming message without a recv between them.
    /// Sub-sends may not themselves be SendSequence (flat only, validated at parse time).
    SendSequence { sends: Vec<ScriptSend> },
    // Generated-only sentinels
    CursorFindIntersect,   // Chain-Sync: search cursor, send IntersectFound/NotFound
    CursorAdvance,         // Chain-Sync: advance cursor, send RollForward
    CursorRange,           // Block-Fetch: satisfy range from fixture cursor (repeatable)
}

/// How StreamBatch obtains its block bodies.
#[derive(Debug, Clone)]
pub enum StreamBatchSources {
    /// Explicit list of resolved bodies.
    Explicit(Vec<BlockSource>),
    /// Resolved at execution time from the requested range (CursorRange behaviour).
    FromRequest,
}

impl ScriptSend {
    pub fn kind_str(&self) -> &'static str {
        match self {
            ScriptSend::RollForward { .. }        => "roll_forward",
            ScriptSend::RollBackward { .. }       => "roll_backward",
            ScriptSend::IntersectFound { .. }     => "intersect_found",
            ScriptSend::IntersectNotFound { .. }  => "intersect_not_found",
            ScriptSend::AwaitReply { .. }         => "await_reply",
            ScriptSend::StartBatch                => "start_batch",
            ScriptSend::Block { .. }              => "block",
            ScriptSend::BatchDone                 => "batch_done",
            ScriptSend::NoBlocks                  => "no_blocks",
            ScriptSend::StreamBatch { .. }        => "stream_batch",
            ScriptSend::Wait { .. }               => "wait",
            ScriptSend::Disconnect                => "disconnect",
            ScriptSend::RawBytes { .. }           => "raw_bytes",
            ScriptSend::SendSequence { .. }       => "send_sequence",
            ScriptSend::CursorFindIntersect       => "cursor_find_intersect",
            ScriptSend::CursorAdvance             => "cursor_advance",
            ScriptSend::CursorRange               => "cursor_range",
        }
    }
}

/// One runtime rule.
#[derive(Debug, Clone)]
pub struct ScriptRule {
    pub on: On,
    pub send: ScriptSend,
    /// If `true`, the rule is not consumed after matching — it can fire again
    /// on subsequent requests. Used for auto-generated Block-Fetch scripts where
    /// the number of range requests from the client is not known in advance.
    pub repeatable: bool,
}

impl ScriptRule {
    pub fn on_str(&self) -> &'static str {
        match self.on {
            On::RequestNext   => "request_next",
            On::FindIntersect => "find_intersect",
            On::RequestRange  => "request_range",
            On::Done          => "done",
            On::Any           => "any",
        }
    }
}

// ── Conversion ────────────────────────────────────────────────────────────────

/// Convert a single `SendDef` to a `ScriptSend`.
///
/// Does NOT handle `SendDef::SendSequence` — callers that need it must check
/// for nesting first and then call this for each sub-send.
fn convert_send_def(
    def: &SendDef,
    cs_fixture: Option<&FixtureChain>,
    bf_fixture: Option<&BlockFixtureChain>,
) -> anyhow::Result<ScriptSend> {
    Ok(match def {
        SendDef::RollForward { header_from_fixture, header_cbor, variant, tip } => {
            let source = match (header_from_fixture, header_cbor) {
                (Some(idx), None) => {
                    let chain = cs_fixture.ok_or_else(|| {
                        anyhow::anyhow!("header_from_fixture requires fixture_path to be set")
                    })?;
                    let entry = chain.entries.get(*idx).ok_or_else(|| {
                        anyhow::anyhow!(
                            "header_from_fixture index {idx} out of range (fixture has {} entries)",
                            chain.entries.len()
                        )
                    })?;
                    HeaderSource::FixtureEntry(entry.clone())
                }
                (None, Some(hex)) => {
                    let cbor = decode_hex(hex)
                        .map_err(|e| anyhow::anyhow!("header_cbor: invalid hex: {e}"))?;
                    HeaderSource::Literal {
                        cbor,
                        variant: variant.unwrap_or(DEFAULT_HEADER_VARIANT),
                    }
                }
                (Some(_), Some(_)) => anyhow::bail!(
                    "roll_forward: header_from_fixture and header_cbor are mutually exclusive"
                ),
                (None, None) => anyhow::bail!(
                    "roll_forward: one of header_from_fixture or header_cbor is required"
                ),
            };
            ScriptSend::RollForward { source, tip: tip.clone() }
        }
        SendDef::RollBackward { point, tip } =>
            ScriptSend::RollBackward { point: point.clone(), tip: tip.clone() },
        SendDef::IntersectFound { point, tip } =>
            ScriptSend::IntersectFound { point: point.clone(), tip: tip.clone() },
        SendDef::IntersectNotFound { tip } =>
            ScriptSend::IntersectNotFound { tip: tip.clone() },
        SendDef::AwaitReply { hold_secs } =>
            ScriptSend::AwaitReply { hold_secs: *hold_secs },

        SendDef::StartBatch => ScriptSend::StartBatch,
        SendDef::BatchDone  => ScriptSend::BatchDone,
        SendDef::NoBlocks   => ScriptSend::NoBlocks,

        SendDef::Block { block_from_fixture, block_cbor } => {
            let source = match (block_from_fixture, block_cbor) {
                (Some(idx), None) => {
                    let chain = bf_fixture.ok_or_else(|| {
                        anyhow::anyhow!("block_from_fixture requires block_fetch_fixture_path")
                    })?;
                    let entry = chain.entries.get(*idx).ok_or_else(|| {
                        anyhow::anyhow!(
                            "block_from_fixture index {idx} out of range (fixture has {} entries)",
                            chain.entries.len()
                        )
                    })?;
                    BlockSource::FixtureEntry(entry.clone())
                }
                (None, Some(hex)) => {
                    let bytes = decode_hex(hex)
                        .map_err(|e| anyhow::anyhow!("block_cbor: invalid hex: {e}"))?;
                    BlockSource::Literal(bytes)
                }
                (Some(_), Some(_)) => anyhow::bail!(
                    "block: block_from_fixture and block_cbor are mutually exclusive"
                ),
                (None, None) => anyhow::bail!(
                    "block: one of block_from_fixture or block_cbor is required"
                ),
            };
            ScriptSend::Block { source }
        }

        SendDef::StreamBatch { block_indices, block_count_from_request } => {
            let sources = match (block_indices, block_count_from_request) {
                (Some(indices), _) => {
                    let chain = bf_fixture.ok_or_else(|| {
                        anyhow::anyhow!("stream_batch with block_indices requires block_fetch_fixture_path")
                    })?;
                    let resolved = indices.iter()
                        .enumerate()
                        .map(|(n, &idx)| {
                            chain.entries.get(idx).map(|e| BlockSource::FixtureEntry(e.clone()))
                                .ok_or_else(|| anyhow::anyhow!(
                                    "stream_batch block_indices[{n}]={idx} out of range"
                                ))
                        })
                        .collect::<anyhow::Result<Vec<_>>>()?;
                    StreamBatchSources::Explicit(resolved)
                }
                (None, Some(true)) => StreamBatchSources::FromRequest,
                (None, _) => anyhow::bail!(
                    "stream_batch: set block_indices or block_count_from_request: true"
                ),
            };
            ScriptSend::StreamBatch { sources }
        }

        SendDef::Wait { duration_secs } =>
            ScriptSend::Wait { duration_secs: *duration_secs },
        SendDef::Disconnect => ScriptSend::Disconnect,
        SendDef::RawBytes { hex } => {
            let bytes = decode_hex(hex)
                .map_err(|e| anyhow::anyhow!("raw_bytes: invalid hex: {e}"))?;
            ScriptSend::RawBytes { bytes }
        }

        // Callers handle nesting rejection before calling this function.
        SendDef::SendSequence { .. } =>
            anyhow::bail!("convert_send_def called on SendSequence — use rule_def_to_script"),
    })
}

/// Convert a user-facing rule definition into a runtime rule.
pub fn rule_def_to_script(
    def: &ResponseRuleDef,
    cs_fixture: Option<&FixtureChain>,
    bf_fixture: Option<&BlockFixtureChain>,
) -> anyhow::Result<ScriptRule> {
    let send = if let SendDef::SendSequence { sends } = &def.send {
        let mut converted = Vec::with_capacity(sends.len());
        for (i, sub) in sends.iter().enumerate() {
            if matches!(sub, SendDef::SendSequence { .. }) {
                anyhow::bail!(
                    "send_sequence: sends[{i}] is itself a send_sequence — \
                     nesting is not allowed; flatten the sub-sends into the outer sequence"
                );
            }
            converted.push(convert_send_def(sub, cs_fixture, bf_fixture)?);
        }
        ScriptSend::SendSequence { sends: converted }
    } else {
        convert_send_def(&def.send, cs_fixture, bf_fixture)?
    };
    Ok(ScriptRule { on: def.on.clone(), send, repeatable: false })
}

// ── Chain-Sync fixture-to-script generation ───────────────────────────────────

pub fn generate_from_fixture(chain: &FixtureChain, await_at_tip_secs: u64) -> Vec<ScriptRule> {
    let mut rules = Vec::new();
    rules.push(ScriptRule { on: On::FindIntersect, send: ScriptSend::CursorFindIntersect, repeatable: false });
    for _ in &chain.entries {
        rules.push(ScriptRule { on: On::RequestNext, send: ScriptSend::CursorAdvance, repeatable: false });
    }
    rules.push(ScriptRule { on: On::RequestNext, send: ScriptSend::AwaitReply { hold_secs: await_at_tip_secs }, repeatable: false });
    rules.push(ScriptRule { on: On::Any, send: ScriptSend::Disconnect, repeatable: false });
    rules
}

// ── Block-Fetch fixture-to-script generation ──────────────────────────────────

/// Auto-generate an honest Block-Fetch response script from a fixture.
///
/// Normal mode: one repeatable `CursorRange` rule (serves whatever range the
/// client asks for, taking entries from the fixture by point), plus a trailing
/// `Disconnect` on any remaining message.
///
/// `no_blocks_default` mode: one repeatable `NoBlocks` rule, so the harness
/// declines every range request, plus a trailing `Disconnect`.
pub fn generate_for_block_fetch(no_blocks_default: bool) -> Vec<ScriptRule> {
    let range_send = if no_blocks_default {
        ScriptSend::NoBlocks
    } else {
        ScriptSend::CursorRange
    };
    vec![
        ScriptRule { on: On::RequestRange, send: range_send, repeatable: true },
        ScriptRule { on: On::Any,          send: ScriptSend::Disconnect, repeatable: false },
    ]
}

// ── Hex helper ────────────────────────────────────────────────────────────────

fn decode_hex(s: &str) -> anyhow::Result<Vec<u8>> {
    anyhow::ensure!(s.len() % 2 == 0, "odd-length hex string");
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).map_err(|e| anyhow::anyhow!("{e}")))
        .collect()
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scenario::fixture::FixtureEntry;
    use crate::scenario::fixture::DEFAULT_HEADER_VARIANT;

    fn make_cs_chain(n: usize) -> FixtureChain {
        FixtureChain {
            anchor: pallas_network::miniprotocols::Point::Origin,
            entries: (0..n as u64).map(|i| FixtureEntry {
                slot: i + 1, block_hash: format!("{:064x}", i),
                block_number: i + 1, cbor_hex: "deadbeef".into(),
                variant: DEFAULT_HEADER_VARIANT,
            }).collect(),
        }
    }

    fn make_bf_chain(n: usize) -> BlockFixtureChain {
        BlockFixtureChain {
            anchor: pallas_network::miniprotocols::Point::Origin,
            entries: (0..n as u64).map(|i| crate::scenario::block_fixture::BlockFixtureEntry {
                slot: i + 1, block_hash: format!("{:064x}", i),
                block_cbor_hex: "cafebabe".into(),
            }).collect(),
        }
    }

    #[test]
    fn rule_parses_roll_forward_fixture() {
        let json = r#"{"on":"request_next","send":{"kind":"roll_forward","header_from_fixture":0}}"#;
        let def: ResponseRuleDef = serde_json::from_str(json).unwrap();
        assert_eq!(def.on, On::RequestNext);
        assert!(matches!(def.send, SendDef::RollForward { header_from_fixture: Some(0), .. }));
    }

    #[test]
    fn rule_parses_no_blocks() {
        let json = r#"{"on":"request_range","send":{"kind":"no_blocks"}}"#;
        let def: ResponseRuleDef = serde_json::from_str(json).unwrap();
        assert_eq!(def.on, On::RequestRange);
        assert!(matches!(def.send, SendDef::NoBlocks));
    }

    #[test]
    fn rule_parses_stream_batch_indices() {
        let json = r#"{"on":"request_range","send":{"kind":"stream_batch","block_indices":[0,1,2]}}"#;
        let def: ResponseRuleDef = serde_json::from_str(json).unwrap();
        assert!(matches!(def.send, SendDef::StreamBatch { block_indices: Some(_), .. }));
    }

    #[test]
    fn rule_parses_stream_batch_from_request() {
        let json = r#"{"on":"request_range","send":{"kind":"stream_batch","block_count_from_request":true}}"#;
        let def: ResponseRuleDef = serde_json::from_str(json).unwrap();
        assert!(matches!(def.send, SendDef::StreamBatch { block_count_from_request: Some(true), .. }));
    }

    #[test]
    fn rule_def_to_script_resolves_block_fixture_entry() {
        let chain = make_bf_chain(3);
        let def: ResponseRuleDef = serde_json::from_str(
            r#"{"on":"request_range","send":{"kind":"block","block_from_fixture":1}}"#
        ).unwrap();
        let rule = rule_def_to_script(&def, None, Some(&chain)).unwrap();
        assert!(matches!(rule.send, ScriptSend::Block { source: BlockSource::FixtureEntry(_) }));
    }

    #[test]
    fn rule_def_to_script_stream_batch_from_request() {
        let def: ResponseRuleDef = serde_json::from_str(
            r#"{"on":"request_range","send":{"kind":"stream_batch","block_count_from_request":true}}"#
        ).unwrap();
        let rule = rule_def_to_script(&def, None, None).unwrap();
        assert!(matches!(rule.send, ScriptSend::StreamBatch { sources: StreamBatchSources::FromRequest }));
    }

    #[test]
    fn generate_from_fixture_count() {
        let chain = make_cs_chain(4);
        let rules = generate_from_fixture(&chain, 30);
        assert_eq!(rules.len(), 7); // 1 + 4 + 1 + 1
        assert!(!rules[0].repeatable);
    }

    #[test]
    fn generate_for_block_fetch_normal() {
        let rules = generate_for_block_fetch(false);
        assert_eq!(rules.len(), 2);
        assert!(matches!(rules[0].send, ScriptSend::CursorRange));
        assert!(rules[0].repeatable);
        assert!(matches!(rules[1].send, ScriptSend::Disconnect));
    }

    #[test]
    fn generate_for_block_fetch_no_blocks() {
        let rules = generate_for_block_fetch(true);
        assert_eq!(rules.len(), 2);
        assert!(matches!(rules[0].send, ScriptSend::NoBlocks));
        assert!(rules[0].repeatable);
    }
}
