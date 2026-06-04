//! Response rule types for scripted Chain-Sync serving.
//!
//! A `serve_chain_sync` step can specify its behaviour either via a
//! `fixture_path` (auto-generated honest script) or an explicit `responses`
//! list. Both paths converge on `Vec<ScriptRule>` at execution time — the
//! execution loop never branches on "fixture mode vs script mode".

use serde::Deserialize;

use crate::scenario::fixture::{FixtureChain, FixtureEntry, DEFAULT_HEADER_VARIANT};

// ── User-facing JSON types ────────────────────────────────────────────────────

/// Which incoming message kind a rule matches.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum On {
    RequestNext,
    FindIntersect,
    Done,
    Any,
}

/// Optional tip to include in a response. Omitting it uses the fixture tip
/// (in cursor mode) or a zero tip (in pure-script mode).
#[derive(Debug, Clone, Deserialize)]
pub struct TipSpec {
    pub point: String,
    pub block_number: u64,
}

/// The response action specified in a scenario JSON `send` object.
/// Serialised with `"kind"` as the tag.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SendDef {
    RollForward {
        /// Zero-based index into the loaded fixture for the header source.
        header_from_fixture: Option<usize>,
        /// Literal CBOR hex for the header body.
        header_cbor: Option<String>,
        /// Era variant byte. Defaults to `DEFAULT_HEADER_VARIANT` when
        /// `header_cbor` is used. Ignored when `header_from_fixture` is used
        /// (the variant is taken from the fixture entry).
        ///
        /// Note: this default is era-specific. Update `DEFAULT_HEADER_VARIANT`
        /// in fixture.rs when Cardano introduces new eras.
        variant: Option<u8>,
        tip: Option<TipSpec>,
    },
    RollBackward {
        point: String,
        tip: Option<TipSpec>,
    },
    IntersectFound {
        point: String,
        tip: Option<TipSpec>,
    },
    IntersectNotFound {
        tip: Option<TipSpec>,
    },
    AwaitReply {
        #[serde(default)]
        hold_secs: u64,
    },
    Wait {
        duration_secs: u64,
    },
    Disconnect,
    RawBytes {
        hex: String,
    },
}

/// One response rule as written in a scenario file.
#[derive(Debug, Clone, Deserialize)]
pub struct ResponseRuleDef {
    pub on: On,
    pub send: SendDef,
    // match_payload: reserved for future fine-grained FindIntersect matching
}

// ── Internal runtime types ────────────────────────────────────────────────────

/// Runtime variant of a header source — already resolved to concrete bytes.
#[derive(Debug, Clone)]
pub enum HeaderSource {
    /// From the fixture at the given index. Variant comes from the fixture entry.
    FixtureEntry(FixtureEntry),
    /// Literal CBOR bytes with an explicit era variant.
    Literal { cbor: Vec<u8>, variant: u8 },
}

/// Runtime send action — either converted from a user `SendDef` or generated
/// by `generate_from_fixture`.
#[derive(Debug, Clone)]
pub enum ScriptSend {
    RollForward { source: HeaderSource, tip: Option<TipSpec> },
    RollBackward { point: String, tip: Option<TipSpec> },
    IntersectFound { point: String, tip: Option<TipSpec> },
    IntersectNotFound { tip: Option<TipSpec> },
    AwaitReply { hold_secs: u64 },
    Wait { duration_secs: u64 },
    Disconnect,
    RawBytes { bytes: Vec<u8> },
    /// Generated only: search the fixture cursor and send the result.
    CursorFindIntersect,
    /// Generated only: advance the fixture cursor and send RollForward.
    CursorAdvance,
}

impl ScriptSend {
    pub fn kind_str(&self) -> &'static str {
        match self {
            ScriptSend::RollForward { .. }        => "roll_forward",
            ScriptSend::RollBackward { .. }       => "roll_backward",
            ScriptSend::IntersectFound { .. }     => "intersect_found",
            ScriptSend::IntersectNotFound { .. }  => "intersect_not_found",
            ScriptSend::AwaitReply { .. }         => "await_reply",
            ScriptSend::Wait { .. }               => "wait",
            ScriptSend::Disconnect                => "disconnect",
            ScriptSend::RawBytes { .. }           => "raw_bytes",
            ScriptSend::CursorFindIntersect       => "cursor_find_intersect",
            ScriptSend::CursorAdvance             => "cursor_advance",
        }
    }
}

/// One runtime rule.
#[derive(Debug, Clone)]
pub struct ScriptRule {
    pub on: On,
    pub send: ScriptSend,
}

impl ScriptRule {
    pub fn on_str(&self) -> &'static str {
        match self.on {
            On::RequestNext   => "request_next",
            On::FindIntersect => "find_intersect",
            On::Done          => "done",
            On::Any           => "any",
        }
    }
}

// ── Conversion ────────────────────────────────────────────────────────────────

/// Convert a user-facing rule definition into a runtime rule.
/// The `fixture` is needed to resolve `header_from_fixture` references.
pub fn rule_def_to_script(
    def: &ResponseRuleDef,
    fixture: Option<&FixtureChain>,
) -> anyhow::Result<ScriptRule> {
    let send = match &def.send {
        SendDef::RollForward { header_from_fixture, header_cbor, variant, tip } => {
            let source = match (header_from_fixture, header_cbor) {
                (Some(idx), None) => {
                    let chain = fixture.ok_or_else(|| {
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
        SendDef::Wait { duration_secs } =>
            ScriptSend::Wait { duration_secs: *duration_secs },
        SendDef::Disconnect => ScriptSend::Disconnect,
        SendDef::RawBytes { hex } => {
            let bytes = decode_hex(hex)
                .map_err(|e| anyhow::anyhow!("raw_bytes: invalid hex: {e}"))?;
            ScriptSend::RawBytes { bytes }
        }
    };
    Ok(ScriptRule { on: def.on.clone(), send })
}

// ── Fixture-to-script generation ─────────────────────────────────────────────

/// Auto-generate a response script from a fixture chain.
///
/// The produced script is behaviourally identical to the slice-2 fixture-based
/// server: FindIntersect → cursor search, RequestNext × N → headers in order,
/// then AwaitReply, then Disconnect. No fixture-mode runtime branch exists;
/// this function is the only difference between the two paths.
pub fn generate_from_fixture(chain: &FixtureChain, await_at_tip_secs: u64) -> Vec<ScriptRule> {
    let mut rules = Vec::new();

    // Single FindIntersect rule: searches the cursor at runtime.
    rules.push(ScriptRule {
        on: On::FindIntersect,
        send: ScriptSend::CursorFindIntersect,
    });

    // One RequestNext rule per fixture entry.
    for _ in &chain.entries {
        rules.push(ScriptRule {
            on: On::RequestNext,
            send: ScriptSend::CursorAdvance,
        });
    }

    // At-tip: send AwaitReply and hold for the configured duration.
    rules.push(ScriptRule {
        on: On::RequestNext,
        send: ScriptSend::AwaitReply { hold_secs: await_at_tip_secs },
    });

    // After the await elapses, disconnect.
    rules.push(ScriptRule {
        on: On::Any,
        send: ScriptSend::Disconnect,
    });

    rules
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

    fn make_chain(n: usize) -> FixtureChain {
        FixtureChain {
            anchor: pallas_network::miniprotocols::Point::Origin,
            entries: (0..n as u64).map(|i| FixtureEntry {
                slot: i + 1,
                block_hash: format!("{:064x}", i),
                block_number: i + 1,
                cbor_hex: "deadbeef".into(),
                variant: DEFAULT_HEADER_VARIANT,
            }).collect(),
        }
    }

    #[test]
    fn response_rule_def_parses_roll_forward_fixture() {
        let json = r#"{"on":"request_next","send":{"kind":"roll_forward","header_from_fixture":0}}"#;
        let def: ResponseRuleDef = serde_json::from_str(json).unwrap();
        assert!(matches!(def.on, On::RequestNext));
        assert!(matches!(def.send, SendDef::RollForward { header_from_fixture: Some(0), .. }));
    }

    #[test]
    fn response_rule_def_parses_roll_forward_cbor() {
        let json = r#"{"on":"request_next","send":{"kind":"roll_forward","header_cbor":"deadbeef","variant":5}}"#;
        let def: ResponseRuleDef = serde_json::from_str(json).unwrap();
        assert!(matches!(def.send, SendDef::RollForward { header_cbor: Some(_), variant: Some(5), .. }));
    }

    #[test]
    fn response_rule_def_parses_raw_bytes() {
        let json = r#"{"on":"any","send":{"kind":"raw_bytes","hex":"cafebabe"}}"#;
        let def: ResponseRuleDef = serde_json::from_str(json).unwrap();
        assert!(matches!(def.send, SendDef::RawBytes { .. }));
    }

    #[test]
    fn response_rule_def_parses_disconnect() {
        let json = r#"{"on":"done","send":{"kind":"disconnect"}}"#;
        let def: ResponseRuleDef = serde_json::from_str(json).unwrap();
        assert!(matches!(def.send, SendDef::Disconnect));
    }

    #[test]
    fn rule_def_to_script_resolves_fixture_entry() {
        let chain = make_chain(3);
        let def: ResponseRuleDef = serde_json::from_str(
            r#"{"on":"request_next","send":{"kind":"roll_forward","header_from_fixture":1}}"#
        ).unwrap();
        let rule = rule_def_to_script(&def, Some(&chain)).unwrap();
        match rule.send {
            ScriptSend::RollForward { source: HeaderSource::FixtureEntry(e), .. } => {
                assert_eq!(e.slot, 2);
            }
            _ => panic!("expected RollForward with FixtureEntry"),
        }
    }

    #[test]
    fn rule_def_to_script_errors_on_out_of_range_fixture_index() {
        let chain = make_chain(2);
        let def: ResponseRuleDef = serde_json::from_str(
            r#"{"on":"request_next","send":{"kind":"roll_forward","header_from_fixture":5}}"#
        ).unwrap();
        assert!(rule_def_to_script(&def, Some(&chain)).is_err());
    }

    #[test]
    fn rule_def_to_script_cbor_uses_default_variant_when_omitted() {
        let def: ResponseRuleDef = serde_json::from_str(
            r#"{"on":"request_next","send":{"kind":"roll_forward","header_cbor":"aabb"}}"#
        ).unwrap();
        let rule = rule_def_to_script(&def, None).unwrap();
        match rule.send {
            ScriptSend::RollForward { source: HeaderSource::Literal { variant, .. }, .. } => {
                assert_eq!(variant, DEFAULT_HEADER_VARIANT);
            }
            _ => panic!("expected RollForward Literal"),
        }
    }

    #[test]
    fn generate_from_fixture_produces_correct_rule_count() {
        let chain = make_chain(4);
        let rules = generate_from_fixture(&chain, 30);
        // 1 FindIntersect + 4 CursorAdvance + 1 AwaitReply + 1 Disconnect = 7
        assert_eq!(rules.len(), 7);
        assert!(matches!(rules[0].send, ScriptSend::CursorFindIntersect));
        assert!(matches!(rules[1].send, ScriptSend::CursorAdvance));
        assert!(matches!(rules[4].send, ScriptSend::CursorAdvance));
        assert!(matches!(rules[5].send, ScriptSend::AwaitReply { hold_secs: 30 }));
        assert!(matches!(rules[6].send, ScriptSend::Disconnect));
    }

    #[test]
    fn generate_from_fixture_empty_chain_produces_minimal_rules() {
        let chain = make_chain(0);
        let rules = generate_from_fixture(&chain, 10);
        // 1 FindIntersect + 1 AwaitReply + 1 Disconnect = 3
        assert_eq!(rules.len(), 3);
    }
}
