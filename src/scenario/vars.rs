use std::collections::HashMap;

use anyhow::Context as _;
use pallas_network::miniprotocols::Point;
use serde_json::Value;

// ── Variable store ────────────────────────────────────────────────────────────

/// Flat, string-keyed variable namespace for a scenario execution.
pub type VarStore = HashMap<String, Value>;

// ── Reference resolution ──────────────────────────────────────────────────────

/// Resolves a variable reference expression against the variable store.
///
/// Supported forms:
/// - `$name`       — the whole variable value
/// - `$name.field` — a field of a record-shaped variable
/// - `$name[i]`    — an element of a list variable
///
/// Returns a clear error if the variable, field, or index does not exist.
pub fn resolve_ref(expr: &str, vars: &VarStore) -> anyhow::Result<Value> {
    let rest = expr
        .strip_prefix('$')
        .ok_or_else(|| anyhow::anyhow!("not a variable reference: \"{expr}\""))?;

    if let Some((name, field)) = rest.split_once('.') {
        let base = vars
            .get(name)
            .ok_or_else(|| anyhow::anyhow!("unknown variable: \"{name}\""))?;
        base.get(field)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("\"{name}\" has no field \"{field}\""))
    } else if let Some(bracket) = rest.find('[') {
        let name = &rest[..bracket];
        let idx_str = rest[bracket + 1..].trim_end_matches(']');
        let idx: usize = idx_str
            .parse()
            .with_context(|| format!("invalid array index in \"{expr}\""))?;
        let base = vars
            .get(name)
            .ok_or_else(|| anyhow::anyhow!("unknown variable: \"{name}\""))?;
        base.get(idx)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("index {idx} out of bounds in \"{name}\""))
    } else {
        vars.get(rest)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("unknown variable: \"{rest}\""))
    }
}

/// Recursively substitutes all `$ref` strings in a JSON value in-place.
///
/// Any `Value::String` that starts with `$` is replaced by the resolved value.
/// Arrays and objects are walked recursively. Non-string, non-container values
/// are left unchanged.
///
/// Returns a list of `(reference_expr, resolved_type_name)` for every
/// substitution performed — callers may emit `VariableReferenced` trace events
/// from this list.
pub fn substitute_in_value(
    v: &mut Value,
    vars: &VarStore,
) -> anyhow::Result<Vec<(String, &'static str)>> {
    let mut refs_resolved: Vec<(String, &'static str)> = Vec::new();
    substitute_inner(v, vars, &mut refs_resolved)?;
    Ok(refs_resolved)
}

fn substitute_inner(
    v: &mut Value,
    vars: &VarStore,
    out: &mut Vec<(String, &'static str)>,
) -> anyhow::Result<()> {
    match v {
        Value::String(s) if s.starts_with('$') => {
            let expr = s.clone();
            let resolved = resolve_ref(&expr, vars)
                .with_context(|| format!("resolving reference \"{expr}\""))?;
            let type_name = json_type_name(&resolved);
            out.push((expr, type_name));
            *v = resolved;
        }
        Value::Array(arr) => {
            for item in arr.iter_mut() {
                substitute_inner(item, vars, out)?;
            }
        }
        Value::Object(obj) => {
            for val in obj.values_mut() {
                substitute_inner(val, vars, out)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn json_type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

// ── Point formatting for variable storage ─────────────────────────────────────

/// Formats a `Point` as the string used in variable storage and scenario files.
/// `Point::Origin` → `"origin"`, `Point::Specific(slot, hash)` → `"slot:hexhash"`.
pub fn point_to_str(point: &Point) -> String {
    match point {
        Point::Origin => "origin".to_string(),
        Point::Specific(slot, hash) => {
            let hex: String = hash.iter().map(|b| format!("{b:02x}")).collect();
            format!("{slot}:{hex}")
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn store(pairs: &[(&str, Value)]) -> VarStore {
        pairs.iter().map(|(k, v)| (k.to_string(), v.clone())).collect()
    }

    // ── resolve_ref ───────────────────────────────────────────────────────────

    #[test]
    fn resolve_simple_variable() {
        let vars = store(&[("count", json!(10))]);
        assert_eq!(resolve_ref("$count", &vars).unwrap(), json!(10));
    }

    #[test]
    fn resolve_field_projection() {
        let vars = store(&[("tip", json!({"point": "62:aabb", "block_number": 260}))]);
        assert_eq!(resolve_ref("$tip.point", &vars).unwrap(), json!("62:aabb"));
        assert_eq!(resolve_ref("$tip.block_number", &vars).unwrap(), json!(260));
    }

    #[test]
    fn resolve_index_access() {
        let vars = store(&[("pts", json!(["origin", "62:aabb", "63:ccdd"]))]);
        assert_eq!(resolve_ref("$pts[0]", &vars).unwrap(), json!("origin"));
        assert_eq!(resolve_ref("$pts[2]", &vars).unwrap(), json!("63:ccdd"));
    }

    #[test]
    fn resolve_unknown_variable_gives_clear_error() {
        let vars = VarStore::new();
        let err = resolve_ref("$foo", &vars).unwrap_err();
        assert!(err.to_string().contains("unknown variable"), "{err}");
        assert!(err.to_string().contains("foo"), "{err}");
    }

    #[test]
    fn resolve_missing_field_gives_clear_error() {
        let vars = store(&[("tip", json!({"point": "origin"}))]);
        let err = resolve_ref("$tip.block_number", &vars).unwrap_err();
        assert!(err.to_string().contains("tip"), "{err}");
        assert!(err.to_string().contains("block_number"), "{err}");
    }

    #[test]
    fn resolve_out_of_bounds_gives_clear_error() {
        let vars = store(&[("pts", json!(["a", "b"]))]);
        let err = resolve_ref("$pts[5]", &vars).unwrap_err();
        assert!(err.to_string().contains("5"), "{err}");
        assert!(err.to_string().contains("pts"), "{err}");
    }

    // ── substitute_in_value ───────────────────────────────────────────────────

    #[test]
    fn substitute_replaces_top_level_string() {
        let vars = store(&[("x", json!(42))]);
        let mut v = json!("$x");
        let resolved = substitute_in_value(&mut v, &vars).unwrap();
        assert_eq!(v, json!(42));
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].0, "$x");
        assert_eq!(resolved[0].1, "number");
    }

    #[test]
    fn substitute_replaces_in_array() {
        let vars = store(&[("p", json!("62:aabb"))]);
        let mut v = json!(["origin", "$p", "literal"]);
        substitute_in_value(&mut v, &vars).unwrap();
        assert_eq!(v, json!(["origin", "62:aabb", "literal"]));
    }

    #[test]
    fn substitute_replaces_in_nested_object() {
        let vars = store(&[("n", json!(5))]);
        let mut v = json!({"count": "$n", "other": 1});
        substitute_in_value(&mut v, &vars).unwrap();
        assert_eq!(v["count"], json!(5));
        assert_eq!(v["other"], json!(1));
    }

    #[test]
    fn substitute_leaves_non_refs_unchanged() {
        let vars = VarStore::new();
        let mut v = json!({"count": 10, "arr": [1, 2]});
        substitute_in_value(&mut v, &vars).unwrap();
        assert_eq!(v, json!({"count": 10, "arr": [1, 2]}));
    }

    #[test]
    fn substitute_returns_error_for_unknown_var() {
        let vars = VarStore::new();
        let mut v = json!("$missing");
        assert!(substitute_in_value(&mut v, &vars).is_err());
    }

    // ── point_to_str ──────────────────────────────────────────────────────────

    #[test]
    fn point_to_str_origin() {
        assert_eq!(point_to_str(&Point::Origin), "origin");
    }

    #[test]
    fn point_to_str_specific() {
        let s = point_to_str(&Point::Specific(62, vec![0xaa, 0xbb]));
        assert_eq!(s, "62:aabb");
    }
}
