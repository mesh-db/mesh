//! `apoc.map.*` — map construction and mutation helpers.
//!
//! All functions are side-effect-free transforms over
//! [`Property::Map`] (which is `HashMap<String, Property>` under the
//! hood — map keys in Mesh Cypher are always strings). Null-
//! propagating on the primary map argument; an argument list with a
//! null map short-circuits to `Null` so chained calls stay total.
//!
//! Functions shipped in this namespace:
//! - Construction: [`merge`], [`from_pairs`], [`from_lists`],
//!   [`from_values`], [`merge_list`].
//! - Mutation: [`set_key`], [`remove_key`], [`remove_keys`].
//! - Projection: [`values`], [`submap`].
//!
//! "Set" / "remove" variants return a *new* map — Cypher's value
//! semantics mean no caller ever observes in-place mutation, and
//! cloning a `HashMap<String, Property>` is cheap enough for the
//! small / medium maps these helpers typically run against.

use crate::ApocError;
use meshdb_core::Property;
use std::collections::HashMap;

pub fn call(suffix: &str, args: &[Property]) -> Result<Property, ApocError> {
    match suffix {
        "merge" => merge(args),
        "frompairs" => from_pairs(args),
        "fromlists" => from_lists(args),
        "fromvalues" => from_values(args),
        "setkey" => set_key(args),
        "removekey" => remove_key(args),
        "removekeys" => remove_keys(args),
        "values" => values(args),
        "submap" => submap(args),
        "mergelist" => merge_list(args),
        _ => Err(ApocError::UnknownFunction(format!("apoc.map.{suffix}"))),
    }
}

// ---------------------------------------------------------------
// Shared helpers.
// ---------------------------------------------------------------

fn check_arity(name: &str, args: &[Property], expected: usize) -> Result<(), ApocError> {
    if args.len() != expected {
        return Err(ApocError::Arity {
            name: format!("apoc.map.{name}"),
            expected,
            got: args.len(),
        });
    }
    Ok(())
}

/// Extract the map arg, short-circuiting on `Null`. `None` from this
/// helper means "return Property::Null".
fn as_map<'a>(
    name: &str,
    p: &'a Property,
) -> Result<Option<&'a HashMap<String, Property>>, ApocError> {
    match p {
        Property::Null => Ok(None),
        Property::Map(m) => Ok(Some(m)),
        other => Err(ApocError::TypeMismatch {
            name: format!("apoc.map.{name}"),
            details: format!("expected a map, got {}", other.type_name()),
        }),
    }
}

fn expect_string<'a>(name: &str, p: &'a Property) -> Result<&'a str, ApocError> {
    match p {
        Property::String(s) => Ok(s.as_str()),
        other => Err(ApocError::TypeMismatch {
            name: format!("apoc.map.{name}"),
            details: format!("expected string key, got {}", other.type_name()),
        }),
    }
}

fn as_string_list(name: &str, p: &Property) -> Result<Vec<String>, ApocError> {
    match p {
        Property::List(items) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                match item {
                    Property::String(s) => out.push(s.clone()),
                    other => {
                        return Err(ApocError::TypeMismatch {
                            name: format!("apoc.map.{name}"),
                            details: format!(
                                "expected a list of strings, got element {}",
                                other.type_name()
                            ),
                        })
                    }
                }
            }
            Ok(out)
        }
        other => Err(ApocError::TypeMismatch {
            name: format!("apoc.map.{name}"),
            details: format!("expected a list, got {}", other.type_name()),
        }),
    }
}

// ---------------------------------------------------------------
// merge / mergeList.
// ---------------------------------------------------------------

fn merge(args: &[Property]) -> Result<Property, ApocError> {
    check_arity("merge", args, 2)?;
    let Some(first) = as_map("merge", &args[0])? else {
        return Ok(Property::Null);
    };
    let Some(second) = as_map("merge", &args[1])? else {
        return Ok(Property::Null);
    };
    // Right-hand wins on key collision — matches Neo4j APOC and
    // standard "merge" semantics elsewhere (e.g. Python's `dict.update`).
    let mut out = first.clone();
    for (k, v) in second {
        out.insert(k.clone(), v.clone());
    }
    Ok(Property::Map(out))
}

fn merge_list(args: &[Property]) -> Result<Property, ApocError> {
    check_arity("mergeList", args, 1)?;
    let items = match &args[0] {
        Property::Null => return Ok(Property::Null),
        Property::List(items) => items,
        other => {
            return Err(ApocError::TypeMismatch {
                name: "apoc.map.mergeList".into(),
                details: format!("expected a list of maps, got {}", other.type_name()),
            })
        }
    };
    let mut out: HashMap<String, Property> = HashMap::new();
    for (i, item) in items.iter().enumerate() {
        match item {
            Property::Null => {
                // Skip null elements — a list element that is itself
                // null contributes nothing to the merge.
            }
            Property::Map(m) => {
                for (k, v) in m {
                    out.insert(k.clone(), v.clone());
                }
            }
            other => {
                return Err(ApocError::TypeMismatch {
                    name: "apoc.map.mergeList".into(),
                    details: format!("element {i} is {} (expected map)", other.type_name()),
                })
            }
        }
    }
    Ok(Property::Map(out))
}

// ---------------------------------------------------------------
// fromPairs / fromLists / fromValues.
// ---------------------------------------------------------------

fn from_pairs(args: &[Property]) -> Result<Property, ApocError> {
    check_arity("fromPairs", args, 1)?;
    let pairs = match &args[0] {
        Property::Null => return Ok(Property::Null),
        Property::List(items) => items,
        other => {
            return Err(ApocError::TypeMismatch {
                name: "apoc.map.fromPairs".into(),
                details: format!("expected a list of [k, v] pairs, got {}", other.type_name()),
            })
        }
    };
    let mut out: HashMap<String, Property> = HashMap::new();
    for (i, pair) in pairs.iter().enumerate() {
        let Property::List(kv) = pair else {
            return Err(ApocError::TypeMismatch {
                name: "apoc.map.fromPairs".into(),
                details: format!("pair {i} is not a list"),
            });
        };
        if kv.len() != 2 {
            return Err(ApocError::TypeMismatch {
                name: "apoc.map.fromPairs".into(),
                details: format!("pair {i} has {} elements (expected 2)", kv.len()),
            });
        }
        let key = expect_string("fromPairs", &kv[0])?.to_string();
        out.insert(key, kv[1].clone());
    }
    Ok(Property::Map(out))
}

fn from_lists(args: &[Property]) -> Result<Property, ApocError> {
    check_arity("fromLists", args, 2)?;
    let keys = match &args[0] {
        Property::Null => return Ok(Property::Null),
        Property::List(items) => items,
        other => {
            return Err(ApocError::TypeMismatch {
                name: "apoc.map.fromLists".into(),
                details: format!("expected a key list, got {}", other.type_name()),
            })
        }
    };
    let vals = match &args[1] {
        Property::Null => return Ok(Property::Null),
        Property::List(items) => items,
        other => {
            return Err(ApocError::TypeMismatch {
                name: "apoc.map.fromLists".into(),
                details: format!("expected a value list, got {}", other.type_name()),
            })
        }
    };
    if keys.len() != vals.len() {
        return Err(ApocError::TypeMismatch {
            name: "apoc.map.fromLists".into(),
            details: format!(
                "key list length {} != value list length {}",
                keys.len(),
                vals.len()
            ),
        });
    }
    let mut out: HashMap<String, Property> = HashMap::with_capacity(keys.len());
    for (k, v) in keys.iter().zip(vals.iter()) {
        let key = expect_string("fromLists", k)?.to_string();
        out.insert(key, v.clone());
    }
    Ok(Property::Map(out))
}

/// Build a map from a flat, alternating `[k1, v1, k2, v2, ...]`
/// list. Odd-length input is a caller bug and surfaces as
/// `TypeMismatch` rather than silently dropping the trailing key.
fn from_values(args: &[Property]) -> Result<Property, ApocError> {
    check_arity("fromValues", args, 1)?;
    let items = match &args[0] {
        Property::Null => return Ok(Property::Null),
        Property::List(items) => items,
        other => {
            return Err(ApocError::TypeMismatch {
                name: "apoc.map.fromValues".into(),
                details: format!("expected a flat k/v list, got {}", other.type_name()),
            })
        }
    };
    if items.len() % 2 != 0 {
        return Err(ApocError::TypeMismatch {
            name: "apoc.map.fromValues".into(),
            details: format!("list has {} elements (need even count)", items.len()),
        });
    }
    let mut out: HashMap<String, Property> = HashMap::with_capacity(items.len() / 2);
    let mut iter = items.chunks_exact(2);
    for chunk in &mut iter {
        let key = expect_string("fromValues", &chunk[0])?.to_string();
        out.insert(key, chunk[1].clone());
    }
    Ok(Property::Map(out))
}

// ---------------------------------------------------------------
// setKey / removeKey / removeKeys.
// ---------------------------------------------------------------

fn set_key(args: &[Property]) -> Result<Property, ApocError> {
    check_arity("setKey", args, 3)?;
    let Some(m) = as_map("setKey", &args[0])? else {
        return Ok(Property::Null);
    };
    let key = expect_string("setKey", &args[1])?.to_string();
    let mut out = m.clone();
    out.insert(key, args[2].clone());
    Ok(Property::Map(out))
}

fn remove_key(args: &[Property]) -> Result<Property, ApocError> {
    check_arity("removeKey", args, 2)?;
    let Some(m) = as_map("removeKey", &args[0])? else {
        return Ok(Property::Null);
    };
    let key = expect_string("removeKey", &args[1])?;
    let mut out = m.clone();
    out.remove(key);
    Ok(Property::Map(out))
}

fn remove_keys(args: &[Property]) -> Result<Property, ApocError> {
    check_arity("removeKeys", args, 2)?;
    let Some(m) = as_map("removeKeys", &args[0])? else {
        return Ok(Property::Null);
    };
    let keys = as_string_list("removeKeys", &args[1])?;
    let mut out = m.clone();
    for k in &keys {
        out.remove(k);
    }
    Ok(Property::Map(out))
}

// ---------------------------------------------------------------
// values / submap.
// ---------------------------------------------------------------

/// Project the values for `keys` in the order given. Missing keys
/// render as `Property::Null` — this mirrors Neo4j APOC's behaviour
/// of returning a fixed-shape list driven by `keys.length`.
fn values(args: &[Property]) -> Result<Property, ApocError> {
    check_arity("values", args, 2)?;
    let Some(m) = as_map("values", &args[0])? else {
        return Ok(Property::Null);
    };
    let keys = as_string_list("values", &args[1])?;
    let out: Vec<Property> = keys
        .iter()
        .map(|k| m.get(k).cloned().unwrap_or(Property::Null))
        .collect();
    Ok(Property::List(out))
}

/// Return a new map containing only the `keys` that actually exist
/// in the input. Missing keys are silently skipped — this is the
/// projection helper, so "not there" shouldn't materialize as
/// `key: null` in the output.
fn submap(args: &[Property]) -> Result<Property, ApocError> {
    check_arity("submap", args, 2)?;
    let Some(m) = as_map("submap", &args[0])? else {
        return Ok(Property::Null);
    };
    let keys = as_string_list("submap", &args[1])?;
    let mut out: HashMap<String, Property> = HashMap::with_capacity(keys.len());
    for k in keys {
        if let Some(v) = m.get(&k) {
            out.insert(k, v.clone());
        }
    }
    Ok(Property::Map(out))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &str) -> Property {
        Property::String(v.into())
    }
    fn i(n: i64) -> Property {
        Property::Int64(n)
    }
    fn m(pairs: &[(&str, Property)]) -> Property {
        let mut map: HashMap<String, Property> = HashMap::new();
        for (k, v) in pairs {
            map.insert((*k).to_string(), v.clone());
        }
        Property::Map(map)
    }
    fn lst(items: Vec<Property>) -> Property {
        Property::List(items)
    }

    fn map_of<'a>(p: &'a Property) -> &'a HashMap<String, Property> {
        match p {
            Property::Map(m) => m,
            _ => panic!("expected map"),
        }
    }

    #[test]
    fn merge_right_wins_on_conflict() {
        let out = merge(&[
            m(&[("a", i(1)), ("b", i(2))]),
            m(&[("b", i(9)), ("c", i(3))]),
        ])
        .unwrap();
        let got = map_of(&out);
        assert_eq!(got.get("a"), Some(&i(1)));
        assert_eq!(got.get("b"), Some(&i(9)));
        assert_eq!(got.get("c"), Some(&i(3)));
    }

    #[test]
    fn merge_null_returns_null() {
        assert_eq!(merge(&[Property::Null, m(&[])]).unwrap(), Property::Null);
    }

    #[test]
    fn from_pairs_builds_map() {
        let out =
            from_pairs(&[lst(vec![lst(vec![s("a"), i(1)]), lst(vec![s("b"), i(2)])])]).unwrap();
        let got = map_of(&out);
        assert_eq!(got.get("a"), Some(&i(1)));
        assert_eq!(got.get("b"), Some(&i(2)));
    }

    #[test]
    fn from_pairs_bad_shape_errors() {
        let err = from_pairs(&[lst(vec![lst(vec![s("a")])])]).unwrap_err();
        assert!(matches!(err, ApocError::TypeMismatch { .. }));
    }

    #[test]
    fn from_lists_zips_parallel_lists() {
        let out = from_lists(&[lst(vec![s("x"), s("y")]), lst(vec![i(10), i(20)])]).unwrap();
        let got = map_of(&out);
        assert_eq!(got.get("x"), Some(&i(10)));
        assert_eq!(got.get("y"), Some(&i(20)));
    }

    #[test]
    fn from_lists_length_mismatch_errors() {
        let err = from_lists(&[lst(vec![s("a")]), lst(vec![i(1), i(2)])]).unwrap_err();
        assert!(matches!(err, ApocError::TypeMismatch { .. }));
    }

    #[test]
    fn from_values_flat_alternating_list() {
        let out = from_values(&[lst(vec![s("a"), i(1), s("b"), i(2)])]).unwrap();
        let got = map_of(&out);
        assert_eq!(got.get("a"), Some(&i(1)));
        assert_eq!(got.get("b"), Some(&i(2)));
    }

    #[test]
    fn from_values_odd_length_errors() {
        let err = from_values(&[lst(vec![s("a"), i(1), s("b")])]).unwrap_err();
        assert!(matches!(err, ApocError::TypeMismatch { .. }));
    }

    #[test]
    fn set_key_adds_or_overwrites() {
        let out = set_key(&[m(&[("a", i(1))]), s("b"), i(2)]).unwrap();
        let got = map_of(&out);
        assert_eq!(got.get("a"), Some(&i(1)));
        assert_eq!(got.get("b"), Some(&i(2)));
    }

    #[test]
    fn remove_key_drops_single() {
        let out = remove_key(&[m(&[("a", i(1)), ("b", i(2))]), s("a")]).unwrap();
        let got = map_of(&out);
        assert!(got.get("a").is_none());
        assert_eq!(got.get("b"), Some(&i(2)));
    }

    #[test]
    fn remove_keys_drops_all_listed() {
        let out = remove_keys(&[
            m(&[("a", i(1)), ("b", i(2)), ("c", i(3))]),
            lst(vec![s("a"), s("c")]),
        ])
        .unwrap();
        let got = map_of(&out);
        assert!(got.get("a").is_none());
        assert_eq!(got.get("b"), Some(&i(2)));
        assert!(got.get("c").is_none());
    }

    #[test]
    fn values_projects_in_order_with_nulls_for_missing() {
        let out = values(&[
            m(&[("a", i(1)), ("c", i(3))]),
            lst(vec![s("a"), s("b"), s("c")]),
        ])
        .unwrap();
        assert_eq!(out, lst(vec![i(1), Property::Null, i(3)]));
    }

    #[test]
    fn submap_drops_missing_keys() {
        let out = submap(&[
            m(&[("a", i(1)), ("b", i(2))]),
            lst(vec![s("a"), s("missing")]),
        ])
        .unwrap();
        let got = map_of(&out);
        assert_eq!(got.len(), 1);
        assert_eq!(got.get("a"), Some(&i(1)));
    }

    #[test]
    fn merge_list_combines_many() {
        let out = merge_list(&[lst(vec![
            m(&[("a", i(1))]),
            m(&[("b", i(2))]),
            m(&[("a", i(9))]), // last wins
        ])])
        .unwrap();
        let got = map_of(&out);
        assert_eq!(got.get("a"), Some(&i(9)));
        assert_eq!(got.get("b"), Some(&i(2)));
    }

    #[test]
    fn merge_list_skips_null_elements() {
        let out = merge_list(&[lst(vec![
            m(&[("a", i(1))]),
            Property::Null,
            m(&[("b", i(2))]),
        ])])
        .unwrap();
        let got = map_of(&out);
        assert_eq!(got.len(), 2);
        assert_eq!(got.get("a"), Some(&i(1)));
        assert_eq!(got.get("b"), Some(&i(2)));
    }

    #[test]
    fn null_map_returns_null() {
        assert_eq!(
            set_key(&[Property::Null, s("k"), i(1)]).unwrap(),
            Property::Null,
        );
    }

    #[test]
    fn arity_errors_name_the_function() {
        let err = merge(&[]).unwrap_err();
        assert!(matches!(&err, ApocError::Arity { name, .. } if name == "apoc.map.merge"));
    }
}
