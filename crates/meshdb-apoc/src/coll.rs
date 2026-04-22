//! `apoc.coll.*` — collection operations.
//!
//! Matches the Neo4j APOC semantics for the subset implemented
//! here. Functions are null-propagating: any `Null` list argument
//! returns `Null` without inspecting the other arguments. Empty
//! lists get function-specific treatment (usually 0 for reductions
//! with an identity, `Null` otherwise — documented per function).
//!
//! Numeric helpers (`sum`, `avg`, `max`, `min`) follow Neo4j's
//! Integer/Float promotion: a mixed-type list returns `Float64` the
//! moment one `Float64` element appears; otherwise the result
//! stays `Int64`. Null elements inside the list are skipped (not
//! propagated).

use crate::ApocError;
use meshdb_core::Property;
use std::collections::HashSet;

pub fn call(suffix: &str, args: &[Property]) -> Result<Property, ApocError> {
    match suffix {
        "sum" => sum(args),
        "avg" => avg(args),
        "max" => max(args),
        "min" => min(args),
        "toset" => to_set(args),
        "sort" => sort(args, false),
        "sortdesc" => sort(args, true),
        "reverse" => reverse(args),
        "contains" => contains(args),
        "union" => union(args),
        "intersection" => intersection(args),
        "subtract" => subtract(args),
        "flatten" => flatten(args),
        "zip" => zip(args),
        "indexof" => index_of(args),
        "occurrences" => occurrences(args),
        "tomap" => to_map(args),
        _ => Err(ApocError::UnknownFunction(format!("apoc.coll.{suffix}"))),
    }
}

/// Enforce a single argument and return it for the list-taking
/// functions. Extracting this keeps the per-function prelude from
/// repeating the arity check + error name.
fn single<'a>(name: &str, args: &'a [Property]) -> Result<&'a Property, ApocError> {
    if args.len() != 1 {
        return Err(ApocError::Arity {
            name: format!("apoc.coll.{name}"),
            expected: 1,
            got: args.len(),
        });
    }
    Ok(&args[0])
}

/// Extract the element slice of a list-typed [`Property`]. Returns
/// `None` when the property is `Null` so the caller can short-
/// circuit the null-propagation.
fn as_list<'a>(name: &str, p: &'a Property) -> Result<Option<&'a [Property]>, ApocError> {
    match p {
        Property::Null => Ok(None),
        Property::List(items) => Ok(Some(items.as_slice())),
        other => Err(ApocError::TypeMismatch {
            name: format!("apoc.coll.{name}"),
            details: format!("expected a list, got {}", other.type_name()),
        }),
    }
}

fn sum(args: &[Property]) -> Result<Property, ApocError> {
    let Some(items) = as_list("sum", single("sum", args)?)? else {
        return Ok(Property::Null);
    };
    let mut int_total: i64 = 0;
    let mut float_total: f64 = 0.0;
    let mut saw_float = false;
    for item in items {
        match item {
            Property::Null => continue,
            Property::Int64(n) => {
                if saw_float {
                    float_total += *n as f64;
                } else {
                    int_total = int_total.saturating_add(*n);
                }
            }
            Property::Float64(f) => {
                if !saw_float {
                    saw_float = true;
                    float_total = int_total as f64;
                }
                float_total += f;
            }
            other => {
                return Err(ApocError::TypeMismatch {
                    name: "apoc.coll.sum".into(),
                    details: format!("non-numeric element {}", other.type_name()),
                })
            }
        }
    }
    Ok(if saw_float {
        Property::Float64(float_total)
    } else {
        Property::Int64(int_total)
    })
}

fn avg(args: &[Property]) -> Result<Property, ApocError> {
    let Some(items) = as_list("avg", single("avg", args)?)? else {
        return Ok(Property::Null);
    };
    let mut total: f64 = 0.0;
    let mut count: usize = 0;
    for item in items {
        match item {
            Property::Null => continue,
            Property::Int64(n) => {
                total += *n as f64;
                count += 1;
            }
            Property::Float64(f) => {
                total += *f;
                count += 1;
            }
            other => {
                return Err(ApocError::TypeMismatch {
                    name: "apoc.coll.avg".into(),
                    details: format!("non-numeric element {}", other.type_name()),
                })
            }
        }
    }
    if count == 0 {
        return Ok(Property::Null);
    }
    Ok(Property::Float64(total / count as f64))
}

/// Reduce over numeric items picking the max (`max=true`) or min
/// (`max=false`). Integer/Float promotion matches `sum`: returns
/// `Float64` the moment any input is Float, else `Int64`.
fn extremum(name: &str, args: &[Property], max: bool) -> Result<Property, ApocError> {
    let Some(items) = as_list(name, single(name, args)?)? else {
        return Ok(Property::Null);
    };
    let mut best: Option<f64> = None;
    let mut saw_float = false;
    for item in items {
        let v = match item {
            Property::Null => continue,
            Property::Int64(n) => *n as f64,
            Property::Float64(f) => {
                saw_float = true;
                *f
            }
            other => {
                return Err(ApocError::TypeMismatch {
                    name: format!("apoc.coll.{name}"),
                    details: format!("non-numeric element {}", other.type_name()),
                })
            }
        };
        best = Some(match best {
            None => v,
            Some(cur) => {
                if max {
                    if v > cur {
                        v
                    } else {
                        cur
                    }
                } else if v < cur {
                    v
                } else {
                    cur
                }
            }
        });
    }
    match best {
        None => Ok(Property::Null),
        Some(v) => Ok(if saw_float {
            Property::Float64(v)
        } else {
            Property::Int64(v as i64)
        }),
    }
}

fn max(args: &[Property]) -> Result<Property, ApocError> {
    extremum("max", args, true)
}

fn min(args: &[Property]) -> Result<Property, ApocError> {
    extremum("min", args, false)
}

/// Deduplicate while preserving the first-occurrence order of each
/// element. Neo4j's `apoc.coll.toSet` has the same semantics; a
/// plain `HashSet` wouldn't preserve order.
fn to_set(args: &[Property]) -> Result<Property, ApocError> {
    let Some(items) = as_list("toSet", single("toSet", args)?)? else {
        return Ok(Property::Null);
    };
    let mut seen: HashSet<PropertyKey> = HashSet::new();
    let mut out: Vec<Property> = Vec::with_capacity(items.len());
    for item in items {
        let key = PropertyKey::from(item);
        if seen.insert(key) {
            out.push(item.clone());
        }
    }
    Ok(Property::List(out))
}

fn sort(args: &[Property], descending: bool) -> Result<Property, ApocError> {
    let name = if descending { "sortDesc" } else { "sort" };
    let Some(items) = as_list(name, single(name, args)?)? else {
        return Ok(Property::Null);
    };
    // Numeric-only and string-only lists sort by native ordering;
    // null elements sink to the start (asc) / end (desc) to match
    // Neo4j. Mixed types error — openCypher doesn't define a total
    // order across types.
    let mut items = items.to_vec();
    let non_null: Vec<&Property> = items
        .iter()
        .filter(|p| !matches!(p, Property::Null))
        .collect();
    if let Some(first) = non_null.first() {
        let kind = first.type_name();
        for p in non_null.iter().skip(1) {
            if p.type_name() != kind {
                return Err(ApocError::TypeMismatch {
                    name: format!("apoc.coll.{name}"),
                    details: format!("mixed element types {kind} + {}", p.type_name()),
                });
            }
        }
    }
    items.sort_by(|a, b| compare(a, b));
    if descending {
        items.reverse();
    }
    Ok(Property::List(items))
}

fn reverse(args: &[Property]) -> Result<Property, ApocError> {
    let Some(items) = as_list("reverse", single("reverse", args)?)? else {
        return Ok(Property::Null);
    };
    let mut out = items.to_vec();
    out.reverse();
    Ok(Property::List(out))
}

fn contains(args: &[Property]) -> Result<Property, ApocError> {
    if args.len() != 2 {
        return Err(ApocError::Arity {
            name: "apoc.coll.contains".into(),
            expected: 2,
            got: args.len(),
        });
    }
    let Some(items) = as_list("contains", &args[0])? else {
        return Ok(Property::Null);
    };
    let needle = &args[1];
    let needle_key = PropertyKey::from(needle);
    let found = items.iter().any(|p| PropertyKey::from(p) == needle_key);
    Ok(Property::Bool(found))
}

/// Set-theoretic union: every element that appears in either list,
/// deduplicated, preserving the order of first appearance (items
/// from `a` first, then items from `b` not already seen).
fn union(args: &[Property]) -> Result<Property, ApocError> {
    two_lists("union", args, |a, b| {
        let mut seen: HashSet<PropertyKey> = HashSet::new();
        let mut out: Vec<Property> = Vec::new();
        for item in a.iter().chain(b.iter()) {
            let key = PropertyKey::from(item);
            if seen.insert(key) {
                out.push(item.clone());
            }
        }
        out
    })
}

/// Set-theoretic intersection: elements present in both lists,
/// deduplicated, in the order they appear in the first list.
fn intersection(args: &[Property]) -> Result<Property, ApocError> {
    two_lists("intersection", args, |a, b| {
        let b_keys: HashSet<PropertyKey> = b.iter().map(PropertyKey::from).collect();
        let mut seen: HashSet<PropertyKey> = HashSet::new();
        let mut out: Vec<Property> = Vec::new();
        for item in a {
            let key = PropertyKey::from(item);
            if b_keys.contains(&key) && seen.insert(key) {
                out.push(item.clone());
            }
        }
        out
    })
}

/// Set difference: elements of the first list not present in the
/// second, deduplicated, in first-list order.
fn subtract(args: &[Property]) -> Result<Property, ApocError> {
    two_lists("subtract", args, |a, b| {
        let b_keys: HashSet<PropertyKey> = b.iter().map(PropertyKey::from).collect();
        let mut seen: HashSet<PropertyKey> = HashSet::new();
        let mut out: Vec<Property> = Vec::new();
        for item in a {
            let key = PropertyKey::from(item);
            if !b_keys.contains(&key) && seen.insert(key) {
                out.push(item.clone());
            }
        }
        out
    })
}

fn two_lists(
    name: &str,
    args: &[Property],
    f: impl FnOnce(&[Property], &[Property]) -> Vec<Property>,
) -> Result<Property, ApocError> {
    if args.len() != 2 {
        return Err(ApocError::Arity {
            name: format!("apoc.coll.{name}"),
            expected: 2,
            got: args.len(),
        });
    }
    let a = match as_list(name, &args[0])? {
        Some(items) => items,
        None => return Ok(Property::Null),
    };
    let b = match as_list(name, &args[1])? {
        Some(items) => items,
        None => return Ok(Property::Null),
    };
    Ok(Property::List(f(a, b)))
}

/// One-level flatten: each list-typed element is spliced into the
/// output in place; non-list elements pass through. Matches Neo4j's
/// `apoc.coll.flatten` default (recursive flatten lives under a
/// different signature in the Neo4j library and is not implemented
/// here).
fn flatten(args: &[Property]) -> Result<Property, ApocError> {
    let Some(items) = as_list("flatten", single("flatten", args)?)? else {
        return Ok(Property::Null);
    };
    let mut out: Vec<Property> = Vec::new();
    for item in items {
        match item {
            Property::List(inner) => out.extend(inner.iter().cloned()),
            other => out.push(other.clone()),
        }
    }
    Ok(Property::List(out))
}

/// `apoc.coll.zip(list1, list2)` — pair elements positionally
/// into 2-element sublists, truncating to the shorter list.
/// Matches Neo4j APOC: no padding, no error on length mismatch.
/// Either list may be null → null out.
fn zip(args: &[Property]) -> Result<Property, ApocError> {
    if args.len() != 2 {
        return Err(ApocError::Arity {
            name: "apoc.coll.zip".into(),
            expected: 2,
            got: args.len(),
        });
    }
    let Some(a) = as_list("zip", &args[0])? else {
        return Ok(Property::Null);
    };
    let Some(b) = as_list("zip", &args[1])? else {
        return Ok(Property::Null);
    };
    let paired: Vec<Property> = a
        .iter()
        .zip(b.iter())
        .map(|(x, y)| Property::List(vec![x.clone(), y.clone()]))
        .collect();
    Ok(Property::List(paired))
}

/// `apoc.coll.indexOf(list, item)` — first 0-indexed position of
/// `item` in `list`, or `-1` if not present. Null list → null
/// result; null item matches explicit null elements.
fn index_of(args: &[Property]) -> Result<Property, ApocError> {
    if args.len() != 2 {
        return Err(ApocError::Arity {
            name: "apoc.coll.indexOf".into(),
            expected: 2,
            got: args.len(),
        });
    }
    let Some(items) = as_list("indexOf", &args[0])? else {
        return Ok(Property::Null);
    };
    for (i, item) in items.iter().enumerate() {
        if properties_equal(item, &args[1]) {
            return Ok(Property::Int64(i as i64));
        }
    }
    Ok(Property::Int64(-1))
}

/// `apoc.coll.occurrences(list, item)` — count of `item` in
/// `list`. Null list → null; null item counts explicit nulls.
fn occurrences(args: &[Property]) -> Result<Property, ApocError> {
    if args.len() != 2 {
        return Err(ApocError::Arity {
            name: "apoc.coll.occurrences".into(),
            expected: 2,
            got: args.len(),
        });
    }
    let Some(items) = as_list("occurrences", &args[0])? else {
        return Ok(Property::Null);
    };
    let count = items
        .iter()
        .filter(|p| properties_equal(p, &args[1]))
        .count();
    Ok(Property::Int64(count as i64))
}

/// `apoc.coll.toMap(list)` — convert a list of `[key, value]`
/// 2-element sublists into a map. Duplicate keys keep the last
/// value encountered (matches Neo4j). Non-pair elements raise
/// TypeMismatch; non-string keys likewise.
fn to_map(args: &[Property]) -> Result<Property, ApocError> {
    let Some(items) = as_list("toMap", single("toMap", args)?)? else {
        return Ok(Property::Null);
    };
    let mut map: std::collections::HashMap<String, Property> =
        std::collections::HashMap::with_capacity(items.len());
    for (i, item) in items.iter().enumerate() {
        let pair = match item {
            Property::List(inner) if inner.len() == 2 => inner,
            other => {
                return Err(ApocError::TypeMismatch {
                    name: "apoc.coll.toMap".into(),
                    details: format!(
                        "element {i} is not a 2-element list, got {}",
                        other.type_name()
                    ),
                });
            }
        };
        let key = match &pair[0] {
            Property::String(s) => s.clone(),
            other => {
                return Err(ApocError::TypeMismatch {
                    name: "apoc.coll.toMap".into(),
                    details: format!(
                        "element {i} key must be a string, got {}",
                        other.type_name()
                    ),
                });
            }
        };
        map.insert(key, pair[1].clone());
    }
    Ok(Property::Map(map))
}

/// Value equality for list-membership operations. Treats
/// `Int64` and `Float64` as comparable when numerically equal
/// (matches Neo4j's `=` semantics for mixed-numeric lists).
/// Nulls compare equal to each other here — `indexOf([1, null,
/// 3], null)` should find the explicit null at index 1.
fn properties_equal(a: &Property, b: &Property) -> bool {
    match (a, b) {
        (Property::Null, Property::Null) => true,
        (Property::Int64(x), Property::Int64(y)) => x == y,
        (Property::Float64(x), Property::Float64(y)) => x == y,
        (Property::Int64(i), Property::Float64(f)) | (Property::Float64(f), Property::Int64(i)) => {
            *f == (*i as f64)
        }
        _ => a == b,
    }
}

/// Hashable witness for equality across [`Property`] values.
/// `Property` doesn't implement `Hash` itself because `Float64` and
/// `Point`'s f64 fields break total ordering — we encode them into a
/// bit-level representation here so `HashSet` / equality checks have
/// a consistent key. Only used internally by the set-style
/// collection operations.
#[derive(Debug, PartialEq, Eq, Hash)]
enum PropertyKey {
    Null,
    String(String),
    Int64(i64),
    Float64Bits(u64),
    Bool(bool),
    Date(i64),
    LocalDateTime(i128),
    DateTime(i128, Option<i32>, Option<String>),
    Time(i64, Option<i32>),
    Duration(i64, i64, i64, i32),
    Point(i32, u64, u64, Option<u64>),
    List(Vec<PropertyKey>),
    /// Map keys go through a sorted representation so equality on
    /// two maps with the same entries in different insertion order
    /// still matches.
    Map(Vec<(String, PropertyKey)>),
}

impl From<&Property> for PropertyKey {
    fn from(p: &Property) -> Self {
        match p {
            Property::Null => PropertyKey::Null,
            Property::String(s) => PropertyKey::String(s.clone()),
            Property::Int64(n) => PropertyKey::Int64(*n),
            Property::Float64(f) => PropertyKey::Float64Bits(f.to_bits()),
            Property::Bool(b) => PropertyKey::Bool(*b),
            Property::Date(d) => PropertyKey::Date(*d),
            Property::LocalDateTime(n) => PropertyKey::LocalDateTime(*n),
            Property::DateTime {
                nanos,
                tz_offset_secs,
                tz_name,
            } => PropertyKey::DateTime(*nanos, *tz_offset_secs, tz_name.clone()),
            Property::Time {
                nanos,
                tz_offset_secs,
            } => PropertyKey::Time(*nanos, *tz_offset_secs),
            Property::Duration(d) => PropertyKey::Duration(d.months, d.days, d.seconds, d.nanos),
            Property::Point(p) => PropertyKey::Point(
                p.srid,
                p.x.to_bits(),
                p.y.to_bits(),
                p.z.map(|z| z.to_bits()),
            ),
            Property::List(items) => {
                PropertyKey::List(items.iter().map(PropertyKey::from).collect())
            }
            Property::Map(m) => {
                let mut pairs: Vec<(String, PropertyKey)> = m
                    .iter()
                    .map(|(k, v)| (k.clone(), PropertyKey::from(v)))
                    .collect();
                pairs.sort_by(|a, b| a.0.cmp(&b.0));
                PropertyKey::Map(pairs)
            }
        }
    }
}

/// Total order for `Property` sort. Numeric kinds compare by their
/// underlying value; strings use lexicographic byte order. Null
/// sinks to the start (Ordering::Less than any non-null). Mixed
/// non-null types don't reach this path — the caller pre-checks
/// that the input is homogeneous.
fn compare(a: &Property, b: &Property) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (a, b) {
        (Property::Null, Property::Null) => Ordering::Equal,
        (Property::Null, _) => Ordering::Less,
        (_, Property::Null) => Ordering::Greater,
        (Property::Int64(x), Property::Int64(y)) => x.cmp(y),
        (Property::Float64(x), Property::Float64(y)) => x.partial_cmp(y).unwrap_or(Ordering::Equal),
        (Property::Int64(x), Property::Float64(y)) => {
            (*x as f64).partial_cmp(y).unwrap_or(Ordering::Equal)
        }
        (Property::Float64(x), Property::Int64(y)) => {
            x.partial_cmp(&(*y as f64)).unwrap_or(Ordering::Equal)
        }
        (Property::String(x), Property::String(y)) => x.cmp(y),
        (Property::Bool(x), Property::Bool(y)) => x.cmp(y),
        // Fall through: identical non-null kinds the caller didn't
        // enumerate above get treated as equal. Shouldn't happen in
        // practice because `sort`'s type check rejects non-sortable
        // kinds up front.
        _ => Ordering::Equal,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lst(items: Vec<Property>) -> Property {
        Property::List(items)
    }
    fn i(n: i64) -> Property {
        Property::Int64(n)
    }
    fn f(x: f64) -> Property {
        Property::Float64(x)
    }
    fn s(s: &str) -> Property {
        Property::String(s.into())
    }

    #[test]
    fn sum_integer_list() {
        assert_eq!(sum(&[lst(vec![i(1), i(2), i(3)])]).unwrap(), i(6));
    }

    #[test]
    fn sum_mixed_promotes_to_float() {
        assert_eq!(sum(&[lst(vec![i(1), f(2.5)])]).unwrap(), f(3.5));
    }

    #[test]
    fn sum_skips_nulls() {
        assert_eq!(sum(&[lst(vec![i(1), Property::Null, i(2)])]).unwrap(), i(3));
    }

    #[test]
    fn sum_null_list_returns_null() {
        assert_eq!(sum(&[Property::Null]).unwrap(), Property::Null);
    }

    #[test]
    fn avg_returns_float() {
        assert_eq!(avg(&[lst(vec![i(1), i(2), i(3)])]).unwrap(), f(2.0));
    }

    #[test]
    fn avg_empty_returns_null() {
        assert_eq!(avg(&[lst(vec![])]).unwrap(), Property::Null);
    }

    #[test]
    fn max_picks_largest() {
        assert_eq!(max(&[lst(vec![i(1), i(5), i(3)])]).unwrap(), i(5));
    }

    #[test]
    fn min_picks_smallest_float() {
        assert_eq!(min(&[lst(vec![f(2.0), f(0.5), f(1.0)])]).unwrap(), f(0.5));
    }

    #[test]
    fn to_set_deduplicates_preserving_order() {
        assert_eq!(
            to_set(&[lst(vec![i(1), i(2), i(1), i(3), i(2)])]).unwrap(),
            lst(vec![i(1), i(2), i(3)]),
        );
    }

    #[test]
    fn sort_ascending_strings() {
        assert_eq!(
            sort(&[lst(vec![s("b"), s("a"), s("c")])], false).unwrap(),
            lst(vec![s("a"), s("b"), s("c")]),
        );
    }

    #[test]
    fn sort_descending_numbers() {
        assert_eq!(
            sort(&[lst(vec![i(1), i(3), i(2)])], true).unwrap(),
            lst(vec![i(3), i(2), i(1)]),
        );
    }

    #[test]
    fn sort_mixed_types_errors() {
        assert!(matches!(
            sort(&[lst(vec![i(1), s("x")])], false).unwrap_err(),
            ApocError::TypeMismatch { .. }
        ));
    }

    #[test]
    fn reverse_reverses_items() {
        assert_eq!(
            reverse(&[lst(vec![i(1), i(2), i(3)])]).unwrap(),
            lst(vec![i(3), i(2), i(1)]),
        );
    }

    #[test]
    fn contains_hits_and_misses() {
        assert_eq!(
            contains(&[lst(vec![i(1), i(2), i(3)]), i(2)]).unwrap(),
            Property::Bool(true),
        );
        assert_eq!(
            contains(&[lst(vec![i(1), i(2), i(3)]), i(9)]).unwrap(),
            Property::Bool(false),
        );
    }

    #[test]
    fn union_dedups_across_lists() {
        assert_eq!(
            union(&[lst(vec![i(1), i(2), i(3)]), lst(vec![i(3), i(4)])]).unwrap(),
            lst(vec![i(1), i(2), i(3), i(4)]),
        );
    }

    #[test]
    fn intersection_keeps_common_order_from_first() {
        assert_eq!(
            intersection(&[
                lst(vec![i(1), i(2), i(3), i(2)]),
                lst(vec![i(3), i(2), i(5)]),
            ])
            .unwrap(),
            lst(vec![i(2), i(3)]),
        );
    }

    #[test]
    fn subtract_removes_second_from_first() {
        assert_eq!(
            subtract(&[lst(vec![i(1), i(2), i(3), i(2)]), lst(vec![i(2)])]).unwrap(),
            lst(vec![i(1), i(3)]),
        );
    }

    #[test]
    fn flatten_splices_lists_one_level() {
        assert_eq!(
            flatten(&[lst(vec![
                lst(vec![i(1), i(2)]),
                lst(vec![i(3), i(4)]),
                i(5),
            ])])
            .unwrap(),
            lst(vec![i(1), i(2), i(3), i(4), i(5)]),
        );
    }

    #[test]
    fn arity_errors_name_the_function() {
        let err = sum(&[]).unwrap_err();
        assert!(
            matches!(&err, ApocError::Arity { name, .. } if name == "apoc.coll.sum"),
            "got {err:?}",
        );
    }

    #[test]
    fn non_numeric_element_in_sum_errors() {
        let err = sum(&[lst(vec![i(1), s("x")])]).unwrap_err();
        assert!(matches!(err, ApocError::TypeMismatch { .. }));
    }

    #[test]
    fn zip_pairs_items_truncating_to_shorter() {
        let out = zip(&[lst(vec![i(1), i(2), i(3)]), lst(vec![s("a"), s("b")])]).unwrap();
        assert_eq!(
            out,
            lst(vec![lst(vec![i(1), s("a")]), lst(vec![i(2), s("b")])]),
        );
    }

    #[test]
    fn zip_null_either_arg_is_null() {
        assert_eq!(
            zip(&[Property::Null, lst(vec![i(1)])]).unwrap(),
            Property::Null,
        );
        assert_eq!(
            zip(&[lst(vec![i(1)]), Property::Null]).unwrap(),
            Property::Null,
        );
    }

    #[test]
    fn index_of_finds_first_occurrence() {
        assert_eq!(
            index_of(&[lst(vec![i(10), i(20), i(30), i(20)]), i(20)]).unwrap(),
            i(1),
        );
    }

    #[test]
    fn index_of_missing_returns_neg_one() {
        assert_eq!(index_of(&[lst(vec![i(1), i(2)]), i(99)]).unwrap(), i(-1));
    }

    #[test]
    fn index_of_matches_explicit_null() {
        assert_eq!(
            index_of(&[lst(vec![i(1), Property::Null, i(3)]), Property::Null]).unwrap(),
            i(1),
        );
    }

    #[test]
    fn occurrences_counts_matches() {
        assert_eq!(
            occurrences(&[lst(vec![i(1), i(2), i(1), i(3), i(1)]), i(1)]).unwrap(),
            i(3),
        );
    }

    #[test]
    fn to_map_converts_pair_list() {
        let out = to_map(&[lst(vec![
            lst(vec![s("name"), s("alice")]),
            lst(vec![s("age"), i(30)]),
        ])])
        .unwrap();
        match out {
            Property::Map(m) => {
                assert_eq!(m.get("name"), Some(&s("alice")));
                assert_eq!(m.get("age"), Some(&i(30)));
            }
            other => panic!("expected Map, got {other:?}"),
        }
    }

    #[test]
    fn to_map_last_wins_on_duplicate_keys() {
        let out = to_map(&[lst(vec![lst(vec![s("k"), i(1)]), lst(vec![s("k"), i(2)])])]).unwrap();
        match out {
            Property::Map(m) => assert_eq!(m.get("k"), Some(&i(2))),
            other => panic!("expected Map, got {other:?}"),
        }
    }

    #[test]
    fn to_map_non_pair_element_errors() {
        let err = to_map(&[lst(vec![lst(vec![s("k"), i(1), i(2)])])]).unwrap_err();
        assert!(matches!(err, ApocError::TypeMismatch { .. }));
    }
}
