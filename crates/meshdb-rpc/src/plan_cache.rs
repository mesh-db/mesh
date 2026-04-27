//! In-memory cache of parsed + planned Cypher queries.
//!
//! The typical Bolt session pattern is "run the same parametrised
//! query thousands of times" — `MATCH (u:User {id: $id}) RETURN u`,
//! issued with a different `$id` per call. Without a cache, each
//! run pays the parse + plan cost on top of execution; with one,
//! the second run onwards skips both phases and goes straight to
//! the executor.
//!
//! ## Design
//!
//! * Bounded LRU keyed on the verbatim query string. Parameters
//!   never enter the key — they're applied during execution, not
//!   planning.
//! * Every cache entry stamps a `schema_fingerprint` — a hash of
//!   the planner-relevant catalog state (property indexes,
//!   edge-property indexes, point indexes) at the moment the plan
//!   was built. On lookup the caller passes the *current*
//!   fingerprint; a mismatch evicts the entry and forces a
//!   re-plan. This works on every peer regardless of how the
//!   schema change arrived (initiator vs. Raft replica vs.
//!   routing fan-out) because the fingerprint is derived purely
//!   from what's visible in the local store.
//! * Cache size + LRU policy are intentionally simple (a
//!   `HashMap` + per-entry `Instant` for last-touch). The volume
//!   is low enough that a real LRU crate is overkill.

use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use meshdb_cypher::LogicalPlan;

/// Default maximum entries in the plan cache. 1024 covers the
/// distinct-query-string set of any realistic application
/// without breaking memory budgets — even at ~10KB per plan
/// that's ~10MB resident.
pub const DEFAULT_PLAN_CACHE_CAPACITY: usize = 1024;

#[derive(Debug)]
struct CacheEntry {
    /// Plan as of `schema_fingerprint`. Wrapped in Arc so a
    /// cache hit returns a cheap clone.
    plan: Arc<LogicalPlan>,
    schema_fingerprint: u64,
    last_touched: Instant,
}

/// Plan cache. Cheap to clone (the inner state is Arc/Mutex'd)
/// so MeshService can hand out clones to spawned tasks without
/// locking.
#[derive(Debug, Clone)]
pub struct PlanCache {
    inner: Arc<PlanCacheInner>,
}

#[derive(Debug)]
struct PlanCacheInner {
    capacity: usize,
    entries: Mutex<HashMap<String, CacheEntry>>,
}

impl PlanCache {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "plan cache capacity must be > 0");
        Self {
            inner: Arc::new(PlanCacheInner {
                capacity,
                entries: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// Lookup. Returns the cached plan when present and at the
    /// supplied schema fingerprint; otherwise `None`. A
    /// fingerprint mismatch evicts the entry so the caller's
    /// re-plan can take its place.
    pub fn get(&self, query: &str, schema_fingerprint: u64) -> Option<Arc<LogicalPlan>> {
        let mut entries = self.inner.entries.lock().expect("plan cache poisoned");
        let entry = entries.get_mut(query)?;
        if entry.schema_fingerprint != schema_fingerprint {
            entries.remove(query);
            return None;
        }
        entry.last_touched = Instant::now();
        Some(entry.plan.clone())
    }

    /// Insert. If the cache is at capacity, evicts the
    /// least-recently-touched entry.
    pub fn insert(&self, query: String, plan: Arc<LogicalPlan>, schema_fingerprint: u64) {
        let mut entries = self.inner.entries.lock().expect("plan cache poisoned");
        if entries.len() >= self.inner.capacity && !entries.contains_key(&query) {
            if let Some(victim) = entries
                .iter()
                .min_by_key(|(_, e)| e.last_touched)
                .map(|(k, _)| k.clone())
            {
                entries.remove(&victim);
            }
        }
        entries.insert(
            query,
            CacheEntry {
                plan,
                schema_fingerprint,
                last_touched: Instant::now(),
            },
        );
    }

    pub fn len(&self) -> usize {
        self.inner
            .entries
            .lock()
            .expect("plan cache poisoned")
            .len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Fingerprint a planner-relevant slice of the catalog. Order
/// the inputs so the hash is deterministic across runs even when
/// the underlying storage iterator yields a different order.
pub fn fingerprint_schema(
    property_indexes: &[(String, Vec<String>)],
    edge_property_indexes: &[(String, Vec<String>)],
    point_indexes: &[(String, String)],
    edge_point_indexes: &[(String, String)],
) -> u64 {
    let mut p = property_indexes.to_vec();
    p.sort();
    let mut ep = edge_property_indexes.to_vec();
    ep.sort();
    let mut pt = point_indexes.to_vec();
    pt.sort();
    let mut ept = edge_point_indexes.to_vec();
    ept.sort();
    let mut hasher = DefaultHasher::new();
    p.hash(&mut hasher);
    ep.hash(&mut hasher);
    pt.hash(&mut hasher);
    ept.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dummy_plan(_label: &'static str) -> Arc<LogicalPlan> {
        // The simplest LogicalPlan we can build in tests. The
        // cache treats it as opaque, so any variant works.
        Arc::new(LogicalPlan::NodeScanAll { var: "n".into() })
    }

    #[test]
    fn round_trips_one_entry() {
        let cache = PlanCache::new(8);
        let fp = 42;
        assert!(cache.get("MATCH (n) RETURN n", fp).is_none());
        cache.insert("MATCH (n) RETURN n".into(), dummy_plan("A"), fp);
        let got = cache.get("MATCH (n) RETURN n", fp).expect("hit");
        assert!(matches!(got.as_ref(), LogicalPlan::NodeScanAll { .. }));
    }

    #[test]
    fn schema_fingerprint_mismatch_evicts_on_lookup() {
        let cache = PlanCache::new(8);
        cache.insert("q".into(), dummy_plan("A"), 1);
        assert!(cache.get("q", 1).is_some());
        // Schema changed → fingerprint changed → cache miss.
        assert!(
            cache.get("q", 2).is_none(),
            "fingerprint mismatch should evict"
        );
        // The stale entry is gone now too.
        assert!(cache.get("q", 1).is_none(), "stale entry should be cleared");
    }

    #[test]
    fn lru_eviction_drops_oldest_when_at_capacity() {
        let cache = PlanCache::new(2);
        let fp = 1;
        cache.insert("a".into(), dummy_plan("a"), fp);
        std::thread::sleep(std::time::Duration::from_millis(2));
        cache.insert("b".into(), dummy_plan("b"), fp);
        std::thread::sleep(std::time::Duration::from_millis(2));
        cache.get("a", fp);
        std::thread::sleep(std::time::Duration::from_millis(2));
        cache.insert("c".into(), dummy_plan("c"), fp);
        assert!(cache.get("a", fp).is_some(), "touched entry must survive");
        assert!(cache.get("b", fp).is_none(), "oldest entry must be evicted");
        assert!(
            cache.get("c", fp).is_some(),
            "newly inserted entry survives"
        );
    }

    #[test]
    fn fingerprint_is_deterministic_across_orderings() {
        let p_a = vec![("L".to_string(), vec!["x".to_string()])];
        let p_b: Vec<(String, Vec<String>)> = Vec::new();
        let pt_a: Vec<(String, String)> = Vec::new();
        let pt_b: Vec<(String, String)> = Vec::new();
        let fp1 = fingerprint_schema(&p_a, &p_b, &pt_a, &pt_b);
        let fp2 = fingerprint_schema(&p_a, &p_b, &pt_a, &pt_b);
        assert_eq!(fp1, fp2);

        let p_c = vec![
            ("Z".to_string(), vec!["c".to_string()]),
            ("A".to_string(), vec!["b".to_string()]),
        ];
        let p_d = vec![
            ("A".to_string(), vec!["b".to_string()]),
            ("Z".to_string(), vec!["c".to_string()]),
        ];
        // Same set, different iter order → same fingerprint.
        assert_eq!(
            fingerprint_schema(&p_c, &p_b, &pt_a, &pt_b),
            fingerprint_schema(&p_d, &p_b, &pt_a, &pt_b),
        );
    }
}
