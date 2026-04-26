//! Idempotency cache for `MeshWrite::ForwardWrite`.
//!
//! When a non-leader peer proxies a write to the partition leader,
//! the proxy hop is one extra TCP round-trip that can succeed
//! end-to-end on the leader and then have its response lost on the
//! way back (TCP RST, leader steps down between commit and ack,
//! whatever). Without dedupe, the caller's one-shot retry would
//! re-propose the same commands and double-apply them — every
//! `CREATE` becomes two creates, every counter increment doubles.
//!
//! The cache shipped here is bounded + TTL'd. Each entry is keyed by
//! a 16-byte client-generated UUID and stores the leader's most
//! recent response. A retry with the same key returns the cached
//! response unchanged, which is correct because the entry only
//! exists if the original propose succeeded (we don't cache failures
//! beyond the leader-hint redirects, which are themselves
//! idempotent — re-issuing them is fine).
//!
//! Sizing: 1024 entries × ~50 bytes ≈ 50 KB, comfortably in noise
//! against any production rocksdb footprint. TTL of 60 seconds
//! covers the vast majority of retry windows; longer windows just
//! re-propose, which is the existing behaviour.

use crate::proto::ForwardWriteResponse;
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

const DEFAULT_MAX_ENTRIES: usize = 1024;
const DEFAULT_TTL: Duration = Duration::from_secs(60);

/// Process-wide cache of recently-seen ForwardWrite idempotency
/// keys. Created once on `MeshService::new` and shared across every
/// clone via `Arc`.
pub struct IdempotencyCache {
    entries: Mutex<HashMap<Vec<u8>, (Instant, ForwardWriteResponse)>>,
    max_entries: usize,
    ttl: Duration,
}

impl IdempotencyCache {
    pub fn new() -> Self {
        Self::with_capacity_and_ttl(DEFAULT_MAX_ENTRIES, DEFAULT_TTL)
    }

    pub fn with_capacity_and_ttl(max_entries: usize, ttl: Duration) -> Self {
        Self {
            entries: Mutex::new(HashMap::with_capacity(max_entries.min(64))),
            max_entries,
            ttl,
        }
    }

    /// Look up a cached response for `key`. Returns `None` if absent
    /// or expired. Expired entries are evicted on access.
    pub fn get(&self, key: &[u8]) -> Option<ForwardWriteResponse> {
        if key.is_empty() {
            return None;
        }
        let mut guard = self.entries.lock().expect("idempotency cache poisoned");
        if let Some((stamped_at, _)) = guard.get(key) {
            if stamped_at.elapsed() > self.ttl {
                guard.remove(key);
                return None;
            }
        }
        guard.get(key).map(|(_, resp)| resp.clone())
    }

    /// Store `resp` for `key`. Evicts the oldest entries when the
    /// table is at capacity. Empty keys are not cached — callers
    /// that pass an empty key are opting out of dedupe entirely.
    pub fn put(&self, key: Vec<u8>, resp: ForwardWriteResponse) {
        if key.is_empty() {
            return;
        }
        let mut guard = self.entries.lock().expect("idempotency cache poisoned");
        if guard.len() >= self.max_entries {
            // Opportunistic TTL sweep first; if that doesn't free
            // space, evict the entry with the oldest insertion stamp.
            let now = Instant::now();
            guard.retain(|_, (stamp, _)| now.duration_since(*stamp) <= self.ttl);
            if guard.len() >= self.max_entries {
                if let Some(oldest_key) = guard
                    .iter()
                    .min_by_key(|(_, (stamp, _))| *stamp)
                    .map(|(k, _)| k.clone())
                {
                    guard.remove(&oldest_key);
                }
            }
        }
        guard.insert(key, (Instant::now(), resp));
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.entries.lock().unwrap().len()
    }
}

impl Default for IdempotencyCache {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ok_response() -> ForwardWriteResponse {
        ForwardWriteResponse {
            ok: true,
            leader_hint: 0,
            error_message: String::new(),
        }
    }

    #[test]
    fn miss_on_unknown_key() {
        let c = IdempotencyCache::new();
        assert!(c.get(b"never-seen").is_none());
    }

    #[test]
    fn hit_after_put() {
        let c = IdempotencyCache::new();
        c.put(b"k1".to_vec(), ok_response());
        let cached = c.get(b"k1").unwrap();
        assert!(cached.ok);
    }

    #[test]
    fn empty_key_is_no_op_for_dedupe() {
        let c = IdempotencyCache::new();
        c.put(Vec::new(), ok_response());
        assert!(c.get(b"").is_none());
        assert_eq!(c.len(), 0);
    }

    #[test]
    fn ttl_expires_entry() {
        let c = IdempotencyCache::with_capacity_and_ttl(8, Duration::from_millis(20));
        c.put(b"k1".to_vec(), ok_response());
        std::thread::sleep(Duration::from_millis(40));
        assert!(c.get(b"k1").is_none());
    }

    #[test]
    fn capacity_evicts_oldest() {
        let c = IdempotencyCache::with_capacity_and_ttl(2, Duration::from_secs(60));
        c.put(b"first".to_vec(), ok_response());
        std::thread::sleep(Duration::from_millis(2));
        c.put(b"second".to_vec(), ok_response());
        std::thread::sleep(Duration::from_millis(2));
        c.put(b"third".to_vec(), ok_response());
        assert_eq!(c.len(), 2);
        assert!(c.get(b"first").is_none(), "oldest must be evicted");
        assert!(c.get(b"second").is_some());
        assert!(c.get(b"third").is_some());
    }
}
