//! The Raft state machine core: applies committed log entries to the stamped [`LocalEngine`].
//!
//! This module holds everything about the state machine that does not need snapshot files: the
//! [`KvStateMachine`] (engine handle, last applied log id, last membership), the [`ApplySink`] seam
//! and the default [`InlineSink`]. The openraft `RaftStateMachine` trait impl (snapshot build and
//! install) lands in plan 03-12 on top of this core.
//!
//! # R-3 resolution: the apply seam
//!
//! Applying a committed entry is a lock-free, bounded-cost write into the engine. For now it runs
//! inline on the openraft state-machine task (chunked by openraft's apply batching). Phase 4 will
//! move apply onto a core-pinned worker; that swap happens by supplying a different [`ApplySink`]
//! to [`KvStateMachine::with_sink`], without touching the Raft glue.
//!
//! # No guards across awaits
//!
//! Nothing here is `async` and no epoch guard is ever created in this module. The engine's reads
//! pin the crossbeam epoch internally and drop the guard before returning. `crossbeam_epoch::Guard`
//! is not `Send`, so the compiler already forbids later async callers from holding one across an
//! await point (threat T-03-12).
//!
//! # R-1: stamped replay
//!
//! Every write is stamped with the Raft log index that produced it, and `*_at` skips an entry
//! whose index is at or below the key's stamp. That is what makes a fuzzy (non-atomic) snapshot
//! scan plus log replay equal to sequential apply; the tests at the bottom of this file are the
//! executable proof (decision D-09).

use std::sync::Arc;

use arc_swap::ArcSwap;
use openraft::entry::EntryPayload;
use openraft::type_config::alias::{EntryOf, LogIdOf, StoredMembershipOf};
use openraft::StoredMembership;

use crate::engine::{ApplyOutcome, LocalEngine};
use crate::raft::types::{KvRequest, KvResponse, TypeConfig};

/// Shared, swappable handle to the live engine. Readers keep their `Arc<LocalEngine>` across a
/// swap, so a restored engine can be installed without blocking them.
pub type EngineHandle = Arc<ArcSwap<LocalEngine>>;

/// Where a committed [`KvRequest`] is applied. The seam that lets Phase 4 replace inline apply
/// with a core-pinned worker handoff (R-3).
pub trait ApplySink: Send + Sync + 'static {
    /// Apply `req` as Raft log entry `index` against `engine`, returning the client response.
    fn apply(&self, engine: &LocalEngine, index: u64, req: &KvRequest) -> KvResponse;
}

/// The default sink: applies inline on the calling task using the engine's stamped `*_at` methods.
#[derive(Debug, Default, Clone, Copy)]
pub struct InlineSink;

impl ApplySink for InlineSink {
    fn apply(&self, engine: &LocalEngine, index: u64, req: &KvRequest) -> KvResponse {
        let outcome = match req {
            KvRequest::Set { key, value } => engine.set_at(index, key, value),
            KvRequest::Delete { key } => engine.delete_at(index, key),
            KvRequest::Cas { key, expected, desired } => {
                engine.cas_at(index, key, expected.as_deref(), desired.as_deref())
            }
        };
        match outcome {
            ApplyOutcome::Applied | ApplyOutcome::Skipped => KvResponse::Ok,
            ApplyOutcome::CasFailed { current } => KvResponse::CasFailed { current },
        }
    }
}

/// The Raft state machine: the engine plus the applied-log bookkeeping openraft needs.
pub struct KvStateMachine {
    engine: EngineHandle,
    sink: Arc<dyn ApplySink>,
    last_applied: Option<LogIdOf<TypeConfig>>,
    last_membership: StoredMembershipOf<TypeConfig>,
}

impl Default for KvStateMachine {
    fn default() -> Self {
        Self::new()
    }
}

impl KvStateMachine {
    /// A state machine with an empty engine and the [`InlineSink`].
    pub fn new() -> Self {
        Self::with_sink(Arc::new(InlineSink))
    }

    /// A state machine with an empty engine and a custom [`ApplySink`].
    pub fn with_sink(sink: Arc<dyn ApplySink>) -> Self {
        Self {
            engine: Arc::new(ArcSwap::from_pointee(LocalEngine::new())),
            sink,
            last_applied: None,
            last_membership: StoredMembership::default(),
        }
    }

    /// Clone out the engine handle (before the machine is moved into `Raft::new`) so the node can
    /// serve reads.
    pub fn engine_handle(&self) -> EngineHandle {
        Arc::clone(&self.engine)
    }

    /// The current engine.
    pub fn engine(&self) -> Arc<LocalEngine> {
        self.engine.load_full()
    }

    /// Install a restored engine together with the applied log id and membership it represents.
    /// Readers holding the previous engine keep it until they drop it.
    pub fn replace_engine(
        &mut self,
        engine: LocalEngine,
        applied: Option<LogIdOf<TypeConfig>>,
        membership: StoredMembershipOf<TypeConfig>,
    ) {
        self.engine.store(Arc::new(engine));
        self.last_applied = applied;
        self.last_membership = membership;
    }

    /// Apply one committed entry. Blank entries only advance the applied id; membership entries
    /// update the tracked membership; normal entries go through the [`ApplySink`] stamped with the
    /// entry's log index. `last_applied` always advances.
    pub fn apply_entry(&mut self, entry: &EntryOf<TypeConfig>) -> KvResponse {
        let response = match &entry.payload {
            EntryPayload::Blank => KvResponse::Ok,
            EntryPayload::Normal(req) => {
                let engine = self.engine.load_full();
                self.sink.apply(&engine, entry.log_id.index(), req)
            }
            EntryPayload::Membership(m) => {
                self.last_membership = StoredMembership::new(Some(entry.log_id.clone()), m.clone());
                KvResponse::Ok
            }
        };
        self.last_applied = Some(entry.log_id.clone());
        response
    }

    /// The last applied log id and the last membership seen.
    pub fn applied_state(&self) -> (Option<LogIdOf<TypeConfig>>, StoredMembershipOf<TypeConfig>) {
        (self.last_applied.clone(), self.last_membership.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use openraft::{BasicNode, Entry, LogId, Membership};
    use std::collections::BTreeSet;
    use std::sync::Mutex;

    fn b(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    fn log_id(index: u64) -> LogIdOf<TypeConfig> {
        LogId::new_term_index(1, index)
    }

    fn normal(index: u64, req: KvRequest) -> EntryOf<TypeConfig> {
        Entry { log_id: log_id(index), payload: EntryPayload::Normal(req) }
    }

    fn set(k: &str, v: &str) -> KvRequest {
        KvRequest::Set { key: b(k), value: b(v) }
    }

    #[test]
    fn apply_set_then_read_and_advance_applied() {
        let mut sm = KvStateMachine::new();
        let resp = sm.apply_entry(&normal(1, set("k", "v")));
        assert_eq!(resp, KvResponse::Ok);
        assert_eq!(sm.engine().get(b"k"), Some(b("v")));
        let (applied, _) = sm.applied_state();
        assert_eq!(applied.map(|l| l.index()), Some(1));
        // The pre-cloned handle sees the same engine.
        let handle = sm.engine_handle();
        assert_eq!(handle.load().get(b"k"), Some(b("v")));
    }

    #[test]
    fn apply_cas_failure_returns_cas_failed_with_current() {
        let mut sm = KvStateMachine::new();
        sm.apply_entry(&normal(1, set("k", "x")));
        let resp = sm.apply_entry(&normal(
            2,
            KvRequest::Cas { key: b("k"), expected: Some(b("y")), desired: Some(b("z")) },
        ));
        assert_eq!(resp, KvResponse::CasFailed { current: Some(b("x")) });
        assert_eq!(sm.engine().get(b"k"), Some(b("x")));
        let resp = sm.apply_entry(&normal(
            3,
            KvRequest::Cas { key: b("k"), expected: Some(b("x")), desired: Some(b("z")) },
        ));
        assert_eq!(resp, KvResponse::Ok);
        assert_eq!(sm.engine().get(b"k"), Some(b("z")));
    }

    #[test]
    fn apply_delete_removes_value() {
        let mut sm = KvStateMachine::new();
        sm.apply_entry(&normal(1, set("k", "v")));
        let resp = sm.apply_entry(&normal(2, KvRequest::Delete { key: b("k") }));
        assert_eq!(resp, KvResponse::Ok);
        assert_eq!(sm.engine().get(b"k"), None);
    }

    #[test]
    fn apply_membership_entry_updates_last_membership() {
        let mut sm = KvStateMachine::new();
        let m: Membership<u64, BasicNode> =
            Membership::new_with_defaults(vec![BTreeSet::from([1u64, 2])], [1u64, 2]);
        let entry: EntryOf<TypeConfig> =
            Entry { log_id: log_id(4), payload: EntryPayload::Membership(m.clone()) };
        let resp = sm.apply_entry(&entry);
        assert_eq!(resp, KvResponse::Ok);
        let (applied, membership) = sm.applied_state();
        assert_eq!(applied, Some(log_id(4)));
        assert_eq!(membership.log_id(), &Some(log_id(4)));
        assert_eq!(membership.membership(), &m);
    }

    #[test]
    fn apply_blank_entry_only_advances_applied() {
        let mut sm = KvStateMachine::new();
        let before = sm.applied_state().1;
        let entry: EntryOf<TypeConfig> = Entry { log_id: log_id(7), payload: EntryPayload::Blank };
        assert_eq!(sm.apply_entry(&entry), KvResponse::Ok);
        let (applied, membership) = sm.applied_state();
        assert_eq!(applied, Some(log_id(7)));
        assert_eq!(membership, before);
    }

    #[test]
    fn sink_is_swappable() {
        #[derive(Default)]
        struct Recorder {
            seen: Mutex<Vec<(u64, KvRequest)>>,
        }
        struct Shared(Arc<Recorder>);
        impl ApplySink for Shared {
            fn apply(&self, _engine: &LocalEngine, index: u64, req: &KvRequest) -> KvResponse {
                self.0.seen.lock().unwrap().push((index, req.clone()));
                KvResponse::Ok
            }
        }

        let rec = Arc::new(Recorder::default());
        let mut sm = KvStateMachine::with_sink(Arc::new(Shared(Arc::clone(&rec))));
        let reqs = vec![
            set("a", "1"),
            KvRequest::Delete { key: b("a") },
            KvRequest::Cas { key: b("b"), expected: None, desired: Some(b("2")) },
        ];
        for (i, r) in reqs.iter().enumerate() {
            assert_eq!(sm.apply_entry(&normal(i as u64 + 1, r.clone())), KvResponse::Ok);
        }
        let seen = rec.seen.lock().unwrap().clone();
        let expected: Vec<(u64, KvRequest)> =
            reqs.into_iter().enumerate().map(|(i, r)| (i as u64 + 1, r)).collect();
        assert_eq!(seen, expected);
        // The recording sink never touched the engine.
        assert_eq!(sm.engine().get(b"a"), None);
    }

    // ---------------------------------------------------------------------------------------
    // R-1 proof (decision D-09): a fuzzy stamped snapshot plus replay equals sequential apply.
    //
    // Constraint: only SINGLE-KEY operations are covered. Multi-key conditionals (FR-2 WATCH)
    // would need a consistent-cut snapshot and must reopen the snapshot design (assumption A12,
    // deferred).
    // ---------------------------------------------------------------------------------------

    use proptest::prelude::*;

    const KEYS: u8 = 4;
    const VALS: u8 = 3;

    /// Test-local operation over small integer keys and values.
    #[derive(Debug, Clone)]
    enum Op {
        Set(u8, u8),
        Delete(u8),
        Cas(u8, Option<u8>, Option<u8>),
    }

    fn key_bytes(k: u8) -> Vec<u8> {
        format!("key{k}").into_bytes()
    }

    fn val_bytes(v: u8) -> Vec<u8> {
        format!("val{v}").into_bytes()
    }

    /// Apply `op` as log entry `index` through the stamped engine API.
    fn apply_op(engine: &LocalEngine, index: u64, op: &Op) {
        match op {
            Op::Set(k, v) => {
                engine.set_at(index, &key_bytes(*k), &val_bytes(*v));
            }
            Op::Delete(k) => {
                engine.delete_at(index, &key_bytes(*k));
            }
            Op::Cas(k, expected, desired) => {
                let e = expected.map(val_bytes);
                let d = desired.map(val_bytes);
                engine.cas_at(index, &key_bytes(*k), e.as_deref(), d.as_deref());
            }
        }
    }

    /// Engine that applied the whole history sequentially with stamps 1..=len.
    fn sequential(history: &[Op]) -> LocalEngine {
        let engine = LocalEngine::new();
        for (i, op) in history.iter().enumerate() {
            apply_op(&engine, i as u64 + 1, op);
        }
        engine
    }

    /// Build a snapshot-restored engine the way a fuzzy snapshot would:
    ///
    /// 1. apply `history[0..a]` to a live engine (stamps 1..=a);
    /// 2. apply `extra_per_key[0]` more entries (writes racing the scan start);
    /// 3. scan the live engine with start index `a`, recording each observed entry and applying
    ///    `extra_per_key[i]` further entries after the i-th record (later keys are observed after
    ///    newer writes);
    /// 4. load every record into a fresh engine and replay `history[a..]` with stamps `a+1..`.
    ///
    /// `stamp_floor_zero` forces every restored stamp to 0 (the unstamped negative control).
    fn fuzzy_snapshot_replay(
        history: &[Op],
        a: usize,
        extra_per_key: &[u8],
        stamp_floor_zero: bool,
    ) -> LocalEngine {
        let a = a.min(history.len());
        let live = LocalEngine::new();
        let mut cursor = 0usize;
        let mut advance = |live: &LocalEngine, n: usize| {
            let end = (cursor + n).min(history.len());
            while cursor < end {
                apply_op(live, cursor as u64 + 1, &history[cursor]);
                cursor += 1;
            }
        };
        advance(&live, a);
        advance(&live, extra_per_key.first().copied().unwrap_or(0) as usize);

        let mut records = Vec::new();
        let mut seen = 0usize;
        live.scan_versioned(a as u64, |entry| {
            records.push(entry);
            seen += 1;
            let n = extra_per_key.get(seen).copied().unwrap_or(0) as usize;
            advance(&live, n);
        });

        let restored = LocalEngine::new();
        for r in &records {
            let stamp = if stamp_floor_zero { 0 } else { r.stamp };
            restored.load_record(&r.key, r.value.as_deref(), stamp);
        }
        for (i, op) in history.iter().enumerate().skip(a) {
            apply_op(&restored, i as u64 + 1, op);
        }
        restored
    }

    fn assert_same_state(left: &LocalEngine, right: &LocalEngine) {
        for k in 0..KEYS {
            assert_eq!(left.get(&key_bytes(k)), right.get(&key_bytes(k)), "key{k} differs");
        }
    }

    /// Fresh engine with `k = x` at stamp 0, mirroring an initial value from before the snapshot
    /// start index.
    fn engine_with_initial(k: &str, x: &str) -> LocalEngine {
        let e = LocalEngine::new();
        e.load_record(k.as_bytes(), Some(x.as_bytes()), 0);
        e
    }

    #[test]
    fn cas_counterexample() {
        // Initial k = x. Entry 1: CAS(k, y -> z) fails. Entry 2: CAS(k, x -> y) succeeds.
        let live = engine_with_initial("k", "x");
        assert!(matches!(
            live.cas_at(1, b"k", Some(b"y"), Some(b"z")),
            ApplyOutcome::CasFailed { .. }
        ));
        assert_eq!(live.cas_at(2, b"k", Some(b"x"), Some(b"y")), ApplyOutcome::Applied);

        // Fuzzy snapshot, start index 0, observes k = y (stamp 2).
        let mut records = Vec::new();
        live.scan_versioned(0, |e| records.push(e));
        let k = records.iter().find(|e| &e.key[..] == b"k").expect("k scanned");
        assert_eq!((k.value.as_deref(), k.stamp), (Some(&b"y"[..]), 2));

        let restored = LocalEngine::new();
        for r in &records {
            restored.load_record(&r.key, r.value.as_deref(), r.stamp);
        }
        // Replay from index 1.
        restored.cas_at(1, b"k", Some(b"y"), Some(b"z"));
        restored.cas_at(2, b"k", Some(b"x"), Some(b"y"));
        assert_eq!(restored.get(b"k"), Some(b("y")));
        assert_eq!(restored.get(b"k"), live.get(b"k"));
    }

    #[test]
    fn tombstone_newer_than_snapshot_is_kept() {
        // k = x. Entry 1: CAS(k, absent -> q) fails. Entry 2: CAS(k, x -> delete) succeeds.
        let live = engine_with_initial("k", "x");
        assert!(matches!(
            live.cas_at(1, b"k", None, Some(b"q")),
            ApplyOutcome::CasFailed { .. }
        ));
        assert_eq!(live.cas_at(2, b"k", Some(b"x"), None), ApplyOutcome::Applied);

        let mut records = Vec::new();
        live.scan_versioned(0, |e| records.push(e));
        let t = records.iter().find(|e| &e.key[..] == b"k").expect("tombstone must be yielded");
        assert_eq!((t.value.as_ref(), t.stamp), (None, 2));

        let restored = LocalEngine::new();
        for r in &records {
            restored.load_record(&r.key, r.value.as_deref(), r.stamp);
        }
        restored.cas_at(1, b"k", None, Some(b"q"));
        restored.cas_at(2, b"k", Some(b"x"), None);
        assert_eq!(restored.get(b"k"), None);
    }

    #[test]
    fn tombstone_at_or_below_start_is_dropped() {
        let history = vec![Op::Set(0, 1), Op::Delete(0), Op::Set(0, 2), Op::Delete(0), Op::Set(1, 0)];
        // Start index 4: the tombstone has stamp 4, which is at or below the start index.
        let live = LocalEngine::new();
        for (i, op) in history.iter().take(4).enumerate() {
            apply_op(&live, i as u64 + 1, op);
        }
        let mut records = Vec::new();
        live.scan_versioned(4, |e| records.push(e));
        assert!(records.is_empty(), "tombstone at stamp 4 must be dropped, got {records:?}");

        let restored = fuzzy_snapshot_replay(&history, 4, &[], false);
        assert_same_state(&restored, &sequential(&history));
    }

    #[test]
    fn naive_replay_diverges_on_counterexample() {
        // Same snapshot as cas_counterexample, but every restored stamp forced to 0.
        let live = engine_with_initial("k", "x");
        live.cas_at(1, b"k", Some(b"y"), Some(b"z"));
        live.cas_at(2, b"k", Some(b"x"), Some(b"y"));
        let mut records = Vec::new();
        live.scan_versioned(0, |e| records.push(e));

        let naive = LocalEngine::new();
        for r in &records {
            naive.load_record(&r.key, r.value.as_deref(), 0);
        }
        naive.cas_at(1, b"k", Some(b"y"), Some(b"z"));
        naive.cas_at(2, b"k", Some(b"x"), Some(b"y"));
        assert_eq!(naive.get(b"k"), Some(b("z")), "naive replay must diverge");
        assert_ne!(naive.get(b"k"), live.get(b"k"));
    }

    #[test]
    fn naive_replay_diverges_in_generated_helper() {
        // Same shape through the property helper: unstamped restore must lose to stamped restore.
        // history: k0 = 0 (idx 1); CAS(k0, 1 -> 2) fails (idx 2); CAS(k0, 0 -> 1) succeeds (idx 3).
        let history = vec![
            Op::Set(0, 0),
            Op::Cas(0, Some(1), Some(2)),
            Op::Cas(0, Some(0), Some(1)),
        ];
        // a = 1; both CAS entries race the scan start (extra_per_key[0] = 2).
        let stamped = fuzzy_snapshot_replay(&history, 1, &[2], false);
        let naive = fuzzy_snapshot_replay(&history, 1, &[2], true);
        let seq = sequential(&history);
        assert_same_state(&stamped, &seq);
        assert_eq!(seq.get(&key_bytes(0)), Some(Bytes::from(val_bytes(1))));
        assert_eq!(naive.get(&key_bytes(0)), Some(Bytes::from(val_bytes(2))));
    }

    fn op_strategy() -> impl Strategy<Value = Op> {
        let key = 0..KEYS;
        let val = 0..VALS;
        // Bias CAS expectations toward present values so both successful and failed CAS occur.
        let opt_val = prop_oneof![1 => Just(None), 4 => (0..VALS).prop_map(Some)];
        prop_oneof![
            3 => (key.clone(), val.clone()).prop_map(|(k, v)| Op::Set(k, v)),
            1 => key.clone().prop_map(Op::Delete),
            4 => (key, opt_val.clone(), opt_val).prop_map(|(k, e, d)| Op::Cas(k, e, d)),
        ]
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 3000,
            failure_persistence: None,
            ..ProptestConfig::default()
        })]

        #[test]
        fn fuzzy_replay_model(
            history in prop::collection::vec(op_strategy(), 1..60),
            a_seed in any::<usize>(),
            extra in prop::collection::vec(0u8..6, (KEYS as usize) + 1),
        ) {
            let a = a_seed % (history.len() + 1);
            let restored = fuzzy_snapshot_replay(&history, a, &extra, false);
            let seq = sequential(&history);
            for k in 0..KEYS {
                prop_assert_eq!(
                    restored.get(&key_bytes(k)),
                    seq.get(&key_bytes(k)),
                    "key{} differs (a={}, extra={:?}, history={:?})", k, a, extra, history
                );
            }
        }
    }
}
