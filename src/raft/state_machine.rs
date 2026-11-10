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
}
