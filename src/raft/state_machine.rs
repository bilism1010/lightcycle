//! The Raft state machine core: applies committed log entries to the stamped [`LocalEngine`].
//!
//! This module holds the [`KvStateMachine`] (engine handle, last applied log id, last membership),
//! the [`ApplySink`] seam and the default [`InlineSink`], plus (plan 03-12) the openraft
//! `RaftStateMachine` implementation that connects the core to the on-disk snapshot store:
//! chunked apply, fuzzy snapshot builds that run off the runtime, and whole-engine install.
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

use std::io;
use std::path::PathBuf;
use std::sync::Arc;

use arc_swap::ArcSwap;
use futures::{Stream, StreamExt};
use openraft::entry::EntryPayload;
use openraft::storage::{EntryResponder, RaftSnapshotBuilder, RaftStateMachine, Snapshot};
use openraft::type_config::alias::{
    EntryOf, LogIdOf, SnapshotMetaOf, SnapshotOf, StoredMembershipOf,
};
use openraft::{OptionalSend, StoredMembership};
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

use crate::engine::{ApplyOutcome, LocalEngine};
use crate::raft::types::{KvRequest, KvResponse, SnapshotFile, TypeConfig};
use crate::snapshot::{load_snapshot, Meta, SnapshotError, SnapshotStore};

/// Entries applied between cooperative yields to the scheduler (assumption A8). openraft hands
/// the state machine up to 4096 entries per `apply`, so this bounds how long one call can hold a
/// runtime thread (a yield every 256 entries gives at least 16 per full batch).
const APPLY_YIELD_EVERY: usize = 256;

/// Map a snapshot error to the `io::Error` openraft's storage traits speak.
fn snapshot_io_error(e: SnapshotError) -> io::Error {
    match e {
        SnapshotError::Io(e) => e,
        SnapshotError::CorruptNeedsWipe => io::Error::new(
            io::ErrorKind::InvalidData,
            "latest snapshot is corrupt and the log below it was purged: wipe this node's data \
             directory and rejoin the cluster",
        ),
        other => io::Error::new(io::ErrorKind::InvalidData, other),
    }
}

fn join_error(e: tokio::task::JoinError) -> io::Error {
    io::Error::other(format!("blocking snapshot task failed: {e}"))
}

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
    /// Snapshot directory handle; `None` for the in-memory constructors.
    snapshots: Option<Arc<SnapshotStore>>,
    /// Held for the whole duration of a snapshot build so builds never overlap (pitfall 11).
    build_lock: Arc<AsyncMutex<()>>,
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
            snapshots: None,
            build_lock: Arc::new(AsyncMutex::new(())),
        }
    }

    /// Open a state machine backed by the snapshot directory `snapshot_dir`. If a snapshot exists
    /// it is fully validated and loaded, and its log id and membership become the applied state;
    /// the caller replays any later log entries. A corrupt latest snapshot is a hard error.
    pub async fn open(snapshot_dir: impl Into<PathBuf>) -> Result<KvStateMachine, io::Error> {
        let dir = snapshot_dir.into();
        let (store, restored) = tokio::task::spawn_blocking(move || {
            let store = SnapshotStore::open(dir)?;
            let restored = match store.latest()? {
                Some((_, file)) => Some(load_snapshot(&file.path)?),
                None => None,
            };
            Ok::<_, SnapshotError>((store, restored))
        })
        .await
        .map_err(join_error)?
        .map_err(snapshot_io_error)?;

        let mut sm = Self::new();
        sm.snapshots = Some(Arc::new(store));
        if let Some((meta, engine)) = restored {
            sm.replace_engine(engine, meta.last_log_id, meta.last_membership);
        }
        Ok(sm)
    }

    /// Capture the snapshot start state: the current engine, `last_applied` and `last_membership`.
    /// Must be called serialized with apply (openraft calls the builder methods with `&mut self`
    /// between apply commands), so the three values are one consistent cut.
    fn snapshot_builder(&self, guard: Option<OwnedMutexGuard<()>>) -> KvSnapshotBuilder {
        KvSnapshotBuilder {
            store: self.snapshots.clone(),
            engine: self.engine.load_full(),
            meta: Meta {
                last_log_id: self.last_applied.clone(),
                last_membership: self.last_membership.clone(),
            },
            lock: Arc::clone(&self.build_lock),
            guard,
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

/// Builds one fuzzy snapshot from the engine and metadata captured when the builder was created.
pub struct KvSnapshotBuilder {
    store: Option<Arc<SnapshotStore>>,
    engine: Arc<LocalEngine>,
    meta: Meta,
    lock: Arc<AsyncMutex<()>>,
    /// Present when created by `try_create_snapshot_builder`; otherwise acquired in the build.
    guard: Option<OwnedMutexGuard<()>>,
}

impl RaftSnapshotBuilder<TypeConfig> for KvSnapshotBuilder {
    type SnapshotData = SnapshotFile;

    async fn build_snapshot(&mut self) -> Result<SnapshotOf<TypeConfig, SnapshotFile>, io::Error> {
        let store = self.store.clone().ok_or_else(|| {
            io::Error::new(io::ErrorKind::Unsupported, "snapshots are not configured")
        })?;
        if self.guard.is_none() {
            self.guard = Some(Arc::clone(&self.lock).lock_owned().await);
        }
        let engine = Arc::clone(&self.engine);
        let meta = self.meta.clone();
        // The scan is a read-only walk of the whole engine: keep it off the async runtime. The
        // store syncs the file, renames it and syncs the directory before this returns, so the
        // log purge openraft performs next can never outrun the snapshot (pitfall 3, T-03-30).
        let result = tokio::task::spawn_blocking(move || {
            let file = store.build(&engine, &meta)?;
            Ok::<_, SnapshotError>((meta, file))
        })
        .await;
        // Release the build slot as soon as the build is over, success or not.
        self.guard = None;
        let (meta, file) = result.map_err(join_error)?.map_err(snapshot_io_error)?;
        Ok(Snapshot { meta, snapshot: file })
    }
}

impl RaftStateMachine<TypeConfig> for KvStateMachine {
    type SnapshotData = SnapshotFile;
    type SnapshotBuilder = KvSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogIdOf<TypeConfig>>, StoredMembershipOf<TypeConfig>), io::Error> {
        Ok(KvStateMachine::applied_state(self))
    }

    async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), io::Error>
    where Strm: Stream<Item = Result<EntryResponder<TypeConfig>, io::Error>> + Unpin + OptionalSend
    {
        let mut applied = 0usize;
        while let Some(item) = entries.next().await {
            let (entry, responder) = item?;
            let response = self.apply_entry(&entry);
            if let Some(responder) = responder {
                responder.send(response);
            }
            applied += 1;
            if applied % APPLY_YIELD_EVERY == 0 {
                tokio::task::yield_now().await;
            }
        }
        Ok(())
    }

    async fn try_create_snapshot_builder(&mut self, _force: bool) -> Option<KvSnapshotBuilder> {
        let guard = Arc::clone(&self.build_lock).try_lock_owned().ok()?;
        Some(self.snapshot_builder(Some(guard)))
    }

    async fn get_snapshot_builder(&mut self) -> KvSnapshotBuilder {
        self.snapshot_builder(None)
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMetaOf<TypeConfig>,
        snapshot: SnapshotFile,
    ) -> Result<(), io::Error> {
        // Order is fixed (D-09): validate + load into a fresh engine, make the file durable as
        // the single current snapshot, and only then swap the engine. Any failure before the
        // swap changes nothing, so a corrupt or mismatched file can never replace live state
        // (T-03-31).
        let store = self.snapshots.clone().ok_or_else(|| {
            io::Error::new(io::ErrorKind::Unsupported, "snapshots are not configured")
        })?;
        // Serialize against snapshot builds: a build that started on the pre-install engine must
        // not publish (and delete older files) around the install. Held until the engine swap.
        let _build_guard = Arc::clone(&self.build_lock).lock_owned().await;
        let expected = meta.clone();
        let path = snapshot.path;
        let engine = tokio::task::spawn_blocking(move || {
            // The file is validated twice by design (once while loading here, once inside
            // `install_received`) to keep the failure ordering simple; a single-pass
            // optimization is out of scope for correctness-focused Phase 3.
            let (header, engine) = load_snapshot(&path)?;
            if header != expected {
                return Err(SnapshotError::MetaMismatch);
            }
            store.install_received(&path, &expected)?;
            Ok::<_, SnapshotError>(engine)
        })
        .await
        .map_err(join_error)?
        .map_err(snapshot_io_error)?;
        self.replace_engine(engine, meta.last_log_id.clone(), meta.last_membership.clone());
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<SnapshotOf<TypeConfig, SnapshotFile>>, io::Error> {
        let Some(store) = self.snapshots.clone() else {
            return Ok(None);
        };
        let latest = tokio::task::spawn_blocking(move || store.latest())
            .await
            .map_err(join_error)?
            .map_err(snapshot_io_error)?;
        Ok(latest.map(|(meta, file)| Snapshot { meta, snapshot: file }))
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

    /// An apply stream of `Set key{i} = val{i}` entries for `range`, with no responders.
    fn set_stream(
        range: std::ops::RangeInclusive<u64>,
    ) -> impl Stream<Item = Result<EntryResponder<TypeConfig>, io::Error>> + Unpin {
        futures::stream::iter(
            range
                .map(|i| Ok((normal(i, set(&format!("key{i}"), &format!("val{i}"))), None)))
                .collect::<Vec<_>>(),
        )
    }

    /// A mixed Set/Delete stream over seven keys, so later entries overwrite and delete earlier
    /// ones.
    fn mixed_stream(
        range: std::ops::RangeInclusive<u64>,
    ) -> impl Stream<Item = Result<EntryResponder<TypeConfig>, io::Error>> + Unpin {
        futures::stream::iter(
            range
                .map(|i| {
                    let key = b(&format!("k{}", i % 7));
                    let req = if i % 5 == 0 {
                        KvRequest::Delete { key }
                    } else {
                        KvRequest::Set { key, value: b(&format!("v{i}")) }
                    };
                    Ok((normal(i, req), None))
                })
                .collect::<Vec<_>>(),
        )
    }

    /// Names of the published `.snap` files in `dir`, sorted.
    fn snap_files(dir: &std::path::Path) -> Vec<String> {
        let mut v: Vec<String> = std::fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .filter(|n| n.ends_with(".snap"))
            .collect();
        v.sort();
        v
    }

    /// Copy `snap`'s file into `target`'s staging area, as the network layer would on receive.
    fn stage_received(target: &KvStateMachine, snap: &SnapshotOf<TypeConfig, SnapshotFile>) -> SnapshotFile {
        let staging = target.snapshots.as_ref().unwrap().staging_path(&snap.meta);
        std::fs::copy(&snap.snapshot.path, &staging).unwrap();
        SnapshotFile { path: staging, len: snap.snapshot.len }
    }

    /// A machine in `dir` holding `key{i} = val{i}` for `range`, plus a snapshot of it.
    async fn machine_with_snapshot(
        dir: &std::path::Path,
        range: std::ops::RangeInclusive<u64>,
    ) -> (KvStateMachine, SnapshotOf<TypeConfig, SnapshotFile>) {
        let mut sm = KvStateMachine::open(dir).await.unwrap();
        sm.apply(set_stream(range)).await.unwrap();
        let snap = sm.get_snapshot_builder().await.build_snapshot().await.unwrap();
        (sm, snap)
    }

    #[tokio::test]
    async fn no_overlapping_snapshot_builds() {
        let mut sm = KvStateMachine::new();
        let first = sm.try_create_snapshot_builder(false).await;
        assert!(first.is_some());
        assert!(sm.try_create_snapshot_builder(false).await.is_none());
        drop(first);
        assert!(sm.try_create_snapshot_builder(false).await.is_some());
    }

    #[tokio::test]
    async fn build_without_store_reports_not_configured() {
        let mut sm = KvStateMachine::new();
        let err = sm.get_snapshot_builder().await.build_snapshot().await.unwrap_err();
        assert!(err.to_string().contains("not configured"), "{err}");
        assert!(sm.get_current_snapshot().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn install_snapshot_replaces_state_and_reports_applied() {
        let (dir_a, dir_b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let (_a, snap) = machine_with_snapshot(dir_a.path(), 1..=10).await;
        let mut b_sm = KvStateMachine::open(dir_b.path()).await.unwrap();
        b_sm.apply(mixed_stream(1..=4)).await.unwrap();
        let old_engine = b_sm.engine();

        let received = stage_received(&b_sm, &snap);
        b_sm.install_snapshot(&snap.meta, received).await.unwrap();

        for i in 1..=10u64 {
            assert_eq!(
                b_sm.engine().get(format!("key{i}").as_bytes()),
                Some(b(&format!("val{i}")))
            );
        }
        assert_eq!(b_sm.engine().get(b"k1"), None, "old contents must be gone");
        let (applied, membership) = RaftStateMachine::applied_state(&mut b_sm).await.unwrap();
        assert_eq!(applied, snap.meta.last_log_id);
        assert_eq!(membership, snap.meta.last_membership);
        // A reader holding the old Arc still sees the old data.
        assert_eq!(old_engine.get(b"k1"), Some(b("v1")));
        assert_eq!(old_engine.get(b"key1"), None);
    }

    #[tokio::test]
    async fn install_waits_for_an_in_flight_build() {
        let (dir_a, dir_b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let (_a, snap) = machine_with_snapshot(dir_a.path(), 1..=10).await;
        let mut b_sm = KvStateMachine::open(dir_b.path()).await.unwrap();
        let received = stage_received(&b_sm, &snap);

        let build = b_sm.try_create_snapshot_builder(false).await.expect("lock is free");
        {
            let install = b_sm.install_snapshot(&snap.meta, received);
            tokio::pin!(install);
            let wait = std::time::Duration::from_millis(100);
            let waited = tokio::time::timeout(wait, &mut install).await;
            assert!(waited.is_err(), "install must wait while a build holds the lock");

            drop(build);
            install.await.unwrap();
        }
        assert_eq!(b_sm.engine().get(b"key10"), Some(b("val10")));
    }

    #[tokio::test]
    async fn engine_handle_tracks_install_snapshot() {
        let (dir_a, dir_b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let (_a, snap) = machine_with_snapshot(dir_a.path(), 1..=10).await;
        let mut b_sm = KvStateMachine::open(dir_b.path()).await.unwrap();
        b_sm.apply(mixed_stream(1..=4)).await.unwrap();

        // Cloned out exactly as RaftNode will before the machine moves into Raft::new.
        let handle = b_sm.engine_handle();
        let before = handle.load_full();

        let received = stage_received(&b_sm, &snap);
        b_sm.install_snapshot(&snap.meta, received).await.unwrap();

        let after = handle.load_full();
        assert!(!Arc::ptr_eq(&before, &after));
        assert_eq!(after.get(b"key10"), Some(b("val10")));
        assert_eq!(before.get(b"k1"), Some(b("v1")));
        assert_eq!(before.get(b"key10"), None);
        assert!(Arc::ptr_eq(&b_sm.engine(), &after));
    }

    #[tokio::test]
    async fn install_rejects_corrupt_file_and_keeps_old_engine() {
        let (dir_a, dir_b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let (_a, snap) = machine_with_snapshot(dir_a.path(), 1..=10).await;
        let mut b_sm = KvStateMachine::open(dir_b.path()).await.unwrap();
        b_sm.apply(mixed_stream(1..=4)).await.unwrap();
        let engine_before = b_sm.engine();
        let applied_before = KvStateMachine::applied_state(&b_sm);
        let snaps_before = snap_files(dir_b.path());

        let received = stage_received(&b_sm, &snap);
        let mut bytes = std::fs::read(&received.path).unwrap();
        let mid = bytes.len() / 2;
        bytes[mid] ^= 0x01;
        std::fs::write(&received.path, &bytes).unwrap();

        let err = b_sm.install_snapshot(&snap.meta, received).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData, "{err}");
        assert!(Arc::ptr_eq(&engine_before, &b_sm.engine()));
        assert_eq!(b_sm.engine().get(b"k1"), Some(b("v1")));
        assert_eq!(b_sm.engine().get(b"key1"), None);
        assert_eq!(KvStateMachine::applied_state(&b_sm), applied_before);
        assert_eq!(snap_files(dir_b.path()), snaps_before);
    }

    #[tokio::test]
    async fn install_rejects_meta_mismatch() {
        let (dir_a, dir_b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let (_a, snap) = machine_with_snapshot(dir_a.path(), 1..=10).await;
        let mut b_sm = KvStateMachine::open(dir_b.path()).await.unwrap();
        let received = stage_received(&b_sm, &snap);
        let mut wrong = snap.meta.clone();
        wrong.last_log_id = Some(log_id(11));
        assert!(b_sm.install_snapshot(&wrong, received).await.is_err());
        assert_eq!(b_sm.engine().get(b"key1"), None);
        assert!(snap_files(dir_b.path()).is_empty());
    }

    #[tokio::test]
    async fn install_keeps_exactly_one_snapshot_on_disk() {
        let (dir_a, dir_b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let (_a, snap) = machine_with_snapshot(dir_a.path(), 1..=10).await;
        let (mut b_sm, old) = machine_with_snapshot(dir_b.path(), 1..=3).await;
        assert_eq!(snap_files(dir_b.path()).len(), 1);
        assert_eq!(old.meta.last_log_id.as_ref().map(|l| l.index()), Some(3));

        let received = stage_received(&b_sm, &snap);
        b_sm.install_snapshot(&snap.meta, received).await.unwrap();

        let files = snap_files(dir_b.path());
        assert_eq!(files.len(), 1, "{files:?}");
        let current = b_sm.get_current_snapshot().await.unwrap().unwrap();
        assert_eq!(current.meta, snap.meta);
    }

    #[tokio::test]
    async fn restart_loads_latest_snapshot_then_replays() {
        let dir = tempfile::tempdir().unwrap();
        let mut sm = KvStateMachine::open(dir.path()).await.unwrap();
        sm.apply(mixed_stream(1..=50)).await.unwrap();
        // The builder captures start index 50; entries 51..=80 land while it is outstanding, so
        // the scan observes some of them (fuzzy).
        let mut builder = sm.try_create_snapshot_builder(false).await.unwrap();
        sm.apply(mixed_stream(51..=80)).await.unwrap();
        let snap = builder.build_snapshot().await.unwrap();
        assert_eq!(snap.meta.last_log_id.as_ref().map(|l| l.index()), Some(50));
        drop(builder);
        drop(sm);

        let mut restarted = KvStateMachine::open(dir.path()).await.unwrap();
        let (applied, _) = RaftStateMachine::applied_state(&mut restarted).await.unwrap();
        assert_eq!(applied.map(|l| l.index()), Some(50));
        restarted.apply(mixed_stream(51..=80)).await.unwrap();

        let mut sequential = KvStateMachine::new();
        sequential.apply(mixed_stream(1..=80)).await.unwrap();
        for k in 0..7 {
            let key = format!("k{k}");
            assert_eq!(
                restarted.engine().get(key.as_bytes()),
                sequential.engine().get(key.as_bytes()),
                "{key} differs"
            );
        }
        assert_eq!(
            KvStateMachine::applied_state(&restarted).0,
            KvStateMachine::applied_state(&sequential).0
        );
    }

    #[tokio::test]
    async fn apply_yields_every_256_entries() {
        use std::sync::atomic::{AtomicU64, Ordering};
        let ticks = Arc::new(AtomicU64::new(0));
        let ticker = {
            let ticks = Arc::clone(&ticks);
            tokio::spawn(async move {
                loop {
                    ticks.fetch_add(1, Ordering::Relaxed);
                    tokio::task::yield_now().await;
                }
            })
        };
        let mut sm = KvStateMachine::new();
        sm.apply(set_stream(1..=2000)).await.unwrap();
        ticker.abort();
        let seen = ticks.load(Ordering::Relaxed);
        assert!(seen >= 7, "ticker ran only {seen} times during 2000 applies");
    }

    #[tokio::test]
    async fn apply_stream_and_build_snapshot_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let mut sm = KvStateMachine::open(dir.path()).await.unwrap();
        sm.apply(set_stream(1..=10)).await.unwrap();

        let snap = sm.get_snapshot_builder().await.build_snapshot().await.unwrap();
        assert_eq!(snap.meta.last_log_id.as_ref().map(|l| l.index()), Some(10));
        assert!(snap.snapshot.path.exists());
        drop(sm);

        let mut reopened = KvStateMachine::open(dir.path()).await.unwrap();
        let (applied, _) = RaftStateMachine::applied_state(&mut reopened).await.unwrap();
        assert_eq!(applied.map(|l| l.index()), Some(10));
        for i in 1..=10u64 {
            assert_eq!(
                reopened.engine().get(format!("key{i}").as_bytes()),
                Some(b(&format!("val{i}")))
            );
        }
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
