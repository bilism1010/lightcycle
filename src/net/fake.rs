//! Deterministic in-process fake network for multi-node consensus tests (decision D-15).
//!
//! Compiled only under `cfg(any(test, feature = "test-util"))`; it carries no external input and
//! never ships in a release build. RPCs are routed to [`RaftHandler`]s registered per node id, so
//! the same stub handlers can drive the real TCP server tests. There is no randomness: link
//! faults and delays are explicit, directed, and controlled through [`NetController`].
//!
//! Like the real transport, the factory hands out separate clients for replication, heartbeat
//! and snapshot traffic (interpretation A4); [`NetController::stats`] counts delivered and
//! dropped calls per lane so tests can assert which lane carried a message.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::{Stream, StreamExt};
use openraft::errors::{NetworkError, RPCError, ReplicationClosed, StreamingError, Unreachable};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendResult, SnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::type_config::alias::{SnapshotOf, VoteOf};
use openraft::{BasicNode, OptionalSend, RaftNetworkFactory, RaftNetworkV2};
use parking_lot::Mutex;

use crate::net::handler::RaftHandler;
use crate::raft::types::{SnapshotFile, TypeConfig};

/// Which of the three per-peer connections a client represents (interpretation A4): the same
/// type the TCP transport tags its connections with.
pub use crate::wire::frame::Lane;

/// Delivered and dropped call counts for one lane.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LaneCounts {
    /// Calls (or stream requests) that reached the target handler.
    pub delivered: u64,
    /// Calls refused because the link was down or the target was not registered.
    pub dropped: u64,
}

/// Per-lane traffic counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NetStats {
    /// Replication lane.
    pub replication: LaneCounts,
    /// Heartbeat lane.
    pub heartbeat: LaneCounts,
    /// Snapshot lane.
    pub snapshot: LaneCounts,
}

impl NetStats {
    /// Counters for `lane`.
    pub fn lane(&self, lane: Lane) -> LaneCounts {
        match lane {
            Lane::Replication => self.replication,
            Lane::Heartbeat => self.heartbeat,
            Lane::Snapshot => self.snapshot,
        }
    }

    fn lane_mut(&mut self, lane: Lane) -> &mut LaneCounts {
        match lane {
            Lane::Replication => &mut self.replication,
            Lane::Heartbeat => &mut self.heartbeat,
            Lane::Snapshot => &mut self.snapshot,
        }
    }
}

struct Registered {
    handler: Arc<dyn RaftHandler>,
    recv_dir: PathBuf,
}

#[derive(Default)]
struct Inner {
    nodes: HashMap<u64, Registered>,
    /// Directed `(from, to)` links that are down.
    blocked: HashSet<(u64, u64)>,
    /// Nodes cut off from every other node, in both directions.
    isolated: HashSet<u64>,
    /// Per directed link one-way delay.
    delays: HashMap<(u64, u64), Duration>,
    stats: NetStats,
}

impl Inner {
    fn link_down(&self, from: u64, to: u64) -> bool {
        self.blocked.contains(&(from, to))
            || self.isolated.contains(&from)
            || self.isolated.contains(&to)
    }
}

/// A resolved destination for one call.
struct Routed {
    handler: Arc<dyn RaftHandler>,
    recv_dir: PathBuf,
}

/// Shared control plane of the fake network. Cheap to clone; all clones see the same state.
#[derive(Clone, Default)]
pub struct NetController {
    inner: Arc<Mutex<Inner>>,
    snapshot_seq: Arc<AtomicU64>,
}

impl NetController {
    /// A fresh network with no registered nodes and all links up.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register `handler` as the receiver for `node_id`. `recv_dir` is where incoming snapshot
    /// files are copied.
    pub fn register(&self, node_id: u64, handler: Arc<dyn RaftHandler>, recv_dir: PathBuf) {
        self.inner
            .lock()
            .nodes
            .insert(node_id, Registered { handler, recv_dir });
    }

    /// Remove `node_id`; subsequent calls to it fail as unreachable.
    pub fn unregister(&self, node_id: u64) {
        self.inner.lock().nodes.remove(&node_id);
    }

    /// Block the `from -> to` direction only.
    pub fn drop_link(&self, from: u64, to: u64) {
        self.inner.lock().blocked.insert((from, to));
    }

    /// Re-open the `from -> to` direction.
    pub fn restore_link(&self, from: u64, to: u64) {
        self.inner.lock().blocked.remove(&(from, to));
    }

    /// Block every pair of nodes that sit in different `groups`, in both directions. Nodes that
    /// appear in no group are unaffected. Additive with earlier blocks; [`heal`](Self::heal)
    /// clears everything.
    pub fn partition(&self, groups: &[&[u64]]) {
        let mut inner = self.inner.lock();
        for (gi, a) in groups.iter().enumerate() {
            for b in groups.iter().skip(gi + 1) {
                for &x in a.iter() {
                    for &y in b.iter() {
                        inner.blocked.insert((x, y));
                        inner.blocked.insert((y, x));
                    }
                }
            }
        }
    }

    /// Cut `node` off from every other node (all links to and from it).
    pub fn isolate(&self, node: u64) {
        self.inner.lock().isolated.insert(node);
    }

    /// Clear every block, partition and isolation. Delays are kept.
    pub fn heal(&self) {
        let mut inner = self.inner.lock();
        inner.blocked.clear();
        inner.isolated.clear();
    }

    /// Set the one-way delay of the `from -> to` link. A zero duration removes the delay.
    pub fn set_delay(&self, from: u64, to: u64, delay: Duration) {
        let mut inner = self.inner.lock();
        if delay.is_zero() {
            inner.delays.remove(&(from, to));
        } else {
            inner.delays.insert((from, to), delay);
        }
    }

    /// Snapshot of the per-lane delivered/dropped counters.
    pub fn stats(&self) -> NetStats {
        self.inner.lock().stats
    }

    /// A network factory for the node `me`.
    pub fn factory(&self, me: u64) -> FakeNetworkFactory {
        FakeNetworkFactory {
            ctl: self.clone(),
            me,
        }
    }

    fn delay(&self, from: u64, to: u64) -> Duration {
        self.inner
            .lock()
            .delays
            .get(&(from, to))
            .copied()
            .unwrap_or(Duration::ZERO)
    }

    fn next_snapshot_seq(&self) -> u64 {
        self.snapshot_seq.fetch_add(1, Ordering::Relaxed)
    }

    /// Resolve the destination of a call, failing fast when the link is down or the target is
    /// not registered. A refusal is always counted as dropped; success is counted as delivered
    /// only when `count_delivered` is set (stream setup defers that to each request).
    fn check(
        &self,
        from: u64,
        to: u64,
        lane: Lane,
        count_delivered: bool,
    ) -> Result<Routed, RPCError<TypeConfig>> {
        let mut inner = self.inner.lock();
        if inner.link_down(from, to) {
            inner.stats.lane_mut(lane).dropped += 1;
            return Err(unreachable("link down"));
        }
        let routed = match inner.nodes.get(&to) {
            Some(r) => Routed {
                handler: r.handler.clone(),
                recv_dir: r.recv_dir.clone(),
            },
            None => {
                inner.stats.lane_mut(lane).dropped += 1;
                return Err(unreachable("node not registered"));
            }
        };
        if count_delivered {
            inner.stats.lane_mut(lane).delivered += 1;
        }
        Ok(routed)
    }

    /// `check` plus the link delay, for unary calls.
    async fn deliver(
        &self,
        from: u64,
        to: u64,
        lane: Lane,
    ) -> Result<Routed, RPCError<TypeConfig>> {
        let routed = self.check(from, to, lane, true)?;
        let delay = self.delay(from, to);
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        Ok(routed)
    }
}

fn unreachable(msg: &str) -> RPCError<TypeConfig> {
    RPCError::Unreachable(Unreachable::from_string(msg))
}

/// Map an error returned by the remote handler. `RPCError<TypeConfig>` has an uninhabited remote
/// error payload, so a handler failure surfaces as a network error.
fn handler_err<E: std::error::Error + 'static>(e: E) -> RPCError<TypeConfig> {
    RPCError::Network(NetworkError::new(&e))
}

/// A remote node that has stopped is unreachable, not merely a failed request.
fn fatal_err<E: std::error::Error + 'static>(e: E) -> RPCError<TypeConfig> {
    RPCError::Unreachable(Unreachable::new(&e))
}

/// Network factory handed to a Raft node; produces one [`FakePeer`] per lane and target.
pub struct FakeNetworkFactory {
    ctl: NetController,
    me: u64,
}

impl FakeNetworkFactory {
    fn peer(&self, target: u64, lane: Lane) -> FakePeer {
        FakePeer {
            ctl: self.ctl.clone(),
            me: self.me,
            target,
            lane,
        }
    }
}

impl RaftNetworkFactory<TypeConfig> for FakeNetworkFactory {
    type Network = FakePeer;

    async fn new_client(&mut self, target: u64, _node: &BasicNode) -> Self::Network {
        self.peer(target, Lane::Replication)
    }

    async fn new_heartbeat_client(&mut self, target: u64, _node: &BasicNode) -> Self::Network {
        self.peer(target, Lane::Heartbeat)
    }

    async fn new_snapshot_client(&mut self, target: u64, _node: &BasicNode) -> Self::Network {
        self.peer(target, Lane::Snapshot)
    }
}

/// One directed client from `me` to `target` on a given lane.
pub struct FakePeer {
    ctl: NetController,
    me: u64,
    target: u64,
    lane: Lane,
}

impl FakePeer {
    /// The lane this client was created for.
    pub fn lane(&self) -> Lane {
        self.lane
    }

    /// The node this client sends from.
    pub fn from_node(&self) -> u64 {
        self.me
    }
}

/// Receive a snapshot: copy the file into the receiver's directory (numeric-only name), hand the
/// copy to the target handler.
async fn deliver_snapshot(
    ctl: NetController,
    me: u64,
    target: u64,
    lane: Lane,
    vote: VoteOf<TypeConfig>,
    snapshot: SnapshotOf<TypeConfig, SnapshotFile>,
) -> Result<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
    let routed = ctl.deliver(me, target, lane).await?;
    let io_err = |e: std::io::Error| StreamingError::Network(NetworkError::new(&e));
    std::fs::create_dir_all(&routed.recv_dir).map_err(io_err)?;
    let n = ctl.next_snapshot_seq();
    let dest = routed.recv_dir.join(format!("recv-{n}.tmp"));
    let len = std::fs::copy(&snapshot.snapshot.path, &dest).map_err(io_err)?;
    let file = SnapshotFile { path: dest, len };
    routed
        .handler
        .install_snapshot(vote, snapshot.meta, file)
        .await
        .map_err(|e| StreamingError::Unreachable(Unreachable::new(&e)))
}

impl RaftNetworkV2<TypeConfig> for FakePeer {
    type SnapshotData = SnapshotFile;

    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendResult<TypeConfig>, RPCError<TypeConfig>> {
        let routed = self.ctl.deliver(self.me, self.target, self.lane).await?;
        routed.handler.append_entries(rpc).await.map_err(handler_err)
    }

    /// Pipelined AppendEntries. Every request is delayed per the link, the link is re-checked
    /// before each is forwarded, and the handler's result stream is returned as-is (so response
    /// order equals request order). A link that goes down mid-stream ends the request stream,
    /// hence the output stream, and openraft backs off.
    fn stream_append<'s, S>(
        &'s mut self,
        input: S,
        _option: RPCOption,
    ) -> BoxFuture<
        's,
        Result<
            BoxStream<'s, Result<AppendResult<TypeConfig>, RPCError<TypeConfig>>>,
            RPCError<TypeConfig>,
        >,
    >
    where
        S: Stream<Item = AppendEntriesRequest<TypeConfig>> + OptionalSend + Unpin + 'static,
    {
        let ctl = self.ctl.clone();
        let (me, target, lane) = (self.me, self.target, self.lane);
        Box::pin(async move {
            let routed = ctl.check(me, target, lane, false)?;
            let wrapped = futures::stream::unfold((input, ctl), move |(mut input, ctl)| async move {
                let req = input.next().await?;
                let delay = ctl.delay(me, target);
                if !delay.is_zero() {
                    tokio::time::sleep(delay).await;
                }
                match ctl.check(me, target, lane, true) {
                    Ok(_) => Some((req, (input, ctl))),
                    Err(_) => None,
                }
            })
            .fuse()
            .boxed();
            let out = routed
                .handler
                .stream_append(wrapped)
                .map(|r| r.map_err(fatal_err))
                .boxed();
            Ok(out)
        })
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        let routed = self.ctl.deliver(self.me, self.target, self.lane).await?;
        routed.handler.vote(rpc).await.map_err(handler_err)
    }

    async fn pre_vote(
        &mut self,
        rpc: VoteRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        let routed = self.ctl.deliver(self.me, self.target, self.lane).await?;
        routed.handler.pre_vote(rpc).await.map_err(handler_err)
    }

    async fn full_snapshot(
        &mut self,
        vote: VoteOf<TypeConfig>,
        snapshot: SnapshotOf<TypeConfig, Self::SnapshotData>,
        cancel: impl std::future::Future<Output = ReplicationClosed> + OptionalSend + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
        let work = deliver_snapshot(
            self.ctl.clone(),
            self.me,
            self.target,
            self.lane,
            vote,
            snapshot,
        );
        tokio::select! {
            biased;
            closed = cancel => Err(StreamingError::Closed(closed)),
            res = work => res,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::atomic::AtomicUsize;

    use futures::stream;
    use openraft::errors::{Fatal, RaftError};
    use openraft::type_config::alias::{LogIdOf, SnapshotMetaOf};
    use openraft::{Snapshot, SnapshotMeta};

    use super::*;

    /// Records calls and returns canned responses.
    #[derive(Default)]
    struct StubHandler {
        votes: AtomicUsize,
        pre_votes: AtomicUsize,
        appends: AtomicUsize,
        install_count: AtomicUsize,
        /// `(received path, bytes read at install time)` per installed snapshot.
        installs: Mutex<Vec<(PathBuf, Vec<u8>)>>,
    }

    impl RaftHandler for StubHandler {
        fn append_entries(
            &self,
            req: AppendEntriesRequest<TypeConfig>,
        ) -> BoxFuture<'static, Result<AppendResult<TypeConfig>, RaftError<TypeConfig>>> {
            self.appends.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move { Ok(Ok(req.prev_log_id)) })
        }

        /// Echoes each request's `prev_log_id` so tests can check response order.
        fn stream_append(
            &self,
            reqs: BoxStream<'static, AppendEntriesRequest<TypeConfig>>,
        ) -> BoxStream<'static, Result<AppendResult<TypeConfig>, Fatal<TypeConfig>>> {
            reqs.map(|r| Ok(Ok(r.prev_log_id))).boxed()
        }

        fn vote(
            &self,
            req: VoteRequest<TypeConfig>,
        ) -> BoxFuture<'static, Result<VoteResponse<TypeConfig>, RaftError<TypeConfig>>> {
            self.votes.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move { Ok(VoteResponse::new(req.vote, None, true)) })
        }

        fn pre_vote(
            &self,
            req: VoteRequest<TypeConfig>,
        ) -> BoxFuture<'static, Result<VoteResponse<TypeConfig>, RaftError<TypeConfig>>> {
            self.pre_votes.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move { Ok(VoteResponse::new(req.vote, None, true)) })
        }

        fn install_snapshot(
            &self,
            vote: VoteOf<TypeConfig>,
            _meta: SnapshotMetaOf<TypeConfig>,
            file: SnapshotFile,
        ) -> BoxFuture<'static, Result<SnapshotResponse<TypeConfig>, Fatal<TypeConfig>>> {
            self.install_count.fetch_add(1, Ordering::SeqCst);
            let bytes = std::fs::read(&file.path).unwrap_or_default();
            self.installs.lock().push((file.path, bytes));
            Box::pin(async move { Ok(SnapshotResponse::new(vote)) })
        }
    }

    struct Cluster {
        ctl: NetController,
        stubs: HashMap<u64, Arc<StubHandler>>,
        dir: tempfile::TempDir,
    }

    impl Cluster {
        fn new(ids: &[u64]) -> Self {
            let ctl = NetController::new();
            let dir = tempfile::tempdir().unwrap();
            let mut stubs = HashMap::new();
            for &id in ids {
                let stub = Arc::new(StubHandler::default());
                ctl.register(id, stub.clone(), dir.path().join(format!("n{id}")));
                stubs.insert(id, stub);
            }
            Cluster { ctl, stubs, dir }
        }

        fn recv_dir(&self, id: u64) -> PathBuf {
            self.dir.path().join(format!("n{id}"))
        }

        async fn client(&self, from: u64, to: u64) -> FakePeer {
            self.ctl.factory(from).new_client(to, &BasicNode::default()).await
        }

        async fn vote_ok(&self, from: u64, to: u64) -> bool {
            self.client(from, to).await.vote(vote_req(), opt()).await.is_ok()
        }
    }

    fn vote_req() -> VoteRequest<TypeConfig> {
        VoteRequest::new(VoteOf::<TypeConfig>::new(2, 1), None)
    }

    fn opt() -> RPCOption {
        RPCOption::new(Duration::from_secs(1))
    }

    fn lid(index: u64) -> LogIdOf<TypeConfig> {
        openraft::LogId::new_term_index(1, index)
    }

    fn append_req(index: u64) -> AppendEntriesRequest<TypeConfig> {
        AppendEntriesRequest {
            vote: VoteOf::<TypeConfig>::new_committed(1, 1),
            prev_log_id: Some(lid(index)),
            entries: Vec::new(),
            leader_commit: None,
        }
    }

    fn snapshot_of(path: &Path, len: u64) -> SnapshotOf<TypeConfig, SnapshotFile> {
        Snapshot {
            meta: SnapshotMeta {
                last_log_id: Some(lid(5)),
                last_membership: Default::default(),
            },
            snapshot: SnapshotFile {
                path: path.to_path_buf(),
                len,
            },
        }
    }

    fn never_cancelled() -> impl std::future::Future<Output = ReplicationClosed> + Send + 'static {
        std::future::pending::<ReplicationClosed>()
    }

    #[tokio::test]
    async fn fake_vote_delivered_to_registered_handler() {
        let c = Cluster::new(&[2]);
        let mut client = c.client(1, 2).await;
        let resp = client.vote(vote_req(), opt()).await.expect("vote");

        assert!(resp.vote_granted);
        assert_eq!(resp.vote, VoteOf::<TypeConfig>::new(2, 1));
        assert_eq!(c.stubs[&2].votes.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn fake_unregistered_peer_is_unreachable() {
        let c = Cluster::new(&[]);
        let mut client = c.client(1, 9).await;
        let err = client.vote(vote_req(), opt()).await.expect_err("must fail");
        assert!(matches!(err, RPCError::Unreachable(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn fake_unregister_makes_node_unreachable() {
        let c = Cluster::new(&[1, 2]);
        assert!(c.vote_ok(1, 2).await);
        c.ctl.unregister(2);
        assert!(!c.vote_ok(1, 2).await);
    }

    #[tokio::test]
    async fn fake_drop_link_is_directional_and_restorable() {
        let c = Cluster::new(&[1, 2]);
        c.ctl.drop_link(1, 2);
        assert!(!c.vote_ok(1, 2).await, "blocked direction");
        assert!(c.vote_ok(2, 1).await, "reverse direction unaffected");
        c.ctl.restore_link(1, 2);
        assert!(c.vote_ok(1, 2).await);
    }

    #[tokio::test]
    async fn fake_partition_drops_both_directions_and_heal_restores() {
        let c = Cluster::new(&[1, 2, 3]);
        c.ctl.partition(&[&[1], &[2, 3]]);

        assert!(!c.vote_ok(1, 2).await);
        assert!(!c.vote_ok(2, 1).await);
        assert!(!c.vote_ok(1, 3).await);
        assert!(!c.vote_ok(3, 1).await);
        assert!(c.vote_ok(2, 3).await, "same side stays connected");
        assert!(c.vote_ok(3, 2).await);

        c.ctl.heal();
        assert!(c.vote_ok(1, 2).await);
        assert!(c.vote_ok(2, 1).await);
        assert!(c.vote_ok(1, 3).await);
        assert!(c.vote_ok(3, 1).await);
    }

    #[tokio::test]
    async fn fake_isolate_blocks_all_links() {
        let c = Cluster::new(&[1, 2, 3]);
        c.ctl.isolate(2);

        assert!(!c.vote_ok(1, 2).await);
        assert!(!c.vote_ok(3, 2).await);
        assert!(!c.vote_ok(2, 1).await);
        assert!(!c.vote_ok(2, 3).await);
        assert!(c.vote_ok(1, 3).await, "other nodes stay connected");

        c.ctl.heal();
        assert!(c.vote_ok(2, 1).await);
        assert!(c.vote_ok(1, 2).await);
    }

    #[tokio::test(start_paused = true)]
    async fn fake_delay_preserves_stream_append_order() {
        let c = Cluster::new(&[1, 2]);
        let delay = Duration::from_millis(40);
        c.ctl.set_delay(1, 2, delay);

        let mut client = c.client(1, 2).await;
        let input = stream::iter((1..=3).map(append_req));
        let started = tokio::time::Instant::now();
        let out: Vec<_> = client
            .stream_append(input, opt())
            .await
            .expect("stream setup")
            .collect()
            .await;

        let ids: Vec<_> = out
            .into_iter()
            .map(|r| r.expect("rpc ok").expect("append ok"))
            .collect();
        assert_eq!(ids, vec![Some(lid(1)), Some(lid(2)), Some(lid(3))]);
        assert!(
            started.elapsed() >= delay,
            "virtual elapsed {:?} < delay {:?}",
            started.elapsed(),
            delay
        );
        assert_eq!(c.ctl.stats().replication.delivered, 3);
    }

    #[tokio::test]
    async fn fake_link_down_mid_stream_ends_output() {
        let c = Cluster::new(&[1, 2]);
        let mut client = c.client(1, 2).await;
        let (tx, rx) = futures::channel::mpsc::unbounded();

        let mut out = client.stream_append(rx, opt()).await.expect("stream setup");

        tx.unbounded_send(append_req(1)).unwrap();
        let first = out.next().await.expect("first response").expect("rpc ok");
        assert_eq!(first.expect("append ok"), Some(lid(1)));

        c.ctl.drop_link(1, 2);
        tx.unbounded_send(append_req(2)).unwrap();
        assert!(out.next().await.is_none(), "output must end when the link drops");
        assert_eq!(c.ctl.stats().replication.dropped, 1);
    }

    #[tokio::test]
    async fn fake_stream_append_on_blocked_link_fails_setup() {
        let c = Cluster::new(&[1, 2]);
        c.ctl.drop_link(1, 2);
        let mut client = c.client(1, 2).await;
        let res = client.stream_append(stream::iter(vec![append_req(1)]), opt()).await;
        assert!(matches!(res, Err(RPCError::Unreachable(_))));
    }

    #[tokio::test]
    async fn fake_snapshot_copies_file_to_receiver_dir() {
        let c = Cluster::new(&[1, 2]);
        let src_dir = tempfile::tempdir().unwrap();
        let src = src_dir.path().join("snap.bin");
        let bytes: Vec<u8> = (0..=255u8).cycle().take(10_000).collect();
        std::fs::write(&src, &bytes).unwrap();

        let mut client = c.ctl.factory(1).new_snapshot_client(2, &BasicNode::default()).await;
        let resp = client
            .full_snapshot(
                VoteOf::<TypeConfig>::new_committed(1, 1),
                snapshot_of(&src, bytes.len() as u64),
                never_cancelled(),
                opt(),
            )
            .await
            .expect("snapshot");
        assert_eq!(resp.vote, VoteOf::<TypeConfig>::new_committed(1, 1));

        let installs = c.stubs[&2].installs.lock();
        assert_eq!(installs.len(), 1);
        let (path, got) = &installs[0];
        assert_ne!(path, &src, "receiver gets a copy, not the source path");
        assert_eq!(path.parent().unwrap(), c.recv_dir(2));
        let name = path.file_name().unwrap().to_str().unwrap();
        let n = name.strip_prefix("recv-").and_then(|s| s.strip_suffix(".tmp")).unwrap();
        assert!(!n.is_empty() && n.chars().all(|ch| ch.is_ascii_digit()), "name {name}");
        assert_eq!(got, &bytes, "copied bytes identical");
        assert!(src.exists(), "source file must survive");
    }

    #[tokio::test]
    async fn fake_snapshot_on_blocked_link_is_unreachable() {
        let c = Cluster::new(&[1, 2]);
        c.ctl.drop_link(1, 2);
        let src_dir = tempfile::tempdir().unwrap();
        let src = src_dir.path().join("snap.bin");
        std::fs::write(&src, b"x").unwrap();

        let mut client = c.ctl.factory(1).new_snapshot_client(2, &BasicNode::default()).await;
        let err = client
            .full_snapshot(
                VoteOf::<TypeConfig>::new_committed(1, 1),
                snapshot_of(&src, 1),
                never_cancelled(),
                opt(),
            )
            .await
            .expect_err("must fail");
        assert!(matches!(err, StreamingError::Unreachable(_)), "got {err:?}");
        assert_eq!(c.stubs[&2].install_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn fake_heartbeat_and_snapshot_clients_use_distinct_lanes() {
        let c = Cluster::new(&[1, 2]);
        let mut factory = c.ctl.factory(1);
        let node = BasicNode::default();
        let mut repl = factory.new_client(2, &node).await;
        let mut hb = factory.new_heartbeat_client(2, &node).await;
        let mut snap = factory.new_snapshot_client(2, &node).await;
        assert_eq!(repl.lane(), Lane::Replication);
        assert_eq!(hb.lane(), Lane::Heartbeat);
        assert_eq!(snap.lane(), Lane::Snapshot);
        assert_eq!(repl.from_node(), 1);

        repl.append_entries(append_req(1), opt()).await.unwrap().unwrap();
        hb.append_entries(append_req(2), opt()).await.unwrap().unwrap();
        hb.append_entries(append_req(3), opt()).await.unwrap().unwrap();
        hb.pre_vote(vote_req(), opt()).await.unwrap();

        let src_dir = tempfile::tempdir().unwrap();
        let src = src_dir.path().join("snap.bin");
        std::fs::write(&src, b"abc").unwrap();
        snap.full_snapshot(
            VoteOf::<TypeConfig>::new_committed(1, 1),
            snapshot_of(&src, 3),
            never_cancelled(),
            opt(),
        )
        .await
        .unwrap();

        let s = c.ctl.stats();
        assert_eq!(s.lane(Lane::Replication), LaneCounts { delivered: 1, dropped: 0 });
        assert_eq!(s.lane(Lane::Heartbeat), LaneCounts { delivered: 3, dropped: 0 });
        assert_eq!(s.lane(Lane::Snapshot), LaneCounts { delivered: 1, dropped: 0 });
        assert_eq!(c.stubs[&2].appends.load(Ordering::SeqCst), 3);
        assert_eq!(c.stubs[&2].pre_votes.load(Ordering::SeqCst), 1);

        // A dropped heartbeat is attributed to the heartbeat lane only.
        c.ctl.drop_link(1, 2);
        assert!(hb.append_entries(append_req(4), opt()).await.is_err());
        let s = c.ctl.stats();
        assert_eq!(s.heartbeat.dropped, 1);
        assert_eq!(s.replication.dropped, 0);
        assert_eq!(s.snapshot.dropped, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn fake_snapshot_cancel_returns_cancelled() {
        let c = Cluster::new(&[1, 2]);
        c.ctl.set_delay(1, 2, Duration::from_secs(60));
        let src_dir = tempfile::tempdir().unwrap();
        let src = src_dir.path().join("snap.bin");
        std::fs::write(&src, b"abc").unwrap();

        let mut client = c.ctl.factory(1).new_snapshot_client(2, &BasicNode::default()).await;
        let err = client
            .full_snapshot(
                VoteOf::<TypeConfig>::new_committed(1, 1),
                snapshot_of(&src, 3),
                async { ReplicationClosed::new("test cancel") },
                opt(),
            )
            .await
            .expect_err("must be cancelled");

        assert!(matches!(err, StreamingError::Closed(_)), "got {err:?}");
        assert_eq!(c.stubs[&2].install_count.load(Ordering::SeqCst), 0);
        assert!(!c.recv_dir(2).exists(), "nothing copied after cancel");
    }
}
