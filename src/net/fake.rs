//! Deterministic in-process fake network for multi-node consensus tests (decision D-15).
//!
//! Compiled only under `cfg(any(test, feature = "test-util"))`; it carries no external input and
//! never ships in a release build. RPCs are routed to [`RaftHandler`]s registered per node id, so
//! the same stub handlers can drive the real TCP server tests. There is no randomness: link
//! faults and delays are explicit, directed, and controlled through [`NetController`].

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use futures::future::BoxFuture;
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

/// Which of the three per-peer connections a client represents (interpretation A4).
///
/// Mirrors the wire-level lane tag; kept local so the fake network does not depend on the wire
/// module.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Lane {
    /// Pipelined log replication.
    Replication,
    /// Heartbeats, isolated from bulk traffic.
    Heartbeat,
    /// Snapshot transfer.
    Snapshot,
}

struct Registered {
    handler: Arc<dyn RaftHandler>,
    #[allow(dead_code)]
    recv_dir: PathBuf,
}

#[derive(Default)]
struct Inner {
    nodes: HashMap<u64, Registered>,
}

/// Shared control plane of the fake network. Cheap to clone; all clones see the same state.
#[derive(Clone, Default)]
pub struct NetController {
    inner: Arc<Mutex<Inner>>,
}

impl NetController {
    /// A fresh network with no registered nodes.
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

    /// A network factory for the node `me`.
    pub fn factory(&self, me: u64) -> FakeNetworkFactory {
        FakeNetworkFactory {
            ctl: self.clone(),
            me,
        }
    }

    fn route(&self, target: u64) -> Result<Arc<dyn RaftHandler>, RPCError<TypeConfig>> {
        let inner = self.inner.lock();
        match inner.nodes.get(&target) {
            Some(r) => Ok(r.handler.clone()),
            None => Err(unreachable("node not registered")),
        }
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

impl RaftNetworkV2<TypeConfig> for FakePeer {
    type SnapshotData = SnapshotFile;

    async fn append_entries(
        &mut self,
        _rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendResult<TypeConfig>, RPCError<TypeConfig>> {
        Err(unreachable("append_entries not wired yet"))
    }

    fn stream_append<'s, S>(
        &'s mut self,
        _input: S,
        _option: RPCOption,
    ) -> BoxFuture<
        's,
        Result<
            futures::stream::BoxStream<'s, Result<AppendResult<TypeConfig>, RPCError<TypeConfig>>>,
            RPCError<TypeConfig>,
        >,
    >
    where
        S: futures::Stream<Item = AppendEntriesRequest<TypeConfig>> + OptionalSend + Unpin + 'static,
    {
        Box::pin(async { Err(unreachable("stream_append not wired yet")) })
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        let handler = self.ctl.route(self.target)?;
        handler.vote(rpc).await.map_err(handler_err)
    }

    async fn full_snapshot(
        &mut self,
        _vote: VoteOf<TypeConfig>,
        _snapshot: SnapshotOf<TypeConfig, Self::SnapshotData>,
        _cancel: impl std::future::Future<Output = ReplicationClosed> + OptionalSend + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
        Err(StreamingError::Unreachable(Unreachable::from_string(
            "full_snapshot not wired yet",
        )))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use futures::stream::{self, BoxStream, StreamExt};
    use openraft::errors::{Fatal, RaftError};
    use openraft::type_config::alias::SnapshotMetaOf;

    use super::*;

    /// Records vote calls and returns canned responses; other methods are inert.
    #[derive(Default)]
    struct StubHandler {
        votes: AtomicUsize,
    }

    impl RaftHandler for StubHandler {
        fn append_entries(
            &self,
            _req: AppendEntriesRequest<TypeConfig>,
        ) -> BoxFuture<'static, Result<AppendResult<TypeConfig>, RaftError<TypeConfig>>> {
            Box::pin(async { Ok(Ok(None)) })
        }

        fn stream_append(
            &self,
            _reqs: BoxStream<'static, AppendEntriesRequest<TypeConfig>>,
        ) -> BoxStream<'static, Result<AppendResult<TypeConfig>, Fatal<TypeConfig>>> {
            stream::empty().boxed()
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
            Box::pin(async move { Ok(VoteResponse::new(req.vote, None, true)) })
        }

        fn install_snapshot(
            &self,
            vote: VoteOf<TypeConfig>,
            _meta: SnapshotMetaOf<TypeConfig>,
            _file: SnapshotFile,
        ) -> BoxFuture<'static, Result<SnapshotResponse<TypeConfig>, Fatal<TypeConfig>>> {
            Box::pin(async move { Ok(SnapshotResponse::new(vote)) })
        }
    }

    fn vote_req() -> VoteRequest<TypeConfig> {
        VoteRequest::new(VoteOf::<TypeConfig>::new(2, 1), None)
    }

    fn opt() -> RPCOption {
        RPCOption::new(Duration::from_secs(1))
    }

    #[tokio::test]
    async fn fake_vote_delivered_to_registered_handler() {
        let ctl = NetController::new();
        let stub = Arc::new(StubHandler::default());
        let dir = tempfile::tempdir().unwrap();
        ctl.register(2, stub.clone(), dir.path().to_path_buf());

        let mut factory = ctl.factory(1);
        let mut client = factory.new_client(2, &BasicNode::default()).await;
        let resp = client.vote(vote_req(), opt()).await.expect("vote");

        assert!(resp.vote_granted);
        assert_eq!(resp.vote, VoteOf::<TypeConfig>::new(2, 1));
        assert_eq!(stub.votes.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn fake_unregistered_peer_is_unreachable() {
        let ctl = NetController::new();
        let mut factory = ctl.factory(1);
        let mut client = factory.new_client(9, &BasicNode::default()).await;
        let err = client.vote(vote_req(), opt()).await.expect_err("must fail");
        assert!(matches!(err, RPCError::Unreachable(_)), "got {err:?}");
    }
}
