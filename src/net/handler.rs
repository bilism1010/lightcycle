//! Server-side dispatch target for inbound Raft RPCs (decision D-15).
//!
//! [`RaftHandler`] is the single seam both the in-process fake network (`net::fake`, test
//! builds) and the TCP server (plan 03-09) call into. A blanket impl forwards it to a real
//! [`openraft::Raft`] node; tests substitute stubs so transport and codec code can be exercised
//! without a full Raft node.
//!
//! The trait is object safe (boxed futures and streams) so it can be held as
//! `Arc<dyn RaftHandler>`.

use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::StreamExt;
use openraft::errors::{Fatal, RaftError};
use openraft::raft::{
    AppendEntriesRequest, AppendResult, SnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::storage::RaftStateMachine;
use openraft::type_config::alias::{SnapshotMetaOf, VoteOf};
use openraft::{Raft, Snapshot};

use crate::raft::types::{SnapshotFile, TypeConfig};

/// Handles inbound Raft RPCs on the receiving node.
pub trait RaftHandler: Send + Sync + 'static {
    /// Unary AppendEntries.
    fn append_entries(
        &self,
        req: AppendEntriesRequest<TypeConfig>,
    ) -> BoxFuture<'static, Result<AppendResult<TypeConfig>, RaftError<TypeConfig>>>;

    /// Pipelined AppendEntries: responses are yielded in request order.
    fn stream_append(
        &self,
        reqs: BoxStream<'static, AppendEntriesRequest<TypeConfig>>,
    ) -> BoxStream<'static, Result<AppendResult<TypeConfig>, Fatal<TypeConfig>>>;

    /// RequestVote.
    fn vote(
        &self,
        req: VoteRequest<TypeConfig>,
    ) -> BoxFuture<'static, Result<VoteResponse<TypeConfig>, RaftError<TypeConfig>>>;

    /// Pre-Vote: asks whether a vote would be granted without persisting anything.
    fn pre_vote(
        &self,
        req: VoteRequest<TypeConfig>,
    ) -> BoxFuture<'static, Result<VoteResponse<TypeConfig>, RaftError<TypeConfig>>>;

    /// Install a completely received snapshot. `file` is a path local to the receiving node.
    fn install_snapshot(
        &self,
        vote: VoteOf<TypeConfig>,
        meta: SnapshotMetaOf<TypeConfig>,
        file: SnapshotFile,
    ) -> BoxFuture<'static, Result<SnapshotResponse<TypeConfig>, Fatal<TypeConfig>>>;
}

impl<SM> RaftHandler for Raft<TypeConfig, SM>
where
    SM: RaftStateMachine<TypeConfig, SnapshotData = SnapshotFile>,
{
    fn append_entries(
        &self,
        req: AppendEntriesRequest<TypeConfig>,
    ) -> BoxFuture<'static, Result<AppendResult<TypeConfig>, RaftError<TypeConfig>>> {
        let raft = self.clone();
        Box::pin(async move { Raft::append_entries(&raft, req).await })
    }

    fn stream_append(
        &self,
        reqs: BoxStream<'static, AppendEntriesRequest<TypeConfig>>,
    ) -> BoxStream<'static, Result<AppendResult<TypeConfig>, Fatal<TypeConfig>>> {
        Raft::stream_append(self, reqs).boxed()
    }

    fn vote(
        &self,
        req: VoteRequest<TypeConfig>,
    ) -> BoxFuture<'static, Result<VoteResponse<TypeConfig>, RaftError<TypeConfig>>> {
        let raft = self.clone();
        Box::pin(async move { Raft::vote(&raft, req).await })
    }

    fn pre_vote(
        &self,
        req: VoteRequest<TypeConfig>,
    ) -> BoxFuture<'static, Result<VoteResponse<TypeConfig>, RaftError<TypeConfig>>> {
        let raft = self.clone();
        Box::pin(async move { Raft::pre_vote(&raft, req).await })
    }

    fn install_snapshot(
        &self,
        vote: VoteOf<TypeConfig>,
        meta: SnapshotMetaOf<TypeConfig>,
        file: SnapshotFile,
    ) -> BoxFuture<'static, Result<SnapshotResponse<TypeConfig>, Fatal<TypeConfig>>> {
        let raft = self.clone();
        Box::pin(async move {
            let snapshot = Snapshot {
                meta,
                snapshot: file,
            };
            Raft::install_full_snapshot(&raft, vote, snapshot).await
        })
    }
}
