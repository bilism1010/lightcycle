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
use crate::snapshot::validate_file;

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

    /// Check a completely received snapshot file before it is handed to
    /// [`install_snapshot`](Self::install_snapshot). An `Err` is a refusal that the transport
    /// answers with a non-fatal failure and drops the file: a bad file from a faulty or hostile
    /// peer must never reach openraft, which treats a state machine error as fatal for the
    /// node. The default accepts everything (stubs that do not read the file).
    fn validate_snapshot(
        &self,
        _meta: &SnapshotMetaOf<TypeConfig>,
        _file: &SnapshotFile,
    ) -> BoxFuture<'static, Result<(), String>> {
        Box::pin(async { Ok(()) })
    }
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

    fn validate_snapshot(
        &self,
        meta: &SnapshotMetaOf<TypeConfig>,
        file: &SnapshotFile,
    ) -> BoxFuture<'static, Result<(), String>> {
        let expected = meta.clone();
        let path = file.path.clone();
        Box::pin(async move {
            let checked = tokio::task::spawn_blocking(move || match validate_file(&path) {
                Ok(header) if header == expected => Ok(()),
                Ok(_) => Err("snapshot header does not match the announced metadata".to_string()),
                Err(e) => Err(format!("invalid snapshot file: {e}")),
            })
            .await;
            checked.unwrap_or_else(|e| Err(format!("snapshot validation did not finish: {e}")))
        })
    }
}
