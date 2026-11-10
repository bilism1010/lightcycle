//! Permanent compile-time pin of the openraft `=0.10.0-alpha.38` trait surface that the
//! Phase 3 design depends on (decision A1). If a version bump removes `stream_append`,
//! `new_heartbeat_client`, `new_snapshot_client`, or seals the storage traits, this file stops
//! compiling. The skeleton method bodies are never executed.

#![allow(clippy::all)]

use std::future::Future;
use std::io;
use std::ops::RangeBounds;
use std::sync::Arc;

use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::Stream;
use lightcycle::raft::types::{SnapshotFile, TypeConfig};
use openraft::errors::{RPCError, ReplicationClosed, StreamingError};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendResult, SnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::storage::{EntryResponder, IOFlushed, RaftLogStorage, RaftStateMachine};
use openraft::type_config::alias::{
    EntryOf, LogIdOf, SnapshotMetaOf, SnapshotOf, StoredMembershipOf, VoteOf,
};
use openraft::{
    BasicNode, Config, LogState, OptionalSend, Raft, RaftLogReader, RaftNetworkFactory,
    RaftNetworkV2, RaftSnapshotBuilder,
};

fn skeleton() -> io::Error {
    io::Error::other("api gate skeleton")
}

// ---- log storage ----------------------------------------------------------------------------

#[derive(Clone)]
struct GateLogReader;

impl RaftLogReader<TypeConfig> for GateLogReader {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + std::fmt::Debug + OptionalSend>(
        &mut self,
        _range: RB,
    ) -> Result<Vec<EntryOf<TypeConfig>>, io::Error> {
        Ok(Vec::new())
    }

    async fn read_vote(&mut self) -> Result<Option<VoteOf<TypeConfig>>, io::Error> {
        Ok(None)
    }
}

struct GateLog;

impl RaftLogStorage<TypeConfig> for GateLog {
    type LogReader = GateLogReader;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, io::Error> {
        Ok(LogState {
            last_purged_log_id: None,
            last_log_id: None,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        GateLogReader
    }

    async fn save_vote(&mut self, _vote: &VoteOf<TypeConfig>) -> Result<(), io::Error> {
        Ok(())
    }

    async fn append<I>(
        &mut self,
        _entries: I,
        _callback: IOFlushed<TypeConfig>,
    ) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = EntryOf<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        Err(skeleton())
    }

    async fn truncate_after(
        &mut self,
        _last_log_id: Option<LogIdOf<TypeConfig>>,
    ) -> Result<(), io::Error> {
        Ok(())
    }

    async fn purge(&mut self, _log_id: LogIdOf<TypeConfig>) -> Result<(), io::Error> {
        Ok(())
    }
}

// ---- state machine --------------------------------------------------------------------------

struct GateSnapshotBuilder;

impl RaftSnapshotBuilder<TypeConfig> for GateSnapshotBuilder {
    type SnapshotData = SnapshotFile;

    async fn build_snapshot(&mut self) -> Result<SnapshotOf<TypeConfig, SnapshotFile>, io::Error> {
        Err(skeleton())
    }
}

struct GateSm;

impl RaftStateMachine<TypeConfig> for GateSm {
    type SnapshotData = SnapshotFile;
    type SnapshotBuilder = GateSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogIdOf<TypeConfig>>, StoredMembershipOf<TypeConfig>), io::Error> {
        Ok((None, Default::default()))
    }

    async fn apply<Strm>(&mut self, _entries: Strm) -> Result<(), io::Error>
    where
        Strm: Stream<Item = Result<EntryResponder<TypeConfig>, io::Error>> + Unpin + OptionalSend,
    {
        Ok(())
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        GateSnapshotBuilder
    }

    async fn install_snapshot(
        &mut self,
        _meta: &SnapshotMetaOf<TypeConfig>,
        _snapshot: Self::SnapshotData,
    ) -> Result<(), io::Error> {
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<SnapshotOf<TypeConfig, Self::SnapshotData>>, io::Error> {
        Ok(None)
    }
}

// ---- network --------------------------------------------------------------------------------

struct GatePeer;

impl RaftNetworkV2<TypeConfig> for GatePeer {
    type SnapshotData = SnapshotFile;

    async fn append_entries(
        &mut self,
        _rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendResult<TypeConfig>, RPCError<TypeConfig>> {
        unimplemented!("api gate skeleton")
    }

    fn stream_append<'s, S>(
        &'s mut self,
        _input: S,
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
        Box::pin(async { unimplemented!("api gate skeleton") })
    }

    async fn vote(
        &mut self,
        _rpc: VoteRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        unimplemented!("api gate skeleton")
    }

    async fn full_snapshot(
        &mut self,
        _vote: VoteOf<TypeConfig>,
        _snapshot: SnapshotOf<TypeConfig, Self::SnapshotData>,
        _cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
        unimplemented!("api gate skeleton")
    }
}

struct GateFactory;

impl RaftNetworkFactory<TypeConfig> for GateFactory {
    type Network = GatePeer;

    async fn new_client(&mut self, _target: u64, _node: &BasicNode) -> Self::Network {
        GatePeer
    }

    async fn new_heartbeat_client(&mut self, _target: u64, _node: &BasicNode) -> Self::Network {
        GatePeer
    }

    async fn new_snapshot_client(&mut self, _target: u64, _node: &BasicNode) -> Self::Network {
        GatePeer
    }
}

// ---- type-check-only proofs -----------------------------------------------------------------

/// Only type-checked: proves `Raft::new` accepts the three skeletons.
async fn raft_new_accepts_skeletons(config: Arc<Config>) -> Raft<TypeConfig, GateSm> {
    Raft::new(1, config, GateFactory, GateLog, GateSm)
        .await
        .expect("never run")
}

/// Takes the Raft handle by value so the generic bound is exercised.
fn takes_raft(_raft: Raft<TypeConfig, GateSm>) {}

#[allow(dead_code)]
fn never_invoked(config: Arc<Config>) {
    let _fut = async move {
        let raft = raft_new_accepts_skeletons(config).await;
        takes_raft(raft);
    };
}

// ---- runtime tests --------------------------------------------------------------------------

#[test]
fn openraft_config_literal_validates() {
    let cfg = Config {
        election_timeout_min: 150,
        election_timeout_max: 300,
        heartbeat_interval: 50,
        enable_pre_vote: Some(true),
        ..Default::default()
    };
    cfg.validate().expect("config must validate");
}

#[test]
fn test_util_visible_to_integration_tests() {
    assert!(lightcycle::test_support::ENABLED);
}
