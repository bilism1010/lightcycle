//! The assembled Raft node (plan 03-13): WAL log store, stamped state machine, snapshot store
//! and a pluggable network under one real openraft core.
//!
//! Startup order matters. Everything that must be read off the WAL store or the state machine
//! (recovery statistics, WAL counters, the shared engine handle) is captured *before* they are
//! moved into `Raft::new`. The node never initializes a cluster implicitly: bootstrapping is the
//! explicit, idempotent [`RaftNode::bootstrap_if_needed`], because re-initializing a restarted
//! node is an error (threat T-03-32).

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::sync::Arc;

use openraft::errors::{ClientWriteError, RaftError};
use openraft::network::NetSnapshot;
use openraft::async_runtime::watch::WatchReceiver;
use openraft::{BasicNode, Raft, RaftMetrics, RaftNetworkFactory};

use super::config::{ConfigError, NodeConfig};
use super::state_machine::{EngineHandle, KvStateMachine};
use super::types::{KvRequest, KvResponse, SnapshotFile, TypeConfig};
use crate::engine::LocalEngine;
use crate::wal::io::{SegmentIo, StdFsIo};
use crate::wal::recovery::RecoveryStats;
use crate::wal::store::{max_entry_bytes, WalLogStore};
use crate::wal::writer::WalStats;
use crate::wal::WalError;

/// Errors surfaced by [`RaftNode`].
#[derive(Debug, thiserror::Error)]
pub enum NodeError {
    /// The node configuration is invalid.
    #[error(transparent)]
    Config(#[from] ConfigError),
    /// The WAL could not be opened or recovered.
    #[error(transparent)]
    Wal(#[from] WalError),
    /// A filesystem or state machine I/O error during startup.
    #[error("node i/o error: {0}")]
    Io(#[from] io::Error),
    /// This node is not the leader; `leader` is the current leader when known.
    #[error("not the leader (leader: {leader:?})")]
    NotLeader {
        /// Current leader id, if known.
        leader: Option<u64>,
    },
    /// The Raft core has stopped or hit a fatal storage error.
    #[error("raft core stopped: {0}")]
    Fatal(String),
    /// A Raft operation was rejected.
    #[error("raft error: {0}")]
    Raft(String),
    /// The command carries more data than one log entry can hold; it was not proposed.
    #[error("request carries {size} bytes, above the {max} byte limit")]
    RequestTooLarge {
        /// Bytes of keys and values in the rejected command.
        size: usize,
        /// Largest accepted command payload.
        max: usize,
    },
}

type RaftHandle = Raft<TypeConfig, KvStateMachine>;

/// One running Raft node.
pub struct RaftNode {
    raft: RaftHandle,
    /// The shared swappable engine handle (never a loaded engine: see [`RaftNode::engine`]).
    engine_handle: EngineHandle,
    recovery: RecoveryStats,
    wal_stats: Arc<WalStats>,
    node_id: u64,
    bootstrap: bool,
    /// Largest command payload one log entry can hold (see [`max_entry_bytes`]).
    max_request_bytes: usize,
}

/// Space reserved in a log entry for everything but the command's keys and values (log id,
/// variant tags, length prefixes).
const ENTRY_ENVELOPE_RESERVE: usize = 1024;

fn join_io(e: tokio::task::JoinError) -> NodeError {
    NodeError::Io(io::Error::other(e.to_string()))
}

impl RaftNode {
    /// Start a node over the real filesystem WAL under `cfg.wal_dir()`.
    pub async fn start<N>(cfg: NodeConfig, network: N) -> Result<RaftNode, NodeError>
    where
        N: RaftNetworkFactory<TypeConfig>,
        N::Network: NetSnapshot<TypeConfig, SnapshotData = SnapshotFile>,
    {
        // Before anything touches the disk: a rejected config must leave no directories behind.
        cfg.validate()?;
        let wal_dir = cfg.wal_dir();
        let io = tokio::task::spawn_blocking(move || StdFsIo::new(wal_dir))
            .await
            .map_err(join_io)??;
        Self::start_with_io(cfg, network, Box::new(io)).await
    }

    /// Start a node over an arbitrary WAL backend (tests inject a fake).
    pub async fn start_with_io<N>(
        cfg: NodeConfig,
        network: N,
        io: Box<dyn SegmentIo>,
    ) -> Result<RaftNode, NodeError>
    where
        N: RaftNetworkFactory<TypeConfig>,
        N::Network: NetSnapshot<TypeConfig, SnapshotData = SnapshotFile>,
    {
        let raft_config = Arc::new(cfg.to_raft_config()?);
        if cfg.is_unsafe() {
            tracing::warn!(
                node = cfg.node_id,
                fsync = ?cfg.wal.fsync,
                "UNSAFE wal fsync mode: acknowledged writes can be lost on power failure"
            );
        }

        let recv_dir = cfg.recv_dir();
        tokio::task::spawn_blocking(move || clean_recv_dir(&recv_dir)).await.map_err(join_io)??;

        let state_machine = KvStateMachine::open(cfg.snapshot_dir()).await?;
        let engine_handle = state_machine.engine_handle();

        let wal_cfg = cfg.wal.clone();
        let max_request_bytes = max_entry_bytes(&wal_cfg).saturating_sub(ENTRY_ENVELOPE_RESERVE);
        let log_store = tokio::task::spawn_blocking(move || WalLogStore::open_with_io(io, wal_cfg))
            .await
            .map_err(join_io)??;
        // Both are read before the store is moved into the Raft core.
        let recovery = log_store.recovery_stats();
        let wal_stats = log_store.wal_stats();

        let raft = Raft::new(cfg.node_id, raft_config, network, log_store, state_machine)
            .await
            .map_err(fatal)?;

        Ok(RaftNode {
            raft,
            engine_handle,
            recovery,
            wal_stats,
            node_id: cfg.node_id,
            bootstrap: cfg.bootstrap,
            max_request_bytes,
        })
    }

    /// The underlying Raft handle (RPC handlers, triggers, waits).
    pub fn raft(&self) -> &RaftHandle {
        &self.raft
    }

    /// The engine as of this call.
    ///
    /// The returned `Arc` is a point-in-time view: when a snapshot is installed the state
    /// machine swaps a new engine into the shared handle, and the next call returns that one.
    /// Call this each time current data is needed and do not hold the result across a point
    /// where a snapshot may install.
    pub fn engine(&self) -> Arc<LocalEngine> {
        self.engine_handle.load_full()
    }

    /// A clone of the shared engine handle (the same `ArcSwap` the state machine swaps).
    pub fn engine_handle(&self) -> EngineHandle {
        Arc::clone(&self.engine_handle)
    }

    /// What WAL recovery found when this node started.
    pub fn recovery_stats(&self) -> RecoveryStats {
        self.recovery
    }

    /// WAL writer counters.
    pub fn wal_stats(&self) -> Arc<WalStats> {
        Arc::clone(&self.wal_stats)
    }

    /// This node's id.
    pub fn node_id(&self) -> u64 {
        self.node_id
    }

    /// A snapshot of the current Raft metrics (leader, applied index, snapshot and purge ids).
    pub fn metrics(&self) -> RaftMetrics<TypeConfig> {
        self.raft.metrics().borrow_watched().clone()
    }

    /// Initialize a brand-new cluster with `members` if this is the designated bootstrap node and
    /// the cluster is not yet initialized. Returns whether it initialized. Never called
    /// implicitly: re-initializing a restarted node is an error.
    pub async fn bootstrap_if_needed(
        &self,
        members: BTreeMap<u64, BasicNode>,
    ) -> Result<bool, NodeError> {
        if !self.bootstrap {
            return Ok(false);
        }
        if self.raft.is_initialized().await.map_err(fatal)? {
            return Ok(false);
        }
        self.raft.initialize(members).await.map_err(|e| match e {
            RaftError::APIError(e) => NodeError::Raft(e.to_string()),
            RaftError::Fatal(f) => fatal(f),
        })?;
        Ok(true)
    }

    /// Replicate a client command and wait for it to apply.
    ///
    /// A command too large for one WAL record is rejected here with
    /// [`NodeError::RequestTooLarge`]: proposing it would make the log store fail, and openraft
    /// treats a storage failure as fatal for the whole node.
    pub async fn write(&self, req: KvRequest) -> Result<KvResponse, NodeError> {
        let size = req.payload_len();
        if size > self.max_request_bytes {
            return Err(NodeError::RequestTooLarge {
                size,
                max: self.max_request_bytes,
            });
        }
        match self.raft.client_write(req).await {
            Ok(resp) => Ok(resp.data),
            Err(e) => Err(map_write_error(e)),
        }
    }

    /// Add `id` at `addr` as a learner and wait for it to catch up.
    pub async fn add_learner(&self, id: u64, addr: impl Into<String>) -> Result<(), NodeError> {
        self.raft
            .add_learner(id, BasicNode { addr: addr.into() }, true)
            .await
            .map(|_| ())
            .map_err(map_write_error)
    }

    /// Change the voter set to `voters`. With `retain`, removed voters stay as learners;
    /// without it they are dropped from the cluster.
    pub async fn change_membership(
        &self,
        voters: BTreeSet<u64>,
        retain: bool,
    ) -> Result<(), NodeError> {
        self.raft.change_membership(voters, retain).await.map(|_| ()).map_err(map_write_error)
    }

    /// Remove `ids` from the voter set and from the cluster (`retain = false`).
    pub async fn remove_voters(&self, ids: &BTreeSet<u64>) -> Result<(), NodeError> {
        let remaining: BTreeSet<u64> = self.raft.voter_ids().filter(|v| !ids.contains(v)).collect();
        self.change_membership(remaining, false).await
    }

    /// Stop the Raft core. The core drops the WAL log store, which drains and joins the writer.
    pub async fn shutdown(self) -> Result<(), NodeError> {
        self.raft.shutdown().await.map_err(fatal)
    }
}

fn fatal(e: impl std::fmt::Display) -> NodeError {
    NodeError::Fatal(e.to_string())
}

fn map_write_error(e: RaftError<TypeConfig, ClientWriteError<TypeConfig>>) -> NodeError {
    match e {
        RaftError::APIError(ClientWriteError::ForwardToLeader(f))
        | RaftError::APIError(ClientWriteError::LogEntryDiscarded(f)) => {
            NodeError::NotLeader { leader: f.leader_id }
        }
        RaftError::APIError(other) => NodeError::Raft(other.to_string()),
        RaftError::Fatal(f) => fatal(f),
    }
}

/// Create the receive scratch directory and remove transfer leftovers from a crashed process
/// (`recv-<n>.tmp`); the transport's in-process guard cannot cover a crash.
fn clean_recv_dir(dir: &std::path::Path) -> io::Result<()> {
    use std::os::unix::fs::DirBuilderExt;
    std::fs::DirBuilder::new().recursive(true).mode(0o700).create(dir)?;
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with("recv-") && name.ends_with(".tmp") && entry.file_type()?.is_file() {
            std::fs::remove_file(entry.path())?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clean_recv_dir_removes_only_transfer_leftovers() {
        let dir = tempfile::tempdir().unwrap();
        let recv = dir.path().join("recv");
        std::fs::create_dir_all(&recv).unwrap();
        std::fs::write(recv.join("recv-12.tmp"), b"x").unwrap();
        std::fs::write(recv.join("keep.dat"), b"x").unwrap();
        clean_recv_dir(&recv).unwrap();
        assert!(!recv.join("recv-12.tmp").exists());
        assert!(recv.join("keep.dat").exists());
        // Creates the directory when missing.
        clean_recv_dir(&dir.path().join("fresh")).unwrap();
        assert!(dir.path().join("fresh").is_dir());
    }
}
