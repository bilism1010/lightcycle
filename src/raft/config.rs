//! Node configuration and its mapping onto the openraft [`openraft::Config`] (plan 03-13).
//!
//! Validation rules, the peer-list parser and the unit tests arrive with the second task of the
//! plan; this first cut carries the struct, the defaults and the mapping the end-to-end tracer
//! needs.

use std::collections::BTreeMap;
use std::path::PathBuf;

use openraft::SnapshotPolicy;

use crate::wal::WalConfig;

/// Errors from validating a [`NodeConfig`].
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// openraft rejected the derived configuration.
    #[error("invalid raft configuration: {0}")]
    Raft(String),
}

/// Everything one Lightcycle node needs to start.
#[derive(Debug, Clone)]
pub struct NodeConfig {
    /// This node's id (non-zero).
    pub node_id: u64,
    /// Cluster name, used only for log context.
    pub cluster_id: String,
    /// Address this node listens on for Raft traffic.
    pub bind: String,
    /// Static peer list: node id to address (excludes this node).
    pub peers: BTreeMap<u64, String>,
    /// True on the one node designated to initialize a brand-new cluster.
    pub bootstrap: bool,
    /// Root data directory (WAL, snapshots and the receive scratch directory live below it).
    pub data_dir: PathBuf,
    /// WAL tunables.
    pub wal: WalConfig,
    /// Lower bound of the randomized election timeout, ms.
    pub election_timeout_min_ms: u64,
    /// Upper bound of the randomized election timeout, ms.
    pub election_timeout_max_ms: u64,
    /// Leader heartbeat interval, ms.
    pub heartbeat_interval_ms: u64,
    /// Pre-vote on: a partitioned or stalled node cannot disrupt a healthy leader.
    pub enable_pre_vote: bool,
    /// Applied entries between automatic snapshots.
    pub snapshot_every_entries: u64,
    /// Snapshot transfer chunk size in bytes.
    pub snapshot_chunk_bytes: u64,
    /// Whole-install timeout for a snapshot transfer, ms.
    pub install_snapshot_timeout_ms: u64,
    /// Log entries kept behind the snapshot before purging.
    pub max_in_snapshot_log_to_keep: u64,
    /// Purge granularity in entries.
    pub purge_batch_size: u64,
    /// Max entries per AppendEntries.
    pub max_append_entries: u64,
    /// Max entries per replication payload.
    pub max_payload_entries: u64,
}

impl NodeConfig {
    /// A configuration with the phase defaults for node `node_id` storing data below `data_dir`.
    pub fn new(node_id: u64, data_dir: impl Into<PathBuf>) -> Self {
        NodeConfig {
            node_id,
            cluster_id: "lightcycle".to_string(),
            bind: "127.0.0.1:7443".to_string(),
            peers: BTreeMap::new(),
            bootstrap: false,
            data_dir: data_dir.into(),
            wal: WalConfig::default(),
            election_timeout_min_ms: 150,
            election_timeout_max_ms: 300,
            heartbeat_interval_ms: 50,
            enable_pre_vote: true,
            snapshot_every_entries: 1_000_000,
            snapshot_chunk_bytes: 1024 * 1024,
            install_snapshot_timeout_ms: 30_000,
            max_in_snapshot_log_to_keep: 1000,
            purge_batch_size: 65_536,
            max_append_entries: 4096,
            max_payload_entries: 300,
        }
    }

    /// Directory holding the WAL segments.
    pub fn wal_dir(&self) -> PathBuf {
        self.data_dir.join("wal")
    }

    /// Directory holding snapshot files.
    pub fn snapshot_dir(&self) -> PathBuf {
        self.data_dir.join("snapshots")
    }

    /// Scratch directory the transport receives snapshot transfers into.
    pub fn recv_dir(&self) -> PathBuf {
        self.data_dir.join("recv")
    }

    /// True when the WAL fsync mode can lose acknowledged writes on power loss (decision D-04).
    pub fn is_unsafe(&self) -> bool {
        self.wal.fsync.is_unsafe()
    }

    /// Map this configuration onto the openraft configuration.
    pub fn to_raft_config(&self) -> Result<openraft::Config, ConfigError> {
        let cfg = openraft::Config {
            cluster_name: self.cluster_id.clone(),
            election_timeout_min: self.election_timeout_min_ms,
            election_timeout_max: self.election_timeout_max_ms,
            heartbeat_interval: self.heartbeat_interval_ms,
            enable_pre_vote: Some(self.enable_pre_vote),
            install_snapshot_timeout: self.install_snapshot_timeout_ms,
            snapshot_policy: SnapshotPolicy::LogsSinceLast(self.snapshot_every_entries),
            snapshot_max_chunk_size: self.snapshot_chunk_bytes,
            replication_lag_threshold: self.snapshot_every_entries.saturating_mul(2),
            max_in_snapshot_log_to_keep: self.max_in_snapshot_log_to_keep,
            purge_batch_size: self.purge_batch_size,
            max_append_entries: Some(self.max_append_entries),
            max_payload_entries: self.max_payload_entries,
            ..Default::default()
        };
        cfg.validate().map_err(|e| ConfigError::Raft(e.to_string()))
    }
}
