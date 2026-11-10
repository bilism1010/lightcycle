//! Node configuration and its mapping onto the openraft [`openraft::Config`] (plan 03-13).
//!
//! Defaults and why (decision D-10 and pitfalls 8, 10, 11 of the phase research):
//!
//! * Pre-vote is on: a node whose fsync stalls longer than the election timeout would otherwise
//!   force a term bump and a needless election when it recovers (pitfall 10, threat T-03-33).
//! * Election 150-300 ms with a 50 ms heartbeat; validation requires the minimum election timeout
//!   to be at least three heartbeats so one lost heartbeat never triggers an election.
//! * Snapshots every 1,000,000 applied entries. At the 500k ops/s target a 100k interval would
//!   snapshot every 0.2 s (pitfall 11).
//! * `install_snapshot_timeout` is 30 s: openraft's 200 ms default is far too small for a bulk
//!   transfer (pitfall 8); the transport additionally enforces a per-chunk idle timeout.
//! * `replication_lag_threshold` is twice the snapshot interval, so a follower that is only a
//!   little behind is replicated to from the log, and one that has fallen off the retained log
//!   gets a snapshot.

use std::collections::BTreeMap;
use std::path::PathBuf;

use openraft::SnapshotPolicy;

use crate::wal::WalConfig;

/// Smallest accepted WAL segment size in bytes.
const MIN_SEGMENT_SIZE: u64 = 4096;

/// Errors from validating a [`NodeConfig`] or parsing the peer list.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ConfigError {
    /// Node ids start at 1.
    #[error("node_id must be non-zero")]
    ZeroNodeId,
    /// The heartbeat interval must be positive.
    #[error("heartbeat_interval_ms must be non-zero")]
    ZeroHeartbeat,
    /// Election timeout minimum is too close to the heartbeat interval.
    #[error(
        "election_timeout_min_ms ({min}) must be at least 3 x heartbeat_interval_ms ({heartbeat})"
    )]
    ElectionTooCloseToHeartbeat {
        /// Configured minimum election timeout.
        min: u64,
        /// Configured heartbeat interval.
        heartbeat: u64,
    },
    /// Election timeout maximum must exceed the minimum.
    #[error("election_timeout_max_ms ({max}) must be greater than election_timeout_min_ms ({min})")]
    ElectionRange {
        /// Configured minimum.
        min: u64,
        /// Configured maximum.
        max: u64,
    },
    /// A snapshot interval of zero would snapshot continuously.
    #[error("snapshot_every_entries must be non-zero")]
    ZeroSnapshotInterval,
    /// A snapshot chunk size of zero can never make progress.
    #[error("snapshot_chunk_bytes must be non-zero")]
    ZeroChunkSize,
    /// The WAL segment size is below the supported minimum.
    #[error("wal segment_size ({0}) is below the minimum of 4096 bytes")]
    SegmentTooSmall(u64),
    /// The peer list contains this node's own id.
    #[error("peer list contains this node's own id {0}")]
    PeerIsSelf(u64),
    /// One `id=addr` entry of the peer list is malformed.
    #[error("invalid peer entry {entry:?}: {reason}")]
    BadPeer {
        /// The offending entry text.
        entry: String,
        /// What was wrong with it.
        reason: &'static str,
    },
    /// The peer list names the same id twice.
    #[error("duplicate peer id {0}")]
    DuplicatePeer(u64),
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

    /// True when the WAL fsync mode can lose acknowledged writes on power loss (decision D-04):
    /// exactly the `Periodic` and `Never` modes.
    pub fn is_unsafe(&self) -> bool {
        self.wal.fsync.is_unsafe()
    }

    /// Check the values that openraft itself does not (or checks only loosely).
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.node_id == 0 {
            return Err(ConfigError::ZeroNodeId);
        }
        if self.heartbeat_interval_ms == 0 {
            return Err(ConfigError::ZeroHeartbeat);
        }
        if self.election_timeout_min_ms < self.heartbeat_interval_ms.saturating_mul(3) {
            return Err(ConfigError::ElectionTooCloseToHeartbeat {
                min: self.election_timeout_min_ms,
                heartbeat: self.heartbeat_interval_ms,
            });
        }
        if self.election_timeout_max_ms <= self.election_timeout_min_ms {
            return Err(ConfigError::ElectionRange {
                min: self.election_timeout_min_ms,
                max: self.election_timeout_max_ms,
            });
        }
        if self.snapshot_every_entries == 0 {
            return Err(ConfigError::ZeroSnapshotInterval);
        }
        if self.snapshot_chunk_bytes == 0 {
            return Err(ConfigError::ZeroChunkSize);
        }
        if self.wal.segment_size < MIN_SEGMENT_SIZE {
            return Err(ConfigError::SegmentTooSmall(self.wal.segment_size));
        }
        if self.peers.contains_key(&self.node_id) {
            return Err(ConfigError::PeerIsSelf(self.node_id));
        }
        Ok(())
    }

    /// Validate, then map this configuration onto the openraft configuration.
    pub fn to_raft_config(&self) -> Result<openraft::Config, ConfigError> {
        self.validate()?;
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

/// Parse a static peer list of comma-separated `id=addr` pairs, e.g.
/// `"2=127.0.0.1:7444,3=127.0.0.1:7445"`.
///
/// Whitespace around ids and addresses is trimmed. An empty (or all-whitespace) string is an
/// empty peer list. Ids must be non-zero decimal integers and unique; addresses must be
/// non-empty. An empty entry (for example a trailing comma) is rejected.
pub fn parse_peers(s: &str) -> Result<BTreeMap<u64, String>, ConfigError> {
    let mut peers = BTreeMap::new();
    if s.trim().is_empty() {
        return Ok(peers);
    }
    for raw in s.split(',') {
        let bad = |reason| ConfigError::BadPeer { entry: raw.trim().to_string(), reason };
        let (id, addr) = raw.split_once('=').ok_or_else(|| bad("expected id=addr"))?;
        let id = id.trim();
        let addr = addr.trim();
        if id.is_empty() {
            return Err(bad("empty node id"));
        }
        let id: u64 = id.parse().map_err(|_| bad("node id is not a number"))?;
        if id == 0 {
            return Err(bad("node id must be non-zero"));
        }
        if addr.is_empty() {
            return Err(bad("empty address"));
        }
        if peers.insert(id, addr.to_string()).is_some() {
            return Err(ConfigError::DuplicatePeer(id));
        }
    }
    Ok(peers)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::Fsync;

    fn cfg() -> NodeConfig {
        NodeConfig::new(1, "/tmp/unused")
    }

    #[test]
    fn defaults_match_the_phase_values() {
        let c = cfg();
        assert!(c.enable_pre_vote);
        assert_eq!((c.election_timeout_min_ms, c.election_timeout_max_ms), (150, 300));
        assert_eq!(c.heartbeat_interval_ms, 50);
        assert_eq!(c.snapshot_every_entries, 1_000_000);
        assert_eq!(c.snapshot_chunk_bytes, 1024 * 1024);
        assert_eq!(c.install_snapshot_timeout_ms, 30_000);

        let raft = c.to_raft_config().unwrap();
        assert_eq!(raft.enable_pre_vote, Some(true));
        assert_eq!(raft.install_snapshot_timeout, 30_000);
        assert_eq!(raft.snapshot_max_chunk_size, 1024 * 1024);
        assert_eq!(raft.replication_lag_threshold, 2 * c.snapshot_every_entries);
        assert!(raft.replication_lag_threshold > c.snapshot_every_entries);
        assert_eq!(raft.snapshot_policy, SnapshotPolicy::LogsSinceLast(1_000_000));
    }

    #[test]
    fn test_sized_config_passes_openraft_validation() {
        let mut c = cfg();
        c.snapshot_every_entries = 50;
        c.max_in_snapshot_log_to_keep = 10;
        c.purge_batch_size = 10;
        c.wal.segment_size = 4096;
        let raft = c.to_raft_config().unwrap();
        assert_eq!(raft.snapshot_policy, SnapshotPolicy::LogsSinceLast(50));
        assert_eq!(raft.replication_lag_threshold, 100);
        assert_eq!(raft.max_in_snapshot_log_to_keep, 10);
        assert_eq!(raft.purge_batch_size, 10);
    }

    #[test]
    fn validate_rejects_bad_values() {
        let reject = |f: &dyn Fn(&mut NodeConfig), want: ConfigError| {
            let mut c = cfg();
            f(&mut c);
            assert_eq!(c.validate().unwrap_err(), want);
            assert!(c.to_raft_config().is_err());
        };
        reject(&|c| c.node_id = 0, ConfigError::ZeroNodeId);
        reject(&|c| c.heartbeat_interval_ms = 0, ConfigError::ZeroHeartbeat);
        // 3 x 50 = 150 is the boundary; 149 is rejected.
        reject(
            &|c| c.election_timeout_min_ms = 149,
            ConfigError::ElectionTooCloseToHeartbeat { min: 149, heartbeat: 50 },
        );
        reject(
            &|c| c.election_timeout_max_ms = 100,
            ConfigError::ElectionRange { min: 150, max: 100 },
        );
        reject(
            &|c| c.election_timeout_max_ms = 150,
            ConfigError::ElectionRange { min: 150, max: 150 },
        );
        reject(&|c| c.snapshot_every_entries = 0, ConfigError::ZeroSnapshotInterval);
        reject(&|c| c.snapshot_chunk_bytes = 0, ConfigError::ZeroChunkSize);
        reject(&|c| c.wal.segment_size = 4095, ConfigError::SegmentTooSmall(4095));
        reject(
            &|c| {
                c.peers.insert(1, "127.0.0.1:1".to_string());
            },
            ConfigError::PeerIsSelf(1),
        );
    }

    #[test]
    fn validate_accepts_boundaries() {
        let mut c = cfg();
        c.election_timeout_min_ms = 150;
        c.heartbeat_interval_ms = 50;
        c.wal.segment_size = 4096;
        c.peers.insert(2, "127.0.0.1:2".to_string());
        c.validate().unwrap();
    }

    #[test]
    fn parse_peers_accepts_pairs() {
        let p = parse_peers("2=127.0.0.1:7444,3=127.0.0.1:7445").unwrap();
        assert_eq!(p.len(), 2);
        assert_eq!(p[&2], "127.0.0.1:7444");
        assert_eq!(p[&3], "127.0.0.1:7445");

        let p = parse_peers("  2 = host-a:7444 ,\t3= host-b:7445  ").unwrap();
        assert_eq!(p[&2], "host-a:7444");
        assert_eq!(p[&3], "host-b:7445");
    }

    #[test]
    fn parse_peers_empty_is_empty_map() {
        assert!(parse_peers("").unwrap().is_empty());
        assert!(parse_peers("   ").unwrap().is_empty());
    }

    #[test]
    fn parse_peers_rejects_malformed() {
        for bad in [
            "=127.0.0.1:1",     // empty id
            "0=127.0.0.1:1",    // id 0
            "x=127.0.0.1:1",    // non-numeric id
            "-1=127.0.0.1:1",   // negative id
            "2",                // missing equals
            "2=",               // empty address
            "2=   ",            // whitespace-only address
            "2=a:1,",           // trailing empty entry
            "2=a:1,,3=b:2",     // empty middle entry
        ] {
            assert!(
                matches!(parse_peers(bad), Err(ConfigError::BadPeer { .. })),
                "{bad:?} should be rejected as a bad peer"
            );
        }
        assert_eq!(parse_peers("2=a:1,2=b:2").unwrap_err(), ConfigError::DuplicatePeer(2));
    }

    #[test]
    fn is_unsafe_only_for_relaxed_fsync_modes() {
        let mut c = cfg();
        c.wal.fsync = Fsync::Always;
        assert!(!c.is_unsafe());
        c.wal.fsync = Fsync::Periodic { interval_ms: 10 };
        assert!(c.is_unsafe());
        c.wal.fsync = Fsync::Never;
        assert!(c.is_unsafe());
    }
}
