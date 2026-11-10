//! Cluster test harness helpers shared by the integration tests (plan 03-13).
//!
//! Each test target compiles this module separately, so unused helpers are expected.
#![allow(dead_code)]

use std::future::Future;
use std::path::Path;
use std::time::{Duration, Instant};

use lightcycle::raft::config::NodeConfig;
use lightcycle::raft::node::RaftNode;

/// A configuration with fast timers for tests: election 100-200 ms, heartbeat 20 ms, pre-vote on
/// and the default snapshot interval. `bootstrap` is off; tests designate the bootstrap node.
pub fn fast_config(node_id: u64, dir: &Path) -> NodeConfig {
    let mut cfg = NodeConfig::new(node_id, dir);
    cfg.election_timeout_min_ms = 100;
    cfg.election_timeout_max_ms = 200;
    cfg.heartbeat_interval_ms = 20;
    cfg.enable_pre_vote = true;
    cfg
}

/// Poll `f` every 10 ms until it returns true; panics with `what` after `timeout`.
pub async fn eventually<F, Fut>(timeout: Duration, what: &str, mut f: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = Instant::now() + timeout;
    loop {
        if f().await {
            return;
        }
        assert!(Instant::now() < deadline, "timed out after {timeout:?} waiting for: {what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Wait until every node reports the same leader (one of `nodes`) and return its id.
pub async fn wait_for_leader(nodes: &[&RaftNode], timeout: Duration) -> u64 {
    let ids: Vec<u64> = nodes.iter().map(|n| n.node_id()).collect();
    let mut found = 0;
    eventually(timeout, "all nodes agree on a leader", || {
        let leaders: Vec<Option<u64>> =
            nodes.iter().map(|n| n.metrics().current_leader).collect();
        let agreed = match leaders.first().copied().flatten() {
            Some(l) if ids.contains(&l) && leaders.iter().all(|x| *x == Some(l)) => Some(l),
            _ => None,
        };
        if let Some(l) = agreed {
            found = l;
        }
        async move { agreed.is_some() }
    })
    .await;
    found
}
