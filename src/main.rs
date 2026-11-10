use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use clap::{Parser, ValueEnum};
use lightcycle::net::client::{TcpNetConfig, TcpNetworkFactory};
use lightcycle::net::server::PeerServer;
use lightcycle::raft::config::{parse_peers, NodeConfig};
use lightcycle::raft::node::RaftNode;
use lightcycle::wal::Fsync;
use openraft::{BasicNode, ServerState};

/// Sync interval used by `--fsync periodic`, in milliseconds.
const PERIODIC_FSYNC_MS: u64 = 10;

/// WAL durability mode selectable from the command line.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum FsyncMode {
    /// Every acknowledged write is durable first (default, safe).
    Always,
    /// UNSAFE: sync at most every 10 ms; acknowledged writes can be lost on power failure.
    Periodic,
    /// UNSAFE: never sync; for benchmarking only.
    Never,
}

impl From<FsyncMode> for Fsync {
    fn from(mode: FsyncMode) -> Fsync {
        match mode {
            FsyncMode::Always => Fsync::Always,
            FsyncMode::Periodic => Fsync::Periodic { interval_ms: PERIODIC_FSYNC_MS },
            FsyncMode::Never => Fsync::Never,
        }
    }
}

#[derive(Parser, Debug)]
#[command(name = "lightcycle", author, version, about = "Project Lightcycle - Sub-millisecond Raft-replicated in-memory cache")]
pub struct Config {
    #[arg(short, long, default_value_t = 1)]
    pub node_id: u64,

    /// Address for the Raft peer port. The port is plaintext and unauthenticated: bind it to a
    /// private interface only (see the README).
    #[arg(short, long, default_value = "127.0.0.1:7443")]
    pub bind: String,

    /// Address other nodes use to reach this one, published in the cluster membership when this
    /// node bootstraps a multi-node cluster. Defaults to the bound address, which must then not
    /// be a wildcard (`0.0.0.0`, `[::]`): a wildcard is not routable from other hosts. Restarts
    /// and standalone nodes publish nothing and do not need it.
    #[arg(long)]
    pub advertise: Option<String>,

    /// Static peer list as comma-separated id=addr pairs, e.g. "2=127.0.0.1:7444,3=127.0.0.1:7445"
    /// (see lightcycle::raft::config::parse_peers).
    #[arg(long, default_value = "")]
    pub peers: String,

    /// Directory for the WAL, snapshots and the snapshot receive scratch area
    /// [default: ./lightcycle-data/node-<node-id>].
    #[arg(long)]
    pub data_dir: Option<PathBuf>,

    /// Cluster identifier; nodes with different values refuse each other's connections.
    #[arg(long, default_value_t = 1)]
    pub cluster_id: u64,

    /// Designate this node to initialize a brand-new cluster (once; later starts are no-ops).
    /// Implied when no peers are given (a standalone single-node cluster; a warning is printed).
    #[arg(long)]
    pub bootstrap: bool,

    /// WAL fsync mode. `periodic` and `never` are UNSAFE and for benchmarking only.
    #[arg(long, value_enum, default_value_t = FsyncMode::Always)]
    pub fsync: FsyncMode,

    /// Applied entries between automatic snapshots.
    #[arg(long, default_value_t = 1_000_000)]
    pub snapshot_every: u64,
}

impl Config {
    fn data_dir(&self) -> PathBuf {
        self.data_dir
            .clone()
            .unwrap_or_else(|| PathBuf::from(format!("./lightcycle-data/node-{}", self.node_id)))
    }
}

/// The address this node publishes into the cluster membership.
///
/// An explicit `advertise` wins (a port of 0 takes the real bound port); otherwise the bound
/// address is used. A wildcard IP is never advertised: peers would dial it as their own loopback.
fn advertised_addr(advertise: Option<&str>, local: SocketAddr) -> anyhow::Result<String> {
    let Some(advertise) = advertise else {
        anyhow::ensure!(
            !local.ip().is_unspecified(),
            "cannot advertise the wildcard address {local}; pass --advertise <routable host:port>"
        );
        return Ok(local.to_string());
    };
    if let Ok(mut addr) = advertise.parse::<SocketAddr>() {
        anyhow::ensure!(
            !addr.ip().is_unspecified(),
            "cannot advertise the wildcard address {addr}; pass a routable --advertise host:port"
        );
        if addr.port() == 0 {
            addr.set_port(local.port());
        }
        return Ok(addr.to_string());
    }
    let (host, port) = advertise
        .rsplit_once(':')
        .filter(|(host, port)| !host.is_empty() && port.parse::<u16>().is_ok())
        .context("--advertise must be host:port")?;
    let port: u16 = port.parse().context("--advertise port")?;
    let port = if port == 0 { local.port() } else { port };
    Ok(format!("{host}:{port}"))
}

/// The address this node puts into the membership it may initialize.
///
/// Only a start that initializes a multi-node cluster publishes an address other nodes must be
/// able to dial, so only that start validates it. A one-member cluster (`standalone`) has no peer
/// to dial this node, and a start that does not initialize publishes nothing; both use the bound
/// address as is. An explicit `--advertise` is still validated and honored when initializing.
fn published_addr(
    will_initialize: bool,
    standalone: bool,
    advertise: Option<&str>,
    local: SocketAddr,
) -> anyhow::Result<String> {
    if !will_initialize || (standalone && advertise.is_none()) {
        return Ok(local.to_string());
    }
    advertised_addr(advertise, local)
}

/// Resolve when SIGINT (ctrl-c) or, on unix, SIGTERM arrives.
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        match signal(SignalKind::terminate()) {
            Ok(mut term) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = term.recv() => {}
                }
            }
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let config = Config::parse();

    let peers = parse_peers(&config.peers).context("invalid --peers")?;
    // A standalone node (no peers) always initializes its own single-member cluster, so it goes
    // through the same code path, WAL included, as a clustered node (decision D-14).
    let bootstrap = config.bootstrap || peers.is_empty();
    if peers.is_empty() && !config.bootstrap {
        // An empty peer list is easy to produce by accident (a typo, a template that expanded to
        // nothing, a node meant to join later). A fresh data directory would then start its own
        // independent cluster, which can never be merged into the real one: say so loudly.
        eprintln!(
            "[Lightcycle] WARNING: no --peers given, so node {} will initialize a standalone \
             one-node cluster if {} holds no cluster state. If this node is meant to join an \
             existing cluster, stop now and pass --peers.",
            config.node_id,
            config.data_dir().display()
        );
    }

    let mut cfg = NodeConfig::new(config.node_id, config.data_dir());
    cfg.cluster_id = config.cluster_id.to_string();
    cfg.bind = config.bind.clone();
    cfg.peers = peers.clone();
    cfg.bootstrap = bootstrap;
    cfg.wal.fsync = config.fsync.into();
    cfg.snapshot_every_entries = config.snapshot_every;
    if cfg.is_unsafe() {
        eprintln!(
            "[Lightcycle] WARNING: --fsync {:?} is UNSAFE: acknowledged writes can be lost on power failure. Benchmarking only.",
            config.fsync
        );
    }

    let net_cfg = TcpNetConfig::new(config.cluster_id, config.node_id, cfg.recv_dir());
    let node = RaftNode::start(cfg.clone(), TcpNetworkFactory::new(net_cfg.clone()))
        .await
        .context("failed to start the raft node")?;

    // The server must be bound before bootstrap so the membership carries the real address.
    let server = match PeerServer::bind(&config.bind, net_cfg, Arc::new(node.raft().clone())).await
    {
        Ok(server) => server,
        Err(e) => {
            let _ = node.shutdown().await;
            return Err(e).with_context(|| format!("failed to bind {}", config.bind));
        }
    };
    let local_addr = server.local_addr();

    let mut members: BTreeMap<u64, BasicNode> = peers
        .into_iter()
        .map(|(id, addr)| (id, BasicNode { addr }))
        .collect();
    // The membership is written only when this start actually initializes a cluster, so the
    // address check runs only then: a restart of a bootstrap node, or a start that finds the
    // cluster already initialized, publishes nothing.
    let will_initialize = if bootstrap {
        match node.raft().is_initialized().await {
            Ok(initialized) => !initialized,
            Err(e) => {
                server.shutdown().await;
                let _ = node.shutdown().await;
                return Err(anyhow::anyhow!("{e}")).context("could not read the cluster state");
            }
        }
    } else {
        false
    };
    let advertise = match published_addr(
        will_initialize,
        members.is_empty(),
        config.advertise.as_deref(),
        local_addr,
    ) {
        Ok(addr) => addr,
        Err(e) => {
            server.shutdown().await;
            let _ = node.shutdown().await;
            return Err(e);
        }
    };
    members.insert(config.node_id, BasicNode { addr: advertise });
    match node.bootstrap_if_needed(members).await {
        Ok(true) if config.peers.is_empty() => println!(
            "[Lightcycle] node {} initialized a new standalone single-node cluster",
            config.node_id
        ),
        Ok(true) => println!("[Lightcycle] node {} initialized a new cluster", config.node_id),
        Ok(false) => {}
        Err(e) => {
            server.shutdown().await;
            let _ = node.shutdown().await;
            return Err(e).context("cluster bootstrap failed");
        }
    }

    println!("[Lightcycle] node {} listening on {}", config.node_id, local_addr);

    let leader_watch = {
        let raft = node.raft().clone();
        let id = config.node_id;
        tokio::spawn(async move {
            // Report each time this node becomes leader; ends when the Raft core stops.
            loop {
                let wait = raft.wait(Some(Duration::from_secs(3600)));
                match wait.state(ServerState::Leader, "this node becomes leader").await {
                    Ok(m) => {
                        println!("[Lightcycle] node {} is leader (term {})", id, m.current_term);
                        let wait = raft.wait(Some(Duration::from_secs(3600)));
                        match wait
                            .metrics(|m| m.state != ServerState::Leader, "leadership lost")
                            .await
                        {
                            Ok(_) => {}
                            Err(openraft::metrics::WaitError::Timeout(..)) => {}
                            Err(_) => break,
                        }
                    }
                    Err(openraft::metrics::WaitError::Timeout(..)) => {}
                    Err(_) => break,
                }
            }
        })
    };

    shutdown_signal().await;
    println!("[Lightcycle] node {} shutting down", config.node_id);

    leader_watch.abort();
    server.shutdown().await;
    node.shutdown().await.context("raft node shutdown")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn local(addr: &str) -> SocketAddr {
        addr.parse().unwrap()
    }

    #[test]
    fn concrete_bound_address_is_advertised_as_is() {
        assert_eq!(advertised_addr(None, local("10.0.0.5:7443")).unwrap(), "10.0.0.5:7443");
    }

    #[test]
    fn wildcard_bind_without_advertise_is_refused() {
        assert!(advertised_addr(None, local("0.0.0.0:7443")).is_err());
        assert!(advertised_addr(None, local("[::]:7443")).is_err());
    }

    #[test]
    fn wildcard_check_runs_only_when_a_multi_node_cluster_is_initialized() {
        let wildcard = local("0.0.0.0:7443");
        // Initializing a cluster with peers publishes the address, so a wildcard is refused.
        assert!(published_addr(true, false, None, wildcard).is_err());
        assert_eq!(
            published_addr(true, false, Some("10.0.0.5:7443"), wildcard).unwrap(),
            "10.0.0.5:7443"
        );
        // A restart (nothing published) and a standalone node (no peer to dial it) are fine.
        assert_eq!(published_addr(false, false, None, wildcard).unwrap(), "0.0.0.0:7443");
        assert_eq!(published_addr(true, true, None, wildcard).unwrap(), "0.0.0.0:7443");
        // An explicit advertise on a standalone initialization is still validated.
        assert!(published_addr(true, true, Some("0.0.0.0:7443"), wildcard).is_err());
    }

    #[test]
    fn explicit_advertise_replaces_a_wildcard_bind() {
        assert_eq!(
            advertised_addr(Some("10.0.0.5:7443"), local("0.0.0.0:7443")).unwrap(),
            "10.0.0.5:7443"
        );
        assert_eq!(
            advertised_addr(Some("hostb.internal:7443"), local("0.0.0.0:7443")).unwrap(),
            "hostb.internal:7443"
        );
    }

    #[test]
    fn wildcard_advertise_is_refused() {
        assert!(advertised_addr(Some("0.0.0.0:7443"), local("10.0.0.5:7443")).is_err());
        assert!(advertised_addr(Some("[::]:7443"), local("10.0.0.5:7443")).is_err());
    }

    #[test]
    fn malformed_advertise_is_refused() {
        assert!(advertised_addr(Some("no-port"), local("10.0.0.5:7443")).is_err());
        assert!(advertised_addr(Some(":7443"), local("10.0.0.5:7443")).is_err());
        assert!(advertised_addr(Some("host:notaport"), local("10.0.0.5:7443")).is_err());
    }

    #[test]
    fn advertised_port_zero_takes_the_bound_port() {
        assert_eq!(
            advertised_addr(Some("10.0.0.5:0"), local("0.0.0.0:41234")).unwrap(),
            "10.0.0.5:41234"
        );
        assert_eq!(
            advertised_addr(Some("hostb:0"), local("0.0.0.0:41234")).unwrap(),
            "hostb:41234"
        );
    }
}
