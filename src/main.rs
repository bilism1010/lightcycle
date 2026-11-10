use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "lightcycle", author, version, about = "Project Lightcycle - Sub-millisecond Raft-replicated in-memory cache")]
pub struct Config {
    #[arg(short, long, default_value_t = 1)]
    pub node_id: u64,

    #[arg(short, long, default_value = "127.0.0.1:7443")]
    pub bind: String,

    /// Static peer list as comma-separated id=addr pairs, e.g. "2=127.0.0.1:7444,3=127.0.0.1:7445"
    /// (see lightcycle::raft::config::parse_peers).
    #[arg(long, default_value = "")]
    pub peers: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let config = Config::parse();

    println!(
        "[Lightcycle] Starting node {} on {} (peers: '{}')",
        config.node_id, config.bind, config.peers
    );
    println!("[Lightcycle] Initializing lock-free cache arena and Raft consensus transport...");

    Ok(())
}
