//! Raft integration layer: shared Raft-facing types, the storage / state-machine adapters, and
//! (plan 03-13) the node configuration and assembled node.
//!
//! Owning plan: 03-01 (types). openraft is pinned to `=0.10.0-alpha.38` (decision A1).

pub mod config;
pub mod node;
pub mod state_machine;
pub mod types;
