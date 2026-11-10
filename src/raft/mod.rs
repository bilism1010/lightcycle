//! Raft integration layer: shared Raft-facing types now, the storage / state-machine / network
//! adapters and the node wiring in later Phase 3 plans.
//!
//! Owning plan: 03-01 (types). openraft is pinned to `=0.10.0-alpha.38` (decision A1).

pub mod state_machine;
pub mod types;
