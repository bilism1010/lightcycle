//! Raft peer transport and the in-process fake network (plans 03-06, 03-09).
//!
//! Invariants: replication, heartbeat and snapshot traffic use separate connections per peer
//! (interpretation A4); the fake network is gated behind `any(test, feature = "test-util")`.

#[cfg(any(test, feature = "test-util"))]
pub mod fake;
pub mod handler;
