//! Shared length-prefixed frame codec and handshake (plan 03-04).
//!
//! Invariant: this module must stay free of imports from the consensus crate so Phase 5 can
//! reuse it (decision D-06). Frame length is bounded before any allocation.
