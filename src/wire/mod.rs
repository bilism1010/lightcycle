//! Shared length-prefixed frame codec and handshake (plan 03-04).
//!
//! Invariant: no consensus-crate dependency, reused by Phase 5 (decision D-06). Frame length is
//! bounded before any allocation.

pub mod frame;

pub use frame::{Frame, FrameCodec, FrameError, FrameHeader, Lane};
