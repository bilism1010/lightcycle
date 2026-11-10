//! Shared length-prefixed frame codec and handshake (plan 03-04).
//!
//! Invariant: no consensus-crate dependency, reused by Phase 5 (decision D-06). Frame length is
//! bounded before any allocation.

pub mod frame;
pub mod hello;

pub use frame::{Frame, FrameCodec, FrameError, FrameHeader, Lane};
pub use hello::{
    client_handshake, client_handshake_expecting, server_handshake, HandshakeError, Hello,
};
