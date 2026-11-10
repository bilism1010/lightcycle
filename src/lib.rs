//! Lightcycle core library: lock-free local execution tier.

pub mod arena;
pub mod engine;
pub mod ipc;
pub mod net;
pub mod raft;
pub mod snapshot;
pub mod storage;
pub mod wal;
pub mod wire;

/// Probe proving the `test-util` feature is visible to lib tests and integration tests.
#[cfg(feature = "test-util")]
#[doc(hidden)]
pub mod test_support {
    pub const ENABLED: bool = true;
}

#[cfg(test)]
mod feature_gate_tests {
    #[test]
    fn test_util_feature_enabled_under_cargo_test() {
        assert!(cfg!(feature = "test-util"));
    }
}
