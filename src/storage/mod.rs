//! Lock-free ordered key-value storage.

pub mod index;

pub use index::{AtStatus, CasError, KvIndex, VersionedEntry};
