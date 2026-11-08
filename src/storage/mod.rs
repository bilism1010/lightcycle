//! Lock-free ordered key-value storage.

pub mod index;

pub use index::{CasError, KvIndex};
