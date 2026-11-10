//! Fuzzy snapshot file format and store (plan 03-10).
//!
//! Invariants: snapshot files are immutable once published; the log is purged only after the
//! snapshot is durable.

pub mod format;
pub mod reader;
pub mod store;
pub mod writer;

pub use format::{Meta, SnapshotError};
pub use reader::{load_snapshot, read_header, validate_file};
pub use store::{SnapshotStore, Stage};
pub use writer::build_snapshot_file;
