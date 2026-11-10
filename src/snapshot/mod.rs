//! Fuzzy snapshot file format and store (plan 03-10).
//!
//! Invariants: snapshot files are immutable once published; the log is purged only after the
//! snapshot is durable.
