//! Segmented group-commit write-ahead log (plans 03-03, 03-07, 03-08, 03-11).
//!
//! Invariants: appends are durable only after the group-commit fsync completes; each flush
//! callback is completed exactly once; mid-log corruption is never treated as a torn tail.
