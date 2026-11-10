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

#[cfg(test)]
mod concurrent_scan {
    //! Fuzzy snapshot under a concurrent mixed-write stream (R-1, NFR-4, pitfall 1).
    //!
    //! A writer thread applies stamped set/delete/cas ops with strictly increasing indices while
    //! the main thread snapshots the engine. Restoring the file and replaying the logged ops after
    //! the snapshot start index must reproduce the live state. A sampler records the slab's
    //! maximum live block count during the scan, which a single scan-long epoch pin would blow
    //! past because every value the writer retires would stay allocated until the scan ended.

    use super::*;
    use crate::engine::LocalEngine;
    use crate::snapshot::format::Meta;
    use openraft::LogId;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    const VALUE_LEN: usize = 64;
    /// Allowed excess of live blocks over the live key count while a scan runs.
    const RETIRED_SLACK: usize = 50_000;

    fn key(i: u64) -> Vec<u8> {
        format!("key-{i:08}").into_bytes()
    }

    fn value(idx: u64) -> [u8; VALUE_LEN] {
        let mut v = [0u8; VALUE_LEN];
        for (n, chunk) in v.chunks_mut(8).enumerate() {
            chunk.copy_from_slice(&idx.wrapping_mul(0x9E37_79B9).wrapping_add(n as u64).to_be_bytes());
        }
        v
    }

    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
    }

    enum Op {
        Set(u64),
        Del(u64),
        Cas { key: u64, expected: Option<Vec<u8>> },
    }

    fn meta_at(index: u64) -> Meta {
        openraft::SnapshotMeta {
            last_log_id: Some(LogId::new_term_index(1, index)),
            last_membership: Default::default(),
        }
    }

    #[test]
    fn concurrent_scan_restore_replay_equals_live() {
        let n: u64 = if cfg!(debug_assertions) { 20_000 } else { 1_000_000 };
        let key_space = n + n / 10; // some writer inserts create new keys
        let live = Arc::new(LocalEngine::new());
        for i in 0..n {
            live.set_at(i + 1, &key(i), &value(i + 1));
        }
        let start_live = live.index().len();

        let applied = Arc::new(AtomicU64::new(n));
        let stop = Arc::new(AtomicBool::new(false));
        let writer = {
            let (live, applied, stop) = (Arc::clone(&live), Arc::clone(&applied), Arc::clone(&stop));
            std::thread::spawn(move || {
                let mut rng = Rng(0x2545_F491_4F6C_DD1D);
                let mut log: Vec<(u64, Op)> = Vec::new();
                let mut idx = n;
                while !stop.load(Ordering::Relaxed) {
                    idx += 1;
                    let k = rng.next() % key_space;
                    let kind = rng.next() % 100;
                    let op = if kind < 70 {
                        live.set_at(idx, &key(k), &value(idx));
                        Op::Set(k)
                    } else if kind < 85 {
                        live.delete_at(idx, &key(k));
                        Op::Del(k)
                    } else {
                        // Mostly a matching expectation, sometimes deliberately wrong.
                        let expected = if rng.next() % 8 == 0 {
                            Some(b"wrong".to_vec())
                        } else {
                            live.get(&key(k)).map(|b| b.to_vec())
                        };
                        live.cas_at(idx, &key(k), expected.as_deref(), Some(&value(idx)));
                        Op::Cas { key: k, expected }
                    };
                    log.push((idx, op));
                    applied.store(idx, Ordering::Release);
                }
                log
            })
        };

        // Let the writer get going, then sample the slab while the snapshot runs.
        while applied.load(Ordering::Acquire) < n + 1_000 {
            std::thread::yield_now();
        }
        let sampler_stop = Arc::new(AtomicBool::new(false));
        let sampler = {
            let (live, sampler_stop) = (Arc::clone(&live), Arc::clone(&sampler_stop));
            std::thread::spawn(move || {
                let mut max = 0usize;
                while !sampler_stop.load(Ordering::Relaxed) {
                    max = max.max(live.slab().live_blocks());
                    std::thread::sleep(Duration::from_micros(50));
                }
                max
            })
        };

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fuzzy.snap");
        let a = applied.load(Ordering::Acquire);
        let t0 = Instant::now();
        build_snapshot_file(&live, &meta_at(a), &path).unwrap();
        let scan_time = t0.elapsed();
        let applied_after_scan = applied.load(Ordering::Acquire);
        sampler_stop.store(true, Ordering::Relaxed);
        let max_live_blocks = sampler.join().unwrap();

        stop.store(true, Ordering::Relaxed);
        let log = writer.join().unwrap();
        let ops_during_scan = applied_after_scan - a;
        let end_live = live.index().len();

        // Restore plus replay of everything after the snapshot start index equals the live state.
        let (m, restored) = load_snapshot(&path).unwrap();
        assert_eq!(m, meta_at(a));
        let mut replayed = 0u64;
        for (idx, op) in log.iter().filter(|(idx, _)| *idx > a) {
            match op {
                Op::Set(k) => {
                    restored.set_at(*idx, &key(*k), &value(*idx));
                }
                Op::Del(k) => {
                    restored.delete_at(*idx, &key(*k));
                }
                Op::Cas { key: k, expected } => {
                    restored.cas_at(*idx, &key(*k), expected.as_deref(), Some(&value(*idx)));
                }
            }
            replayed += 1;
        }
        let mismatches: Vec<u64> =
            (0..key_space).filter(|&i| live.get(&key(i)) != restored.get(&key(i))).collect();
        assert!(
            mismatches.is_empty(),
            "{} keys differ, first {:?}",
            mismatches.len(),
            &mismatches[..mismatches.len().min(5)]
        );
        assert_eq!(restored.index().len(), end_live);

        // Bounded retired memory: a single pin across the scan would let every value the writer
        // retired stay allocated, i.e. roughly 0.8 * ops_during_scan extra blocks. The allowed
        // excess is the plan's 50k, tightened to a quarter of the ops actually observed so the
        // check keeps its teeth when the writer is slower than expected; the measured excess
        // with per-key pins is a few dozen blocks.
        let base = start_live.max(end_live);
        let slack = RETIRED_SLACK.min(ops_during_scan as usize / 4);
        eprintln!(
            "concurrent_scan: keys={n} scan={scan_time:?} ops_during_scan={ops_during_scan} \
             replayed={replayed} max_live_blocks={max_live_blocks} \
             live_keys(start/end)={start_live}/{end_live} bound={}",
            base + slack
        );
        assert!(
            max_live_blocks <= base + slack,
            "live blocks peaked at {max_live_blocks}, bound {}",
            base + slack
        );
        if !cfg!(debug_assertions) {
            // The check is only meaningful if the writer retired far more than the noise floor.
            assert!(
                ops_during_scan >= 20_000,
                "writer only did {ops_during_scan} ops during the scan; raise the key count"
            );
        }

        // After the scan the retired values drain (polled, never asserted immediately).
        let deadline = Instant::now() + Duration::from_secs(30);
        while live.slab().live_blocks() > end_live + 1_000 && Instant::now() < deadline {
            crossbeam_epoch::pin().flush();
            std::thread::yield_now();
        }
        assert!(
            live.slab().live_blocks() <= end_live + 1_000,
            "live blocks {} did not drain to about {end_live}",
            live.slab().live_blocks()
        );
    }
}
