//! Informational microbenchmarks for the group-commit WAL writer.
//!
//! INFORMATIONAL ONLY. Nothing here asserts on a latency value, and nothing here validates NFR-1
//! or NFR-2. On Apple targets `File::sync_data` issues `F_FULLFSYNC` (measured about 4 ms p50 on
//! the development machine), so only a Linux NVMe host can validate NFR-1; the run on the user's
//! Linux host is the deferred manual verification recorded in 03-VALIDATION.md.
//!
//! Besides Criterion's statistics this prints an explicit append-to-ack tail report (`[tail]`) for
//! three scenarios and the writer's fsync histogram summary (`[fsync]`), so the group-commit
//! amortization (many records per sync) is visible:
//!
//! 1. FakeIo with zero delay: pure CPU and ring cost.
//! 2. StdFsIo, `Fsync::Always`, 64 acks in flight.
//! 3. StdFsIo, `Fsync::Always`, a single op in flight.
//!
//! Set `LC_WAL_BENCH_OPS` to override the op count of all three scenarios for a short local run.
//!
//! The report is expensive (the real-fsync scenarios take tens of seconds where an fsync is
//! about 4 ms), so it runs only for a plain `cargo bench --bench wal`. It is skipped under
//! `cargo test --benches` / `--all-targets` (test mode), with `--list`, and when a Criterion
//! filter is given (`cargo bench --bench wal -- fakeio`). `LC_WAL_BENCH_TAIL=1` forces it and
//! `LC_WAL_BENCH_TAIL=0` suppresses it. A flag value such as `--save-baseline name` looks like a
//! filter to this check and also skips the report; set `LC_WAL_BENCH_TAIL=1` in that case.
//!
//! The StdFsIo scenarios create their WAL directory under `LC_WAL_BENCH_DIR` when set, otherwise
//! under the system temp directory. On many Linux hosts `/tmp` is tmpfs, where `fsync` is nearly
//! free, so for a meaningful `[tail]`/`[fsync]` figure point `LC_WAL_BENCH_DIR` at the device
//! under test. The resolved directory is printed in the `[info]` line.

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use lightcycle::wal::io::{FakeIo, SegmentIo, StdFsIo};
use lightcycle::wal::writer::{Ack, WalHandle, WalOp, WalStats};
use lightcycle::wal::{Fsync, NoopObserver, WalConfig, WalStart};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const PAYLOAD_LEN: usize = 128;

fn percentile(sorted: &[u64], p: f64) -> u64 {
    let idx = ((sorted.len() as f64) * p).ceil() as usize;
    sorted[idx.saturating_sub(1).min(sorted.len() - 1)]
}

fn tail_report(label: &str, mut lat: Vec<u64>) -> u64 {
    lat.sort_unstable();
    let p99 = percentile(&lat, 0.99);
    println!(
        "[tail] {label:<34} n={:<8} p50={:>9}ns p99={:>10}ns p99.9={:>10}ns max={:>11}ns",
        lat.len(),
        percentile(&lat, 0.50),
        p99,
        percentile(&lat, 0.999),
        lat[lat.len() - 1]
    );
    p99
}

fn ops_override(default: usize) -> usize {
    std::env::var("LC_WAL_BENCH_OPS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(default)
}

/// Directory the StdFsIo scenarios create their WAL under: `LC_WAL_BENCH_DIR`, else the system
/// temp directory.
fn bench_dir() -> std::path::PathBuf {
    std::env::var_os("LC_WAL_BENCH_DIR")
        .filter(|v| !v.is_empty())
        .map(std::path::PathBuf::from)
        .unwrap_or_else(std::env::temp_dir)
}

fn spawn(io: Box<dyn SegmentIo>, segment_size: u64) -> WalHandle<Vec<u8>> {
    let cfg = WalConfig {
        segment_size,
        fsync: Fsync::Always,
        ..WalConfig::default()
    };
    WalHandle::spawn(io, cfg, WalStart::fresh(), Box::new(NoopObserver)).expect("spawn wal writer")
}

/// Submits `ops` entries keeping at most `window` acks in flight and returns the append-to-ack
/// latency of every op in nanoseconds.
fn drive(h: &mut WalHandle<Vec<u8>>, ops: usize, window: usize) -> Vec<u64> {
    let slots: Arc<Vec<AtomicU64>> = Arc::new((0..ops).map(|_| AtomicU64::new(0)).collect());
    let inflight = Arc::new(AtomicUsize::new(0));
    for i in 0..ops {
        while inflight.load(Ordering::Acquire) >= window {
            std::thread::yield_now();
        }
        inflight.fetch_add(1, Ordering::AcqRel);
        let started = Instant::now();
        let (slots, inflight) = (Arc::clone(&slots), Arc::clone(&inflight));
        let ack: Ack = Box::new(move |result| {
            let ns = started.elapsed().as_nanos() as u64;
            slots[i].store(if result.is_ok() { ns.max(1) } else { u64::MAX }, Ordering::Release);
            inflight.fetch_sub(1, Ordering::AcqRel);
        });
        let mut op = WalOp::Entry {
            index: i as u64 + 1,
            payload: vec![0xA5u8; PAYLOAD_LEN],
            ack: Some(ack),
        };
        loop {
            match h.try_submit(op) {
                Ok(()) => break,
                Err(back) => {
                    op = back;
                    std::thread::yield_now();
                }
            }
        }
    }
    while inflight.load(Ordering::Acquire) > 0 {
        std::thread::yield_now();
    }
    let lat: Vec<u64> = slots.iter().map(|s| s.load(Ordering::Acquire)).collect();
    // A failed ack is recorded as u64::MAX; it is not a latency, and a failing (fast-erroring)
    // write path must never produce numbers that look like a fast WAL.
    let failed = lat.iter().filter(|&&v| v == u64::MAX).count();
    assert_eq!(failed, 0, "{failed} of {ops} appends failed; latencies are not meaningful");
    lat
}

fn fsync_report(label: &str, stats: &WalStats) {
    let batches = stats.batches.load(Ordering::Relaxed);
    let records = stats.records.load(Ordering::Relaxed);
    let syncs = stats.syncs.load(Ordering::Relaxed);
    println!(
        "[fsync] {label:<34} p50={}us p99={}us max={}us batches={batches} records={records} syncs={syncs}",
        stats.fsync_percentile_us(0.50),
        stats.fsync_percentile_us(0.99),
        stats.fsync_percentile_us(1.0),
    );
}

fn std_scenario(label: &str, ops: usize, window: usize) {
    let tmp = tempfile::tempdir_in(bench_dir()).expect("create a bench dir under LC_WAL_BENCH_DIR");
    let io = StdFsIo::new(tmp.path().join("wal")).expect("open wal dir");
    let mut h = spawn(Box::new(io), 64 * 1024 * 1024);
    let lat = drive(&mut h, ops, window);
    tail_report(label, lat);
    fsync_report(label, &h.stats());
    h.shutdown();
}

fn append_ack_report() {
    println!("[info] INFORMATIONAL only: NFR-1 and NFR-2 are not validated by this bench");
    println!(
        "[info] stdfs scenarios write under {} (set LC_WAL_BENCH_DIR to change; tmpfs makes fsync free)",
        bench_dir().display()
    );

    let ops = ops_override(200_000);
    let mut h = spawn(Box::new(FakeIo::new()), 64 * 1024 * 1024);
    let lat = drive(&mut h, ops, 256);
    tail_report("fakeio zero delay, window 256", lat);
    h.shutdown();

    std_scenario("stdfs always, 64 in flight", ops_override(5_000), 64);
    std_scenario("stdfs always, 1 in flight", ops_override(300), 1);
}

/// Whether to print the tail report before the Criterion groups (see the header).
fn report_wanted() -> bool {
    match std::env::var("LC_WAL_BENCH_TAIL").as_deref() {
        Ok("1") => return true,
        Ok("0") => return false,
        _ => {}
    }
    let args: Vec<String> = std::env::args().skip(1).collect();
    let has = |flag: &str| args.iter().any(|a| a == flag);
    let filtered = args.iter().any(|a| !a.starts_with('-'));
    has("--bench") && !has("--test") && !has("--list") && !filtered
}

fn bench_wal(c: &mut Criterion) {
    if report_wanted() {
        append_ack_report();
    }

    let mut g = c.benchmark_group("wal/append_ack");
    let mut h = spawn(Box::new(FakeIo::new()), 4 * 1024 * 1024);
    let mut next = 1u64;
    g.bench_function("fakeio_round_trip", |b| {
        b.iter(|| {
            let (tx, rx) = std::sync::mpsc::channel();
            let ack: Ack = Box::new(move |r| {
                let _ = tx.send(r);
            });
            let mut op = WalOp::Entry {
                index: next,
                payload: vec![0xA5u8; PAYLOAD_LEN],
                ack: Some(ack),
            };
            next += 1;
            loop {
                match h.try_submit(op) {
                    Ok(()) => break,
                    Err(back) => {
                        op = back;
                        std::thread::yield_now();
                    }
                }
            }
            let result = rx.recv().expect("ack");
            assert!(result.is_ok(), "append failed; the round-trip figure is not meaningful");
            black_box(result)
        })
    });
    g.finish();
    h.shutdown();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(20)
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(2));
    targets = bench_wal
}
criterion_main!(benches);
