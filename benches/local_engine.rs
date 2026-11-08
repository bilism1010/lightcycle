//! Microbenchmarks for `LocalEngine`.
//!
//! Besides Criterion's throughput/mean statistics, this prints an explicit
//! per-operation tail-latency report (P50/P99/P99.9) for single- and
//! multi-threaded GET/SET and checks the P99 < 150us memory-read target.

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use lightcycle::engine::LocalEngine;
use std::sync::{Arc, Barrier};
use std::time::Instant;

const KEYS: usize = 10_000;
const VALUE_LEN: usize = 128;
const P99_READ_TARGET_NS: u64 = 150_000;

fn key(i: usize) -> [u8; 8] {
    ((i % KEYS) as u64).to_be_bytes()
}

fn populated() -> Arc<LocalEngine> {
    let e = Arc::new(LocalEngine::new());
    let v = [0xCDu8; VALUE_LEN];
    for i in 0..KEYS {
        e.set(&key(i), &v);
    }
    e
}

fn percentile(sorted: &[u64], p: f64) -> u64 {
    let idx = ((sorted.len() as f64) * p).ceil() as usize;
    sorted[idx.saturating_sub(1).min(sorted.len() - 1)]
}

fn sample(e: &LocalEngine, write: bool, ops: usize, seed: usize) -> Vec<u64> {
    let v = [0xEFu8; VALUE_LEN];
    let mut lat = Vec::with_capacity(ops);
    for i in 0..ops {
        let k = key(i.wrapping_mul(2654435761).wrapping_add(seed));
        let t = Instant::now();
        if write {
            e.set(&k, &v);
        } else {
            black_box(e.get(&k));
        }
        lat.push(t.elapsed().as_nanos() as u64);
    }
    lat
}

fn tail_report(label: &str, mut lat: Vec<u64>) -> u64 {
    lat.sort_unstable();
    let p99 = percentile(&lat, 0.99);
    println!(
        "[tail] {label:<28} n={:<8} p50={:>7}ns p99={:>8}ns p99.9={:>8}ns max={:>9}ns",
        lat.len(),
        percentile(&lat, 0.50),
        p99,
        percentile(&lat, 0.999),
        lat[lat.len() - 1]
    );
    p99
}

fn tail_latency_report() {
    const OPS: usize = 200_000;
    let e = populated();
    let read_p99_1t = tail_report("GET 1 thread", sample(&e, false, OPS, 1));
    tail_report("SET 1 thread", sample(&e, true, OPS, 2));

    for (label, write) in [("GET 8 threads", false), ("SET 8 threads", true)] {
        let barrier = Arc::new(Barrier::new(8));
        let hs: Vec<_> = (0..8)
            .map(|t| {
                let e = Arc::clone(&e);
                let b = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    b.wait();
                    sample(&e, write, OPS / 4, t * 977)
                })
            })
            .collect();
        let all: Vec<u64> = hs.into_iter().flat_map(|h| h.join().unwrap()).collect();
        let p99 = tail_report(label, all);
        if !write {
            let verdict = if p99 < P99_READ_TARGET_NS { "PASS" } else { "FAIL" };
            println!("[tail] multi-thread read P99 target <150us: {verdict} ({p99}ns)");
        }
    }
    let verdict = if read_p99_1t < P99_READ_TARGET_NS { "PASS" } else { "FAIL" };
    println!("[tail] single-thread read P99 target <150us: {verdict} ({read_p99_1t}ns)");
}

fn bench_local_engine(c: &mut Criterion) {
    tail_latency_report();

    let e = populated();
    let v = [0xEFu8; VALUE_LEN];

    let mut g = c.benchmark_group("local_engine/single_thread");
    let mut i = 0usize;
    g.bench_function("get_hit", |b| {
        b.iter(|| {
            i = i.wrapping_add(1);
            black_box(e.get(&key(i)))
        })
    });
    g.bench_function("set_overwrite", |b| {
        b.iter(|| {
            i = i.wrapping_add(1);
            e.set(&key(i), black_box(&v))
        })
    });
    g.finish();

    let mut g = c.benchmark_group("local_engine/multi_thread_8");
    for (name, write) in [("get_hit", false), ("set_overwrite", true)] {
        g.bench_function(name, |b| {
            b.iter_custom(|iters| {
                let per = (iters as usize / 8).max(1);
                let barrier = Arc::new(Barrier::new(9));
                let hs: Vec<_> = (0..8)
                    .map(|t| {
                        let e = Arc::clone(&e);
                        let barrier = Arc::clone(&barrier);
                        std::thread::spawn(move || {
                            let v = [0xEFu8; VALUE_LEN];
                            barrier.wait();
                            for i in 0..per {
                                let k = key(i.wrapping_add(t * 1237));
                                if write {
                                    e.set(&k, &v);
                                } else {
                                    black_box(e.get(&k));
                                }
                            }
                        })
                    })
                    .collect();
                barrier.wait();
                let start = Instant::now();
                for h in hs {
                    h.join().unwrap();
                }
                start.elapsed()
            })
        });
    }
    g.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(20)
        .warm_up_time(std::time::Duration::from_millis(500))
        .measurement_time(std::time::Duration::from_secs(2));
    targets = bench_local_engine
}
criterion_main!(benches);
