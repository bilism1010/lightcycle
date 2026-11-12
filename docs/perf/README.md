# WAL performance charts

A static page charting the Phase 3 WAL benchmark. Open `index.html` through any web server
(for example `python3 -m http.server --directory docs/perf 8088`); it needs no network access.

**Informational only.** It does not validate NFR-1 or NFR-2: it measures the WAL in isolation, not a
3-node cluster, with small sample counts on a shared volume.

## Where the numbers come from

| Charts | Source |
|---|---|
| Commit latency, fsync latency, group-commit batching | `LC_WAL_BENCH_DIR=/datanapps/lc-wal-bench LC_WAL_BENCH_TAIL=1 cargo bench --bench wal`, run 2026-11-11 on a Linux host (32 cores) with the directory on an NVMe drive (nvme2n1, ext4), commit `f6ea78b`. The `[tail]` and `[fsync]` lines are the inputs. |
| GET p99 guard | `bash scripts/check-get-regression.sh` on the macOS dev host: 250 ns at 1 thread (limit 334), 542 ns at 8 threads (limit 667). |

Left out on purpose: the `fakeio` scenarios (an in-memory queueing test, not disk latency). The fsync
percentiles are bucketed to powers of two by the bench's histogram.

The values are typed into `source/main.tsx`; the page does not read bench output. To update it, edit the
arrays at the top of that file.

## Rebuilding

The charts use [Lumagrid](https://github.com/bsommers/lumagrid) (Apache-2.0), which is not on npm yet, so
the page is built against a checkout of its source:

```bash
git clone https://github.com/bsommers/lumagrid.git && cd lumagrid && npm ci
mkdir -p perf-report/src
cp <lightcycle>/docs/perf/source/index.html <lightcycle>/docs/perf/source/vite.config.ts perf-report/
cp <lightcycle>/docs/perf/source/main.tsx perf-report/src/main.tsx
npx vite build --config perf-report/vite.config.ts      # writes ../perf-site (next to the lumagrid checkout)
cp -R ../perf-site/index.html ../perf-site/assets <lightcycle>/docs/perf/   # remove the old assets/ first
```
