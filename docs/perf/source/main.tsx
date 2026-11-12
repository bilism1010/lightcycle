import React, { useEffect, useRef, useState } from 'react';
import { createRoot } from 'react-dom/client';
import { ThemeProvider, WidgetContainer, useTheme } from 'lumagrid';
import type { ThemeDefinition, TransformedData } from 'lumagrid';
import { ColumnChart } from 'lumagrid/viz';
import { darkTheme } from 'lumagrid/theme';


// ---- measured data (WAL bench: monster, /datanapps = nvme2n1, commit f6ea78b; GET guard: macOS dev host) ----
const S64 = '64 in flight';
const S1 = '1 in flight';

const commitLatency = [
  { pct: 'p50', series: S64, us: 596 }, { pct: 'p50', series: S1, us: 210 },
  { pct: 'p99', series: S64, us: 7899 }, { pct: 'p99', series: S1, us: 546 },
];
const fsync = [
  { pct: 'p50', series: S64, us: 512 }, { pct: 'p50', series: S1, us: 256 },
  { pct: 'p99', series: S64, us: 1024 }, { pct: 'p99', series: S1, us: 1024 },
  { pct: 'max', series: S64, us: 8192 }, { pct: 'max', series: S1, us: 1024 },
];
const batching = [
  { scenario: S64, recordsPerFsync: +(5001 / 101).toFixed(1) },
  { scenario: S1, recordsPerFsync: +(301 / 300).toFixed(2) },
];
const getGuard = [
  { threads: '1 thread', series: 'measured p99', ns: 250 }, { threads: '1 thread', series: 'limit', ns: 334 },
  { threads: '8 threads', series: 'measured p99', ns: 542 }, { threads: '8 threads', series: 'limit', ns: 667 },
];

const table = (rows: Record<string, unknown>[], fields: [string, 'string' | 'number'][]): TransformedData => ({
  rows,
  fields: fields.map(([key, type]) => ({ key, type })),
});

const neon: ThemeDefinition = {
  ...darkTheme,
  name: 'neon',
  colors: {
    ...darkTheme.colors,
    categorical: ['#22e5ff', '#ff3fa4', '#b6ff3b', '#ffb02e', '#9d7bff', '#2dffb3'],
  },
  surface: {
    ...darkTheme.surface,
    background: '#0b1220',
    surface: '#0f1a2e',
    border: '#1e3a5f',
    borderRadius: '14px',
    shadowSm: '0 0 0 1px rgba(34,229,255,.10)',
    shadowMd: '0 10px 40px rgba(34,229,255,.08)',
  },
  text: { ...darkTheme.text, primary: '#e8f4ff', secondary: '#7f95b5' },
};

const font = "Inter, ui-sans-serif, system-ui, -apple-system, 'Segoe UI', sans-serif";

function useWidth(): number {
  const [w, setW] = useState(() => window.innerWidth);
  useEffect(() => {
    const f = () => setW(window.innerWidth);
    window.addEventListener('resize', f);
    return () => window.removeEventListener('resize', f);
  }, []);
  return w;
}

function Tile({ label, value, unit, note, color }: { label: string; value: string; unit: string; note: string; color: string }) {
  return (
    <div style={{ minWidth: 0, background: 'linear-gradient(160deg,#0f1a2e,#0b1220)', border: '1px solid #1e3a5f', borderRadius: 14, padding: '18px 20px', boxShadow: `0 0 28px ${color}1f` }}>
      <div style={{ color: '#7f95b5', fontSize: 12, letterSpacing: '.12em', textTransform: 'uppercase' }}>{label}</div>
      <div style={{ marginTop: 8, color, fontSize: 40, fontWeight: 700, lineHeight: 1, textShadow: `0 0 18px ${color}66` }}>
        {value}<span style={{ fontSize: 16, fontWeight: 500, marginLeft: 6, color: '#a9bbd6' }}>{unit}</span>
      </div>
      <div style={{ marginTop: 10, color: '#7f95b5', fontSize: 12.5, lineHeight: 1.4 }}>{note}</div>
    </div>
  );
}

function Card({ title, sub, children }: { title: string; sub: string; children: React.ReactNode }) {
  return (
    <section style={{ minWidth: 0, overflow: 'hidden', background: '#0b1220', border: '1px solid #1e3a5f', borderRadius: 16, padding: '18px 18px 14px', boxShadow: '0 10px 40px rgba(34,229,255,.06)' }}>
      <h2 style={{ margin: 0, fontSize: 17, color: '#e8f4ff', fontWeight: 650 }}>{title}</h2>
      <p style={{ margin: '4px 0 10px', fontSize: 12.5, color: '#7f95b5' }}>{sub}</p>
      {children}
    </section>
  );
}

/**
 * The chart's width comes from a wrapper this page controls, and the chart is absolutely positioned inside it,
 * so nothing the chart renders can change the wrapper's size. (Letting the container measure itself with
 * width: '100%' inside a grid creates a feedback loop: a box slightly wider than its cell widens the cell.)
 */
function Column({ data, label, value, color, h = 300, title }: { data: TransformedData; label: string; value: string; color?: string; h?: number; title: string }) {
  const theme = useTheme();
  const ref = useRef<HTMLDivElement>(null);
  const [w, setW] = useState(0);
  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    const ro = new ResizeObserver(() => setW(Math.floor(el.getBoundingClientRect().width)));
    ro.observe(el);
    setW(Math.floor(el.getBoundingClientRect().width));
    return () => ro.disconnect();
  }, []);
  return (
    <div ref={ref} style={{ position: 'relative', width: '100%', minWidth: 0, height: h, overflow: 'hidden' }}>
      {w > 0 && (
        <div style={{ position: 'absolute', inset: 0 }}>
          <WidgetContainer data={data} width={w} height={h}>
            {(d, cw, ch) => <ColumnChart theme={theme} data={d} width={cw} height={ch} labelField={label} valueField={value} colorField={color} showValues ariaLabel={title} />}
          </WidgetContainer>
        </div>
      )}
    </div>
  );
}

function App() {
  const w = useWidth();
  const cols = w > 1000 ? 2 : 1;
  return (
    <ThemeProvider theme={neon}>
      <div style={{ minHeight: '100vh', padding: '40px 24px 56px', fontFamily: font, background: 'radial-gradient(1200px 600px at 15% -10%, #12305a55, transparent), radial-gradient(900px 500px at 100% 0%, #5a124455, transparent), #070b14' }}>
        <div style={{ maxWidth: 1180, margin: '0 auto' }}>
          <div style={{ color: '#22e5ff', fontSize: 12, letterSpacing: '.28em', textTransform: 'uppercase' }}>Project Lightcycle · Phase 3</div>
          <h1 style={{ margin: '8px 0 6px', fontSize: 38, color: '#e8f4ff', fontWeight: 750, letterSpacing: '-.01em' }}>WAL performance on NVMe</h1>
          <p style={{ margin: 0, maxWidth: 760, color: '#a9bbd6', fontSize: 15, lineHeight: 1.55 }}>
            <code>cargo bench --bench wal</code> on <b>monster</b> (Linux, 32 cores), writing to <b>nvme2n1</b> mounted at <code>/datanapps</code>,
            commit <code>f6ea78b</code>. <b>Informational only:</b> this measures the WAL in isolation, not a 3-node cluster, with small sample counts on a shared volume.
            It does not validate NFR-1 or NFR-2.
          </p>

          <div style={{ display: 'grid', gridTemplateColumns: `repeat(${w > 760 ? 4 : 2}, minmax(0, 1fr))`, gap: 14, margin: '26px 0' }}>
            <Tile label="fsync p50" value="512" unit="µs" note="64 in flight, 101 fsyncs. Log is bucketed to powers of two." color="#22e5ff" />
            <Tile label="Records per fsync" value="49.5" unit="×" note="Group commit: 5,001 records acknowledged by 101 fsyncs." color="#b6ff3b" />
            <Tile label="Commit p99 (serial)" value="546" unit="µs" note="One record at a time, fsync every record (n = 297)." color="#ff3fa4" />
            <Tile label="GET p99 · 8 threads" value="542" unit="ns" note="Limit 667 ns. Regression guard passed (macOS dev host)." color="#ffb02e" />
          </div>

          <div style={{ display: 'grid', gridTemplateColumns: `repeat(${cols}, minmax(0, 1fr))`, gap: 18 }}>
            <Card title="Commit latency" sub="Time from append to acknowledgement, µs. Fsync on every batch (StdFsIo).">
              <Column title="Commit latency percentiles" data={table(commitLatency, [['pct', 'string'], ['series', 'string'], ['us', 'number']])} label="pct" value="us" color="series" />
              <p style={{ margin: '8px 0 0', fontSize: 12, color: '#7f95b5' }}>64 in flight (group commit) also had p99.9 = 36,047 µs (n = 4,950). The serial run has too few samples (297) for a p99.9.</p>
            </Card>
            <Card title="fsync latency" sub="Per-fsync time in µs, bucketed to powers of two by the bench's histogram.">
              <Column title="fsync latency percentiles" data={table(fsync, [['pct', 'string'], ['series', 'string'], ['us', 'number']])} label="pct" value="us" color="series" />
            </Card>
            <Card title="Group-commit batching" sub="Records acknowledged per fsync. More in flight, fewer syncs per record.">
              <Column title="Records per fsync" data={table(batching, [['scenario', 'string'], ['recordsPerFsync', 'number']])} label="scenario" value="recordsPerFsync" />
              <p style={{ margin: '8px 0 0', fontSize: 12, color: '#7f95b5' }}>64 in flight: 5,001 records / 101 fsyncs. 1 in flight (serial): 301 records / 300 fsyncs.</p>
            </Card>
            <Card title="GET latency regression guard" sub="Measured p99 against the guard limit, ns. Phase 2 read path after the Phase 3 changes.">
              <Column title="GET p99 against limit" data={table(getGuard, [['threads', 'string'], ['series', 'string'], ['ns', 'number']])} label="threads" value="ns" color="series" />
              <p style={{ margin: '8px 0 0', fontSize: 12, color: '#7f95b5' }}>Run by the phase verifier on the macOS dev host, not monster.</p>
            </Card>
          </div>

          <footer style={{ marginTop: 28, color: '#7f95b5', fontSize: 12.5, lineHeight: 1.6 }}>
            Left out on purpose: the <code>fakeio zero delay, window 256</code> scenario (p50 30.8 ms) is an in-memory queueing test, not disk latency, and the
            criterion <code>fakeio_round_trip</code> (72.5 µs mean) is likewise not a disk measurement.
            Charts drawn with <a style={{ color: '#22e5ff' }} href="https://github.com/bsommers/lumagrid">Lumagrid</a>.
          </footer>
        </div>
      </div>
    </ThemeProvider>
  );
}

createRoot(document.getElementById('root')!).render(<App />);
