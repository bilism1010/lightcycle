//! Group-commit WAL writer thread (decisions D-01, D-02, D-03, D-04).
//!
//! One dedicated `std::thread` owns the [`SegmentIo`] backend: it is the only place WAL bytes are
//! written and the only place fsync is called, which keeps blocking I/O off the Tokio runtime and
//! serializes all write I/O as openraft requires. Callers publish [`WalOp`]s through
//! [`WalHandle::try_submit`] into the Phase 2 SPSC ring; the writer drains a batch, encodes it into
//! one reusable buffer, writes it, fsyncs once and only then completes every op's [`Ack`]. There is
//! no timer based batching window: batching is self clocking (ops that arrive while one fsync runs
//! form the next batch).
//!
//! Durability ordering under [`Fsync::Always`] (never reorder these to save a sync):
//!
//! * roll: `sync_segment(old)`, `create_segment(new)`, `sync_dir`, write the new preamble, write
//!   the batch, `sync_segment(new)`, then acks. The directory entry of a new segment is durable
//!   before the sync that covers its first entry.
//! * purge: the marker record is written and `sync_segment(active)` returns before any
//!   `delete_segment`; one `sync_dir` follows the last deletion.
//! * any write, sync, create, delete or directory sync error is fail-stop: every pending and later
//!   ack gets `Err`, an fsync is never retried, and the thread keeps draining until shutdown so no
//!   callback is orphaned.
//!
//! The writer is generic over an opaque [`WalPayload`] so it does not depend on the consensus
//! library; the store adapter wraps openraft's callback into an [`Ack`].

use std::collections::{BTreeMap, VecDeque};
use std::io;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{fence, AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::{JoinHandle, Thread};
use std::time::{Duration, Instant};

use crate::ipc::ring_buffer::{Consumer, Producer, RingBuffer};

use super::io::SegmentIo;
use super::record::{encode_record, RecordType, MAX_RECORD_LEN, RECORD_HEADER_LEN};
use super::segment::{
    encode_preamble, fits, EntryLocation, Fsync, Preamble, PreambleState, WalConfig, WalError,
    WalObserver, WalStart, FORMAT_VERSION,
};

/// Capacity of the op ring between the Raft task and the writer thread.
pub const WAL_RING_CAPACITY: usize = 8192;

/// Most ops drained from the ring per writer iteration.
const MAX_OPS_PER_BATCH: usize = 512;
/// Idle iterations the writer spins before it parks.
const IDLE_SPINS: usize = 100;
/// Upper bound on one park; a lost-wakeup backstop, never a batching window.
const PARK_BACKSTOP: Duration = Duration::from_millis(100);
/// Smallest accepted segment size (must hold a preamble and a useful record).
const MIN_SEGMENT_SIZE: u64 = 256;
/// Buckets in the fsync latency histogram (bucket `b` holds `[2^(b-1), 2^b)` microseconds).
const HIST_BUCKETS: usize = 40;

/// Body of an entry record, encoded on the writer thread.
pub trait WalPayload: Send + 'static {
    /// Appends the entry body (everything after the 8-byte index) to `out`.
    fn encode_into(&self, out: &mut Vec<u8>);
}

impl WalPayload for Vec<u8> {
    fn encode_into(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(self);
    }
}

/// Completion callback. Called exactly once: `Ok` after the covering fsync (or after the write in
/// the unsafe fsync modes), `Err` on any failure.
pub type Ack = Box<dyn FnOnce(io::Result<()>) + Send>;

/// One mutation of the log.
pub enum WalOp<P> {
    /// A log entry.
    Entry {
        /// Log index.
        index: u64,
        /// Entry body.
        payload: P,
        /// Completion callback.
        ack: Option<Ack>,
    },
    /// The persisted vote (opaque encoded blob).
    Vote {
        /// Encoded vote.
        blob: Vec<u8>,
        /// Completion callback.
        ack: Option<Ack>,
    },
    /// Truncation marker: drop every entry after `keep_through` (`None` = drop everything).
    TruncateAfter {
        /// Last index to keep.
        keep_through: Option<u64>,
        /// Completion callback.
        ack: Option<Ack>,
    },
    /// Purge marker: entries up to and including `upto` are no longer needed. Record payload is
    /// `upto` as u64 LE followed by `blob` (the encoded purged log id).
    Purge {
        /// Last purged index.
        upto: u64,
        /// Encoded purged log id.
        blob: Vec<u8>,
        /// Completion callback.
        ack: Option<Ack>,
    },
    /// The persisted committed log id (opaque encoded blob).
    Committed {
        /// Encoded committed log id.
        blob: Vec<u8>,
        /// Completion callback.
        ack: Option<Ack>,
    },
}

impl<P> WalOp<P> {
    fn take_ack(&mut self) -> Option<Ack> {
        match self {
            WalOp::Entry { ack, .. }
            | WalOp::Vote { ack, .. }
            | WalOp::TruncateAfter { ack, .. }
            | WalOp::Purge { ack, .. }
            | WalOp::Committed { ack, .. } => ack.take(),
        }
    }
}

/// Counters kept by the writer thread. All fields are monotonic.
pub struct WalStats {
    /// Group-commit batches processed.
    pub batches: AtomicU64,
    /// Records written (entries, markers and preambles excluded from neither: every framed record).
    pub records: AtomicU64,
    /// Bytes written to segments.
    pub bytes: AtomicU64,
    /// Segment data syncs performed.
    pub syncs: AtomicU64,
    /// Times the park backstop elapsed while work was waiting (a lost wake-up would show here).
    pub timeout_wakeups_with_work: AtomicU64,
    /// Fsync latency histogram in log2 microsecond buckets.
    pub fsync_hist: [AtomicU64; HIST_BUCKETS],
}

impl Default for WalStats {
    fn default() -> Self {
        WalStats {
            batches: AtomicU64::new(0),
            records: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            syncs: AtomicU64::new(0),
            timeout_wakeups_with_work: AtomicU64::new(0),
            fsync_hist: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }
}

impl WalStats {
    fn record_fsync(&self, elapsed: Duration) {
        let us = u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX);
        let bucket = ((64 - us.leading_zeros()) as usize).min(HIST_BUCKETS - 1);
        self.fsync_hist[bucket].fetch_add(1, Ordering::Relaxed);
        self.syncs.fetch_add(1, Ordering::Relaxed);
    }

    /// Upper edge, in microseconds, of the histogram bucket holding the `p` quantile of fsync
    /// latency (`p` in `0.0..=1.0`, e.g. `0.99`). Returns 0 when no sync has been timed. macOS
    /// `File::sync_data` is a full-device flush, so numbers taken there are correctness-only.
    pub fn fsync_percentile_us(&self, p: f64) -> u64 {
        let counts: Vec<u64> = self.fsync_hist.iter().map(|c| c.load(Ordering::Relaxed)).collect();
        let total: u64 = counts.iter().sum();
        if total == 0 {
            return 0;
        }
        let rank = ((total as f64) * p.clamp(0.0, 1.0)).ceil().max(1.0) as u64;
        let mut seen = 0;
        for (bucket, count) in counts.iter().enumerate() {
            seen += count;
            if seen >= rank {
                return if bucket == 0 { 0 } else { 1u64 << bucket };
            }
        }
        1u64 << (HIST_BUCKETS - 1)
    }
}

/// State shared between the handle and the writer thread.
struct Shared {
    stats: Arc<WalStats>,
    failed: AtomicBool,
    parked: AtomicBool,
    stop: AtomicBool,
}

/// Calls an ack, isolating the writer from a panicking callback.
fn call_ack(ack: Ack, result: io::Result<()>) {
    if catch_unwind(AssertUnwindSafe(move || ack(result))).is_err() {
        tracing::error!("wal ack callback panicked");
    }
}

/// Owns the acks of the batch in flight. Every ack is completed exactly once: `Ok` through
/// [`AckBatch::complete_ok`], `Err` through [`AckBatch::fail_all`] or, as the last resort against
/// an orphaned callback (Pitfall 9), in `Drop`.
struct AckBatch {
    acks: VecDeque<Ack>,
}

impl AckBatch {
    fn push(&mut self, ack: Ack) {
        self.acks.push_back(ack);
    }

    fn complete_ok(&mut self) {
        while let Some(ack) = self.acks.pop_front() {
            call_ack(ack, Ok(()));
        }
    }

    fn fail_all(&mut self, kind: io::ErrorKind, msg: &str) {
        while let Some(ack) = self.acks.pop_front() {
            call_ack(ack, Err(io::Error::new(kind, msg.to_string())));
        }
    }

    fn fail_last(&mut self, kind: io::ErrorKind, msg: &str) {
        if let Some(ack) = self.acks.pop_back() {
            call_ack(ack, Err(io::Error::new(kind, msg.to_string())));
        }
    }
}

impl Drop for AckBatch {
    fn drop(&mut self) {
        self.fail_all(io::ErrorKind::Other, "wal writer dropped an unfinished ack");
    }
}

/// Entry range of one segment, tracked for the purge rule.
#[derive(Debug, Clone, Copy, Default)]
struct SegRange {
    first: Option<u64>,
    last: Option<u64>,
}

struct Writer<P: WalPayload> {
    io: Box<dyn SegmentIo>,
    cfg: WalConfig,
    observer: Box<dyn WalObserver>,
    consumer: Consumer<WalOp<P>, WAL_RING_CAPACITY>,
    shared: Arc<Shared>,
    /// Ops drained from the ring but not yet processed.
    pending: VecDeque<WalOp<P>>,
    /// Acks of the ops processed in the current batch.
    acks: AckBatch,
    /// Segment being appended to (0 = none yet).
    active_seq: u64,
    /// Offset in the active segment where `buf` will be written.
    tail: u64,
    /// Live durable state, re-emitted in every new segment's preamble.
    state: PreambleState,
    /// Entry range per surviving segment.
    segs: BTreeMap<u64, SegRange>,
    /// Index the next entry is expected to carry.
    next_index: u64,
    /// Encoded records not yet written.
    buf: Vec<u8>,
    /// Payload of the record being built.
    scratch: Vec<u8>,
    /// Locations of the entries in `buf`.
    locs: Vec<EntryLocation>,
    /// Bytes encoded in the current batch (caps it at `max_batch_bytes`).
    batch_bytes: usize,
    /// Highest entry index written but not yet reported through `on_flushed`.
    unreported: Option<u64>,
    /// Bytes written to the active segment since its last sync.
    dirty: bool,
    last_sync: Instant,
}

impl<P: WalPayload> Writer<P> {
    fn stats(&self) -> &WalStats {
        &self.shared.stats
    }

    fn sync_enabled(&self) -> bool {
        !matches!(self.cfg.fsync, Fsync::Never)
    }

    fn run(mut self) {
        loop {
            let stop = self.shared.stop.load(Ordering::SeqCst);
            if self.pending.len() < MAX_OPS_PER_BATCH {
                let room = MAX_OPS_PER_BATCH - self.pending.len();
                let pending = &mut self.pending;
                self.consumer.consume_batch(room, |op| pending.push_back(op));
            }
            if self.pending.is_empty() {
                if stop {
                    break;
                }
                self.idle();
                continue;
            }
            self.handle_batch();
        }
        // Periodic mode: do not leave the final writes unsynced on a clean shutdown.
        if matches!(self.cfg.fsync, Fsync::Periodic { .. })
            && self.dirty
            && !self.shared.failed.load(Ordering::SeqCst)
        {
            if let Err(e) = self.sync_active() {
                tracing::error!(error = %e, "wal final periodic sync failed");
            }
        }
    }

    /// No work: spin briefly, then park with the lost-wakeup handshake.
    fn idle(&mut self) {
        for _ in 0..IDLE_SPINS {
            if !self.consumer.is_empty() {
                return;
            }
            std::hint::spin_loop();
        }
        self.shared.parked.store(true, Ordering::SeqCst);
        fence(Ordering::SeqCst);
        if !self.consumer.is_empty() || self.shared.stop.load(Ordering::SeqCst) {
            self.shared.parked.store(false, Ordering::SeqCst);
            return;
        }
        let parked_at = Instant::now();
        std::thread::park_timeout(PARK_BACKSTOP);
        self.shared.parked.store(false, Ordering::SeqCst);
        if parked_at.elapsed() >= PARK_BACKSTOP && !self.consumer.is_empty() {
            self.stats().timeout_wakeups_with_work.fetch_add(1, Ordering::Relaxed);
        }
        self.idle_periodic_sync();
    }

    fn idle_periodic_sync(&mut self) {
        if let Fsync::Periodic { interval_ms } = self.cfg.fsync {
            if self.dirty
                && !self.shared.failed.load(Ordering::SeqCst)
                && self.last_sync.elapsed() >= Duration::from_millis(interval_ms)
            {
                if let Err(e) = self.sync_active() {
                    self.fail_stop(e);
                }
            }
        }
    }

    fn handle_batch(&mut self) {
        if self.shared.failed.load(Ordering::SeqCst) {
            self.fail_pending();
            return;
        }
        match catch_unwind(AssertUnwindSafe(|| self.process_batch())) {
            Ok(Ok(())) => {}
            Ok(Err(e)) => self.fail_stop(e),
            Err(_) => self.fail_stop(io::Error::other("wal writer panicked")),
        }
    }

    /// Marks the writer failed, completes every outstanding ack with `Err` and never touches the
    /// disk again. The thread stays alive so later ops are completed with `Err` too.
    fn fail_stop(&mut self, err: io::Error) {
        tracing::error!(error = %err, "wal writer failed; refusing all further work");
        self.shared.failed.store(true, Ordering::SeqCst);
        let msg = format!("wal writer failed: {err}");
        self.acks.fail_all(err.kind(), &msg);
        self.buf.clear();
        self.locs.clear();
        self.unreported = None;
        self.fail_pending();
    }

    fn fail_pending(&mut self) {
        while let Some(mut op) = self.pending.pop_front() {
            if let Some(ack) = op.take_ack() {
                call_ack(ack, Err(io::Error::other("wal writer has failed")));
            }
        }
    }

    fn process_batch(&mut self) -> io::Result<()> {
        self.buf.clear();
        self.locs.clear();
        self.batch_bytes = 0;
        let mut ops = 0u64;
        while self.batch_bytes < self.cfg.max_batch_bytes {
            let Some(op) = self.pending.pop_front() else { break };
            self.apply_op(op)?;
            ops += 1;
        }
        self.write_run()?;
        self.finish_sync()?;
        self.acks.complete_ok();
        if ops > 0 {
            self.stats().batches.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }

    fn apply_op(&mut self, mut op: WalOp<P>) -> io::Result<()> {
        // The ack moves into the batch guard before anything that can panic runs.
        let has_ack = match op.take_ack() {
            Some(ack) => {
                self.acks.push(ack);
                true
            }
            None => false,
        };
        self.scratch.clear();
        let accepted = match op {
            WalOp::Entry { index, payload, .. } => {
                self.scratch.extend_from_slice(&index.to_le_bytes());
                payload.encode_into(&mut self.scratch);
                self.append(RecordType::Entry, Some(index))?
            }
            WalOp::Vote { blob, .. } => {
                self.scratch.extend_from_slice(&blob);
                let ok = self.append(RecordType::Vote, None)?;
                if ok {
                    self.state.vote = Some(blob);
                }
                ok
            }
            WalOp::Committed { blob, .. } => {
                self.scratch.extend_from_slice(&blob);
                let ok = self.append(RecordType::Committed, None)?;
                if ok {
                    self.state.committed = Some(blob);
                }
                ok
            }
            WalOp::TruncateAfter { keep_through, .. } => {
                match keep_through {
                    None => self.scratch.push(0),
                    Some(k) => {
                        self.scratch.push(1);
                        self.scratch.extend_from_slice(&k.to_le_bytes());
                    }
                }
                let ok = self.append(RecordType::TruncateAfter, None)?;
                if ok {
                    self.apply_truncate(keep_through);
                    // Entries before the marker reach the observer first, then the truncation.
                    self.write_run()?;
                    self.observer.on_truncate(keep_through);
                }
                ok
            }
            WalOp::Purge { upto, blob, .. } => {
                self.scratch.extend_from_slice(&upto.to_le_bytes());
                self.scratch.extend_from_slice(&blob);
                let ok = self.append(RecordType::Purge, None)?;
                if ok {
                    self.state.purged = Some((upto, blob));
                    self.run_purge(upto)?;
                }
                ok
            }
        };
        if !accepted && has_ack {
            self.acks.fail_last(
                io::ErrorKind::InvalidInput,
                "wal record does not fit in a segment",
            );
        }
        Ok(())
    }

    /// Frames `scratch` as a `ty` record into `buf`, rolling the segment first when it does not
    /// fit. Returns `false` (nothing appended, nothing changed) for a record that can never fit.
    fn append(&mut self, ty: RecordType, entry_index: Option<u64>) -> io::Result<bool> {
        let seg_size = self.cfg.segment_size;
        let rec_len = RECORD_HEADER_LEN + self.scratch.len();
        if 1 + self.scratch.len() > MAX_RECORD_LEN as usize {
            return Ok(false);
        }
        let first_hint = entry_index.unwrap_or(self.next_index);
        let offset = self.tail + self.buf.len() as u64;
        if self.active_seq == 0 || !fits(offset, rec_len, seg_size) {
            let pre_len = self.preamble_record(self.active_seq + 1, first_hint).len();
            if !fits(pre_len as u64, rec_len, seg_size) {
                return Ok(false);
            }
            self.roll(first_hint)?;
        }
        let offset = self.tail + self.buf.len() as u64;
        let n = encode_record(&mut self.buf, ty, &self.scratch);
        self.batch_bytes += n;
        self.stats().records.fetch_add(1, Ordering::Relaxed);
        if let Some(index) = entry_index {
            self.locs.push(EntryLocation {
                index,
                seq: self.active_seq,
                offset,
                len: n as u32,
            });
            let range = self.segs.entry(self.active_seq).or_default();
            if range.first.is_none() {
                range.first = Some(index);
            }
            range.last = Some(index);
            self.next_index = index.wrapping_add(1);
        }
        Ok(true)
    }

    fn preamble_record(&self, seq: u64, first_index: u64) -> Vec<u8> {
        let preamble = Preamble {
            format_version: FORMAT_VERSION,
            segment_seq: seq,
            first_index,
            state: self.state.clone(),
        };
        let mut out = Vec::new();
        encode_record(&mut out, RecordType::Preamble, &encode_preamble(&preamble));
        out
    }

    /// Starts the next segment. Order matters: finish the old segment, create the new one, make
    /// its directory entry durable, and only then write the preamble and anything after it.
    fn roll(&mut self, first_index: u64) -> io::Result<()> {
        if self.active_seq != 0 {
            self.write_run()?;
            if self.sync_enabled() {
                self.sync_active()?;
            }
        }
        let seq = self.active_seq + 1;
        self.io.create_segment(seq, self.cfg.segment_size)?;
        if self.sync_enabled() {
            self.io.sync_dir()?;
        }
        let preamble = self.preamble_record(seq, first_index);
        self.io.write_at(seq, 0, &preamble)?;
        self.stats().bytes.fetch_add(preamble.len() as u64, Ordering::Relaxed);
        self.stats().records.fetch_add(1, Ordering::Relaxed);
        self.segs.insert(seq, SegRange::default());
        self.active_seq = seq;
        self.tail = preamble.len() as u64;
        self.dirty = true;
        Ok(())
    }

    /// Writes the encoded run and tells the observer where the entries landed.
    fn write_run(&mut self) -> io::Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        self.io.write_at(self.active_seq, self.tail, &self.buf)?;
        self.tail += self.buf.len() as u64;
        self.stats().bytes.fetch_add(self.buf.len() as u64, Ordering::Relaxed);
        self.dirty = true;
        if let Some(highest) = self.locs.iter().map(|l| l.index).max() {
            self.unreported = Some(self.unreported.map_or(highest, |u| u.max(highest)));
            self.observer.on_entries_written(&self.locs);
        }
        self.buf.clear();
        self.locs.clear();
        Ok(())
    }

    /// Syncs the active segment, records the latency and reports newly durable entries.
    fn sync_active(&mut self) -> io::Result<()> {
        let started = Instant::now();
        self.io.sync_segment(self.active_seq)?;
        self.stats().record_fsync(started.elapsed());
        self.dirty = false;
        self.last_sync = Instant::now();
        self.report_flushed();
        Ok(())
    }

    fn report_flushed(&mut self) {
        if let Some(upto) = self.unreported.take() {
            self.observer.on_flushed(upto);
        }
    }

    /// End of batch: the single group-commit fsync (per the fsync mode), then the flushed report.
    fn finish_sync(&mut self) -> io::Result<()> {
        match self.cfg.fsync {
            Fsync::Always => {
                if self.dirty {
                    self.sync_active()?;
                }
            }
            Fsync::Periodic { interval_ms } => {
                if self.dirty && self.last_sync.elapsed() >= Duration::from_millis(interval_ms) {
                    self.sync_active()?;
                }
            }
            Fsync::Never => {}
        }
        // Unsafe modes have no sync to wait for: report right after the write.
        self.report_flushed();
        Ok(())
    }

    fn apply_truncate(&mut self, keep_through: Option<u64>) {
        for range in self.segs.values_mut() {
            match keep_through {
                None => *range = SegRange::default(),
                Some(k) => {
                    if range.first.is_some_and(|f| f > k) {
                        *range = SegRange::default();
                    } else if range.last.is_some_and(|l| l > k) {
                        range.last = Some(k);
                    }
                }
            }
        }
        self.next_index = match keep_through {
            Some(k) => k.wrapping_add(1),
            None => self.state.purged.as_ref().map_or(0, |(upto, _)| upto.wrapping_add(1)),
        };
        // Never report durability beyond what the truncation kept.
        self.unreported = match (self.unreported, keep_through) {
            (Some(u), Some(k)) => Some(u.min(k)),
            _ => None,
        };
    }

    /// Purge: marker durable first, then delete the oldest wholly purged non-active segments, then
    /// sync the directory. Only a contiguous prefix of segments is ever deleted so a truncation
    /// marker can never be deleted while the entries it cancels survive.
    fn run_purge(&mut self, upto: u64) -> io::Result<()> {
        self.write_run()?;
        if self.sync_enabled() {
            self.sync_active()?;
        }
        let victims: Vec<u64> = self
            .segs
            .iter()
            .take_while(|(seq, range)| {
                **seq != self.active_seq && range.last.is_none_or(|last| last <= upto)
            })
            .map(|(seq, _)| *seq)
            .collect();
        for seq in &victims {
            self.io.delete_segment(*seq)?;
            self.segs.remove(seq);
        }
        if !victims.is_empty() && self.sync_enabled() {
            self.io.sync_dir()?;
        }
        self.observer.on_purge(upto);
        Ok(())
    }
}

impl<P: WalPayload> Drop for Writer<P> {
    /// Backstop: whatever is still queued is completed with `Err`, never dropped.
    fn drop(&mut self) {
        self.fail_pending();
        while let Some(mut op) = self.consumer.try_consume() {
            if let Some(ack) = op.take_ack() {
                call_ack(ack, Err(io::Error::other("wal writer exited")));
            }
        }
    }
}

/// Handle to the writer thread. Owns the producing half of the op ring.
pub struct WalHandle<P: WalPayload> {
    producer: Producer<WalOp<P>, WAL_RING_CAPACITY>,
    shared: Arc<Shared>,
    wake: Thread,
    thread: Option<JoinHandle<()>>,
}

impl<P: WalPayload> WalHandle<P> {
    /// Starts the writer thread `lightcycle-wal-writer` over `io`, resuming at `start`. A fresh
    /// `start` (`active_seq == 0`) creates segment 1 lazily when the first op arrives.
    pub fn spawn(
        io: Box<dyn SegmentIo>,
        cfg: WalConfig,
        start: WalStart,
        observer: Box<dyn WalObserver>,
    ) -> Result<WalHandle<P>, WalError> {
        if cfg.segment_size < MIN_SEGMENT_SIZE {
            return Err(WalError::Config(format!(
                "segment_size {} is below the minimum {MIN_SEGMENT_SIZE}",
                cfg.segment_size
            )));
        }
        if cfg.max_batch_bytes == 0 {
            return Err(WalError::Config("max_batch_bytes must be non-zero".into()));
        }
        if start.active_seq != 0 && start.tail_offset > cfg.segment_size {
            return Err(WalError::Config(format!(
                "tail_offset {} exceeds segment_size {}",
                start.tail_offset, cfg.segment_size
            )));
        }
        if cfg.fsync.is_unsafe() {
            tracing::warn!(
                fsync = ?cfg.fsync,
                "UNSAFE wal fsync mode: acknowledged writes can be lost on power loss"
            );
        }

        let mut segs: BTreeMap<u64, SegRange> = start
            .segments
            .iter()
            .map(|s| {
                (
                    s.seq,
                    SegRange {
                        first: s.first_index,
                        last: s.last_index,
                    },
                )
            })
            .collect();
        if start.active_seq != 0 {
            segs.entry(start.active_seq).or_default();
        }
        let next_index = segs
            .values()
            .filter_map(|r| r.last)
            .max()
            .map(|last| last.wrapping_add(1))
            .or_else(|| start.state.purged.as_ref().map(|(upto, _)| upto.wrapping_add(1)))
            .unwrap_or(0);

        let (producer, consumer) = RingBuffer::<WalOp<P>, WAL_RING_CAPACITY>::channel();
        let shared = Arc::new(Shared {
            stats: Arc::new(WalStats::default()),
            failed: AtomicBool::new(false),
            parked: AtomicBool::new(false),
            stop: AtomicBool::new(false),
        });
        let writer = Writer {
            io,
            cfg,
            observer,
            consumer,
            shared: Arc::clone(&shared),
            pending: VecDeque::new(),
            acks: AckBatch { acks: VecDeque::new() },
            active_seq: start.active_seq,
            tail: start.tail_offset,
            state: start.state,
            segs,
            next_index,
            buf: Vec::new(),
            scratch: Vec::new(),
            locs: Vec::new(),
            batch_bytes: 0,
            unreported: None,
            dirty: false,
            last_sync: Instant::now(),
        };
        let thread = std::thread::Builder::new()
            .name("lightcycle-wal-writer".into())
            .spawn(move || writer.run())
            .map_err(WalError::Io)?;
        Ok(WalHandle {
            producer,
            shared,
            wake: thread.thread().clone(),
            thread: Some(thread),
        })
    }

    /// Publishes an op for the writer. Returns the op back when the ring is full. After the
    /// writer has failed the op is still accepted and its ack completes with `Err`.
    pub fn try_submit(&mut self, op: WalOp<P>) -> Result<(), WalOp<P>> {
        self.producer.try_publish(op)?;
        fence(Ordering::SeqCst);
        if self.shared.parked.load(Ordering::SeqCst) {
            self.wake.unpark();
        }
        Ok(())
    }

    /// Shared writer counters.
    pub fn stats(&self) -> Arc<WalStats> {
        Arc::clone(&self.shared.stats)
    }

    /// True once the writer hit an I/O error or panicked and went fail-stop.
    pub fn is_failed(&self) -> bool {
        self.shared.failed.load(Ordering::SeqCst)
    }

    /// Stops the writer after it has drained the ring and completed every ack, then joins it.
    pub fn shutdown(mut self) {
        self.stop_and_join();
    }

    fn stop_and_join(&mut self) {
        if let Some(thread) = self.thread.take() {
            self.shared.stop.store(true, Ordering::SeqCst);
            fence(Ordering::SeqCst);
            self.wake.unpark();
            let _ = thread.join();
        }
    }
}

impl<P: WalPayload> Drop for WalHandle<P> {
    fn drop(&mut self) {
        self.stop_and_join();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::io::{FakeIo, StdFsIo};
    use crate::wal::record::{decode_record, Decoded};
    use crate::wal::segment::{decode_preamble, NoopObserver};
    use std::sync::mpsc::{channel, Receiver};

    struct Rec {
        ty: RecordType,
        payload: Vec<u8>,
    }

    /// Walks a segment image; the flag is true when the walk ended on unwritten space.
    fn walk(bytes: &[u8]) -> (Vec<Rec>, bool) {
        let mut off = 0;
        let mut recs = Vec::new();
        loop {
            match decode_record(&bytes[off..]) {
                Decoded::Record { ty, payload, consumed } => {
                    recs.push(Rec { ty, payload: payload.to_vec() });
                    off += consumed;
                }
                Decoded::End => return (recs, true),
                Decoded::Invalid => return (recs, false),
            }
        }
    }

    fn entry_index(rec: &Rec) -> u64 {
        u64::from_le_bytes(rec.payload[..8].try_into().unwrap())
    }

    fn ack_chan() -> (Ack, Receiver<io::Result<()>>) {
        let (tx, rx) = channel();
        let ack: Ack = Box::new(move |r| {
            let _ = tx.send(r);
        });
        (ack, rx)
    }

    fn wait(rx: &Receiver<io::Result<()>>) -> io::Result<()> {
        rx.recv_timeout(Duration::from_secs(5)).expect("ack timed out")
    }

    fn cfg(segment_size: u64) -> WalConfig {
        WalConfig {
            segment_size,
            ..WalConfig::default()
        }
    }

    fn spawn_fake(config: WalConfig) -> (WalHandle<Vec<u8>>, FakeIo) {
        let io = FakeIo::new();
        let handle = WalHandle::spawn(
            Box::new(io.clone()),
            config,
            WalStart::fresh(),
            Box::new(NoopObserver),
        )
        .unwrap();
        (handle, io)
    }

    fn submit<P: WalPayload>(h: &mut WalHandle<P>, mut op: WalOp<P>) {
        loop {
            match h.try_submit(op) {
                Ok(()) => return,
                Err(back) => {
                    op = back;
                    std::thread::yield_now();
                }
            }
        }
    }

    fn segments(io: &FakeIo) -> Vec<u64> {
        SegmentIo::list_segments(io).unwrap()
    }

    #[test]
    fn tracer_one_record_acked_after_sync() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("wal");
        let io = StdFsIo::new(&dir).unwrap();
        let mut h = WalHandle::<Vec<u8>>::spawn(
            Box::new(io),
            cfg(1 << 20),
            WalStart::fresh(),
            Box::new(NoopObserver),
        )
        .unwrap();
        let (ack, rx) = ack_chan();
        submit(&mut h, WalOp::Entry { index: 1, payload: b"hello".to_vec(), ack: Some(ack) });
        wait(&rx).unwrap();
        let stats = h.stats();
        h.shutdown();

        let bytes = std::fs::read(dir.join("0000000000000001.wal")).unwrap();
        let (recs, clean) = walk(&bytes);
        assert!(clean);
        assert_eq!(recs.len(), 2);
        assert_eq!(recs[0].ty, RecordType::Preamble);
        let pre = decode_preamble(&recs[0].payload).unwrap();
        assert_eq!((pre.segment_seq, pre.first_index), (1, 1));
        assert_eq!(recs[1].ty, RecordType::Entry);
        let mut want = 1u64.to_le_bytes().to_vec();
        want.extend_from_slice(b"hello");
        assert_eq!(recs[1].payload, want);
        assert!(stats.syncs.load(Ordering::Relaxed) >= 1);
    }

    #[test]
    fn tracer_acked_entry_survives_power_loss() {
        let (mut h, io) = spawn_fake(cfg(1 << 16));
        let (ack, rx) = ack_chan();
        submit(&mut h, WalOp::Entry { index: 1, payload: b"durable".to_vec(), ack: Some(ack) });
        wait(&rx).unwrap();
        h.shutdown();
        io.power_loss(0);

        assert_eq!(segments(&io), vec![1]);
        let (recs, _) = walk(&io.segment_bytes(1));
        assert_eq!(recs[0].ty, RecordType::Preamble);
        assert_eq!(recs[1].ty, RecordType::Entry);
        assert_eq!(entry_index(&recs[1]), 1);
        assert_eq!(&recs[1].payload[8..], b"durable");
    }
}
