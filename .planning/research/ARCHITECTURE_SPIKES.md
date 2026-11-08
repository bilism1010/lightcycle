# Deep Research Spikes: Project Lightcycle

## 1. Memory Reclamation Evaluation
- **Option A: Epoch-Based Reclamation (EBR)** (e.g., `crossbeam-epoch`)
  - *Pros*: Extremely low read overhead (just registering epoch entry), battle-tested in Rust ecosystem.
  - *Cons*: Stalled threads holding old epochs can delay reclamation and cause temporary memory bloat.
- **Option B: Hazard Pointers (HP)**
  - *Pros*: Strict upper bound on unreclaimed memory, robust against thread stalling.
  - *Cons*: Per-read hazard pointer slot acquisition requires full memory fences, which hurts microsecond-scale read latency.
- **Option C: Quiescent-State-Based Reclamation (QSBR)**
  - *Pros*: Absolutely zero read overhead on the hot path.
  - *Cons*: Requires cooperative event loop / thread design where every thread passes through periodic quiescent states.

---

## 2. Raft Consensus Framework Selection
- **Option A: Production Foundation (`openraft`)**
  - *Pros*: Production-grade, highly optimized async Raft implementation with custom storage hooks, flexible network transport, and active community maintenance.
  - *Cons*: Heavy generic trait plumbing; need to carefully align async Tokio model with lock-free synchronous fast-paths.
- **Option B: Tailored In-House Raft Implementation**
  - *Pros*: Perfect control over memory layout, zero extraneous allocations, custom pipelining directly wired to Disruptor rings.
  - *Cons*: Massive surface area for subtle consensus edge cases and bug risks.

---

## 3. Threading & Execution Architecture
- **Option A: Thread-per-Core / Shared-Nothing with Disruptor IPC**
  - Worker threads pinned to CPU cores; requests routed by key hash; internal communication via cacheline-padded SPSC/MPMC lock-free ring buffers.
- **Option B: Tokio Async Runtime with Shared Lock-Free Data Structures**
  - Standard multi-threaded Tokio work-stealing runtime with lock-free concurrent hash maps / skiplists accessible across all worker tasks.

---

## 4. Linearizable Read Strategies
- **Option A: Read-Index (Raft Section 6.4)**
  - Leader checks its commit index, sends a lightweight heartbeat round to verify leadership, and returns once state machine reaches that index.
- **Option B: Leader Leases**
  - Leader relies on bounded election timeouts and local monotonic clock to serve reads directly from memory with zero network latency, assuming bounded clock drift.
