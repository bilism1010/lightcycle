# Lightcycle - Planning State

- **Current Status**: Phase 2 Planned and Verified. Ready for execution.
- **Active Phase**: 02-lock-free-core
- **Next Step**: Execute Phase 2 tasks (`/gsd-execute-phase 2`).
- **Decisions Record**:
  1. **Consensus Framework**: `openraft` selected for robust, battle-tested async Raft foundation.
  2. **Memory Reclamation**: Epoch-Based Reclamation (EBR via `crossbeam-epoch`) chosen for sub-microsecond lock-free reads.
  3. **Threading Architecture**: Hybrid model — Tokio for network I/O & Raft RPCs, with dedicated core-pinned worker threads running lock-free cache arenas and Disruptor ring buffers.
  4. **Phase 2 Index**: `crossbeam-skiplist` selected as core concurrent ordered index for predictable latency and ordered range traversal.
