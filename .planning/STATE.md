# Lightcycle - Planning State

- **Current Status**: Onboarding complete. Phase 1 active.
- **Active Step**: Key architectural decisions locked in; preparing technical specs for Phase 2.
- **Decisions Record**:
  1. **Consensus Framework**: `openraft` selected for robust, battle-tested async Raft foundation with custom storage & transport.
  2. **Memory Reclamation**: Epoch-Based Reclamation (EBR via `crossbeam-epoch`) chosen for sub-microsecond lock-free reads and balanced reclamation overhead.
  3. **Threading Architecture**: Hybrid model — Tokio for network I/O & Raft RPCs, with dedicated core-pinned worker threads running lock-free cache arenas and Disruptor ring buffers.
