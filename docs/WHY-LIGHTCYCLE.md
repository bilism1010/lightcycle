# Why Lightcycle? A Reply to "Can't We Just Use Redis?"

> Skeptic: "OMG we already have 100 of these type of in-memory, low latency things. can't we just use memcached, redis, or something else similar? why do we need lightcycle ???"

Evidence labels used below: **[doc]** official documentation fetched while writing this, **[jepsen]** Jepsen analysis, **[vendor]** vendor-published number (unaudited), **[repo]** this repository's own planning and verification files, **[unverified]** general knowledge I did not re-check against a primary source this session.

## 1. Short answer

For most workloads you do not need Lightcycle. If you want a cache, or a fast store where losing a few recent writes on failover is acceptable, use Redis, Valkey or memcached. They are mature, well tooled and well understood.

The one place this changes is a narrow combination: a replicated in-memory store that is linearizable, does not lose acknowledged writes on failover, and has microsecond-class reads. The popular fast stores give up the first two to get the third. The stores that give the first two (etcd, Consul, TiKV, FoundationDB) pay disk and quorum costs that put them in the millisecond class. Lightcycle is a bet that the Raft read optimizations (Read-Index and leader leases) plus a lock-free in-memory core can close that gap. As of today that bet is unproven: only in-process, single-node numbers exist.

**Verdict: if you can tolerate losing acknowledged writes on failover, use Redis or Valkey (or memcached for a pure cache). Lightcycle's niche applies only if you need linearizable, quorum-durable state at sub-millisecond read latency and nothing off the shelf has met that bar for you. Even then, wait for the benchmarks in section 6.**

## 2. Comparison table

| System | Replication / consistency | Persistence | Read latency class | Where it fits | Gap vs Lightcycle's goals |
|---|---|---|---|---|---|
| Memcached | None: "no crosstalk, no synchronization, no broadcasting, no replication" [doc: docs.memcached.org]. No consensus. | None; eviction by LRU [doc]. | Microsecond class (network-bound) [unverified] | Pure cache in front of a database. | No replication, no durability, no consistency story. Wrong category, which is a fair point for the skeptic. |
| Redis / Valkey (primary + replicas, Sentinel) | Asynchronous replication. Sentinel setups "do not guarantee that acknowledged writes are retained" (quoted in Jepsen's Redis post) [jepsen: aphyr.com/posts/283-jepsen-redis]. | RDB snapshots; AOF with `always` / `everysec` / `no`. `everysec` can lose about 1s [doc: redis persistence]. | Sub-millisecond class on a LAN [unverified]. Command execution is single-threaded by design [unverified]. | Cache, session store, queues and counters where bounded loss is OK. | Acknowledged writes can be lost on failover. Not linearizable. |
| Redis Cluster | "Does not guarantee strong consistency"; can lose acknowledged writes because replication is asynchronous [doc: redis.io cluster docs]. | Same as above. | Same. | Horizontal scale-out. | Same loss window, plus sharding. |
| Redis `WAIT` | Docs: "`WAIT` does not make Redis a strongly consistent store"; a synchronously replicated write can still be lost [doc: redis.io/commands/wait]. | n/a | Adds a replica round trip. | Reducing, not eliminating, loss risk. | Not linearizable and not a consensus protocol. |
| RedisRaft | Raft on Redis; claims strong consistency. README: "not yet ready for any real production use" [doc: github.com/RedisLabs/redisraft]. | Raft log + snapshots. | Unknown. | Nothing in production. | Jepsen's early build: 21 issues, including lost data on failover and stale reads; "essentially unusable" at the first build tested, mostly fixed later [jepsen: redis-raft-1b3fbf6]. It is also a C module bolted onto a single-threaded server. |
| Dragonfly | Primary/replica "similarly to Valkey"; no consensus described; manual failover documented [doc: dragonflydb.io replication]. | Snapshots (details not checked). | Vendor claims 25x Redis throughput on a 64-thread Graviton2 box; p99 "a little higher" (about +0.2ms) [vendor: dragonflydb.io blog]. | High-throughput single-node Redis replacement. | Same consistency class as Redis, so the same loss window. |
| KeyDB | Multithreaded Redis fork; optional active-active multi-master [doc: github.com/Snapchat/KeyDB]. Async or sync not stated in what I fetched. | Redis-style. | Redis-class. | Throughput-hungry Redis users. | Multi-master means conflict resolution, not linearizability. README says no commercial support. |
| Garnet (Microsoft) | "Asynchronous replication ... leader-follower"; docs state replicas may miss records "resulting in lost writes" on primary failure [doc: microsoft.github.io/garnet replication]. | Checkpoints + AOF; tiered storage. | Microsoft Research page: client latencies "often less than 300 microseconds at the 99.9th percentile" on Azure VMs [vendor]. | Closest in latency ambition, with a Redis-compatible protocol. | Async replication, so no linearizable failover. |
| Aerospike | Strong Consistency mode (Enterprise only): all replicas ack before commit; linearizable single-record reads available [doc: aerospike.com consistency]. | Disk-oriented. | Vendor-positioned as low latency; no numbers in sources I fetched. | Large-scale, SSD-backed, high-throughput stores. | Jepsen found stale/dirty reads in AP mode, and in SC mode lost acknowledged writes on crashes until a flush-before-ack option was added. SC mode is not open source. Single-record scope [jepsen: aerospike-3-99-0-3]. |
| Hazelcast | CP Subsystem uses Raft for linearizable structures (CPMap, locks, atomics); maps are AP [doc: docs.hazelcast.com]. | Optional. | JVM; no numbers fetched. | Java-centric data grids. | The CP part is a small set of structures, not a general fast KV. JVM GC pauses are a tail-latency risk [unverified]. |
| etcd / Consul | Raft; etcd reads are linearizable by default, and `serializable` reads are stale-but-fast [doc: etcd api_guarantees]. | WAL + bbolt on disk. | etcd's published benchmark (v3.2.0, GCE SSD): linearizable read 0.7ms at 1 client, 5.5ms at 1,000 clients; writes 1.6ms and 22ms [doc: etcd performance, old numbers]. | Config, coordination, leader election. | Right semantics, wrong latency and throughput. Not designed for a hot data path. |
| TiKV | Raft per region; transactions; RocksDB storage [unverified]. | Disk (LSM). | Millisecond class [unverified]. | Distributed transactional storage. | Disk and distributed-transaction costs. I could not fetch a primary source this session. |
| FoundationDB | Strict serializability. Docs report read latency 0.1 to 1ms and commit about 1.5 to 2.5ms under 75% load; about 90k reads/s per core in the memory engine (old SATA-SSD hardware) [doc: apple.github.io/foundationdb/performance.html]. | Durable commits. | Hundreds of microseconds to a millisecond for reads; around 2ms for commits. | Transactional layer for databases. | The closest consistency-plus-speed precedent, but its latency is not Lightcycle's 150us target and it is a much larger system. |
| Anna (UC Berkeley) | Coordination-free; lattice-based eventual consistency [unverified; not fetched]. | Optional tiers. | Fast by design. | Research on elastic KV. | Deliberately gives up linearizability. Opposite end of the design space. |

## 3. The specific technical gap

The three properties are consistency, durability and latency. Each class of system gets two.

1. **Fast and in-memory, but lossy on failover.** Redis, Valkey, Dragonfly, KeyDB and Garnet use asynchronous replication. Their own docs say acknowledged writes can be lost: the Redis Cluster docs, the Garnet replication page, and Jepsen's Sentinel test, where 1,998 of 2,000 writes were acknowledged but only 872 survived a partition (about 56% lost) [jepsen]. That test is from the Redis 2.6 era and the product has changed, but the structural reason is the same. The primary acknowledges before replicas have the write, and a promoted replica may not have it.
2. **Why bolting on `WAIT` does not close it.** The Redis docs say `WAIT` does not make Redis strongly consistent. Failover promotes a replica chosen on a best-effort basis, so a write that reached replicas can still be lost. Even if you got durability, linearizability needs a replicated state machine with a leader election that cannot produce two primaries. Sentinel and Cluster failure detection are not that.
3. **Why bolting Raft on does not close it either.** RedisRaft is the attempt, and its README disclaims production use. Jepsen found 21 issues in the first builds, including total data loss on failover and stale reads in healthy clusters. It was reported as largely fixed later, with Jepsen noting that testing is not proof of correctness [jepsen]. Redis's single-threaded execution model is also a poor fit for pipelined consensus.
4. **Strong, but not fast.** etcd gives linearizable reads and strictly serializable writes, but linearizable reads go through Raft, and its published benchmark shows 0.7ms at a single client and 5.5ms with 1,000 clients on SSD hardware from the v3.2 era [doc]. Its `serializable` read option is fast but may be stale, so you are back to the first problem on the read side.
5. **The unoccupied corner.** FoundationDB shows strong consistency can reach roughly 0.1 to 1ms reads, but with a transaction-system cost. Aerospike SC mode shows strong consistency with replication on a performance-oriented store, but it is proprietary, record-scoped, and Jepsen found a crash-durability loss window until a flush option was added. A small open-source KV with a Redis wire format, linearizable reads served from memory on the leader, and quorum-durable writes is not something I found a mature example of. That absence may mean there is little demand, or that it is harder than it looks.

The theoretical lever is straightforward. A linearizable read does not need to touch disk or run a full log round. It needs proof that the leader is still the leader (Read-Index: one heartbeat round, or Leader Lease: zero network if the clock assumptions hold) and that the state machine has applied up to the read index [repo: ARCHITECTURE_SPIKES.md; Raft thesis section 6.4, not fetched by me]. Writes still pay one quorum round trip plus an fsync, which is why the write target is milliseconds, not microseconds.

## 4. Technical defense of the design choices

- **Lock-free index with epoch-based reclamation.** The Phase 2 implementation uses `crossbeam-skiplist` and `crossbeam-epoch`, with replaced values going through `defer_destroy`; CAS is a pointer compare-exchange, and a 16-thread by 2,000-increment CAS test gives an exact total [repo: 02-VERIFICATION.md]. The measured local GET p99 is 250ns on 1 thread and 500ns on 8 threads, in-process only [repo]. This supports the narrow claim that the read path will not be what blows the 150us budget. It does not say anything about end-to-end latency. Epoch reclamation's known cost, that a stalled thread delays reclamation and bloats memory, is noted in the project's own spike document [repo].
- **Slab arenas.** Slab-sized blocks recycled through EBR avoid general allocator calls on the hot path. Phase 2 verification confirms slab recycling via EBR in engine tests [repo]. Be aware that the verification file also flags that the pool is unbounded and never returns memory to the OS, so the memory-bounding requirement is not yet enforced [repo]. Memcached has used slab classes for similar reasons for years [unverified], so this is a known-good idea, not a novelty.
- **Raft with Read-Index and Leader Lease.** Without this, a Raft store is stuck with the etcd-style costs above. Lease reads are what moves linearizable reads from the "quorum round trip" class to the "local memory" class. Leases have a real price, covered in section 5.
- **`openraft` rather than a custom Raft.** The spike document lists the custom option's risk as "massive surface area for subtle consensus edge cases" [repo]. The project plans to integrate an existing library, which reduces (but does not remove) consensus-bug risk.
- **Group commit.** Batching many log entries into one fsync is the standard way to amortize fsync cost. Redis's own `appendfsync always` mode does the same thing within a batch [doc: redis persistence], so the idea is sound. It is the reason a 3-node write p99 under 2ms is plausible rather than absurd, but it is still a target, not a result.
- **RESP compatibility.** Reusing the Redis protocol means existing client libraries work for the supported subset (GET, SET, MGET, DEL, WATCH, CAS) [repo]. This lowers adoption cost, though it also invites the expectation of full Redis compatibility, which will not hold.

## 5. Where the skeptic is right

- **Most use cases are already covered.** If the data is a cache, or can be rebuilt from a source of truth, lost-on-failover is a non-issue and Redis/Valkey/memcached win on every other axis.
- **Ecosystem and operations.** Redis has client libraries in every language, managed offerings, monitoring, backup tooling, a large body of operational knowledge, and a Jepsen-and-production track record. Lightcycle has none of that. Operational maturity is the main reason people pick boring tools.
- **Garnet and Dragonfly are close on the latency and throughput axis.** If your pain is single-node throughput or tail latency under many connections, they may fix it without any consistency change.
- **Consensus is hard.** Raft implementations routinely have bugs (see the RedisRaft Jepsen result above). Using `openraft` helps, but snapshotting, membership change, pipelining and lease reads are exactly where subtle bugs live.
- **Leader leases rest on clock assumptions.** If clock drift or a long process pause exceeds the assumed bound, a deposed leader can serve a stale read. Jepsen's Aerospike report found a similar class of problem, where a long node pause (27 seconds in that test) could cause committed updates to be lost [jepsen]. Lease reads should be optional, with Read-Index as the safe fallback, and the clock-drift bound must be explicit and tested.
- **Most of the product is not built.** Phase 2 of 6 is done. Raft, the WAL, the wire protocol and cross-system benchmarks do not exist yet [repo].

**What is unproven today:**
- The only numbers are in-process local-tier microbenchmarks (GET p99 250ns / 500ns). They include no network, no RESP parsing, no Raft, and no WAL.
- The benchmark prints PASS/FAIL but is not a CI gate [repo].
- There is no cluster benchmark, no head-to-head comparison against any other system, and no Jepsen or Elle run.
- The "zero data races" evidence rests on a recorded UAT pass; the verifier could not reproduce Miri or TSan runs in its environment [repo].
- The 150us read target is not yet tested over a real network path. Network and kernel overhead alone may consume a large share of it.

## 6. What would falsify the project

Lightcycle is not worth continuing as a product if any of these hold once the pieces exist:

1. **Latency.** Over real TCP on the same hardware, a 3-node cluster cannot hold P99 read under 150us (leader lease) and P99 write under 2ms at the target load, measured with a coordinated-omission-safe tool (HdrHistogram with intended-send-time correction, or wrk2-style constant-rate load). If the measured read p99 is no better than Redis on the same box, the lock-free core bought nothing.
2. **Head-to-head.** Run Redis/Valkey (with `WAIT` and AOF `always`), Dragonfly, Garnet, etcd and ideally FoundationDB on identical hardware and workload. Lightcycle has to beat etcd decisively on linearizable reads, and match Redis/Garnet-class latency on reads while providing a stronger guarantee. If it only matches etcd, there is no niche.
3. **Throughput.** More than 500k ops/s per node at those latencies. Throughput numbers without a stated latency percentile do not count.
4. **Correctness.** A Jepsen-style test (Knossos or Elle for linearizability) with partitions, kills, clock skew and process pauses, run against both Read-Index and lease modes. Any lost acknowledged write or stale linearizable read invalidates the central claim. A passing result is still only evidence, as Jepsen itself says of RedisRaft [jepsen].
5. **Durability.** Kill -9 and power-loss tests confirming that an acknowledged write survives a quorum-wide restart when fsync-on-ack is enabled.
6. **Tail behavior.** P99.9 and max under sustained load, snapshotting, and membership change, because a median that looks good with a bad tail is the failure mode the project says it exists to avoid.

Also useful: a cost comparison. If Redis + `WAIT` + AOF `always` + a conflict-tolerant application design meets the actual business requirement, that is the right answer regardless of what Lightcycle achieves.

## 7. Recommendation and framing

**For the decision at hand:** use Redis or Valkey (or memcached) for any workload that is a cache or tolerates bounded write loss. Use etcd or FoundationDB if you need linearizable state and a millisecond is acceptable. Do not put Lightcycle on any production path today; it has no network or Raft layer yet. Revisit if (a) you have a concrete workload that needs linearizable reads below a millisecond and (b) the section 6 benchmarks and a Jepsen run exist.

**Framing:** the repository describes itself as "ACTIVE RESEARCH" [repo: README badge] and lives in a systems-programming research directory. The README and project docs frame it as a project exploring lock-free structures, epoch reclamation, Disruptor-style rings and pipelined Raft, and the spike document is explicitly a comparison of design options (hazard pointers vs. EBR vs. QSBR, `openraft` vs. custom, thread-per-core vs. Tokio) [repo]. That is what a learning and research project looks like, and it is fair to say so plainly. The honest pitch is not "everyone should switch to Lightcycle". It is that the consistency/latency/durability corner is under-served, the techniques to attack it are well documented, and building it is a rigorous way to test whether the corner is empty because it is hard or because nobody needed it. Publishing the negative result would also be a valid outcome.

## 8. Sources

Fetched this session:
- Redis WAIT: https://redis.io/docs/latest/commands/wait/
- Redis Cluster docs: https://redis.io/docs/latest/operate/oss_and_stack/management/scaling/
- Redis persistence: https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/
- Jepsen on Redis (Sentinel): https://aphyr.com/posts/283-jepsen-redis
- Jepsen on Redis-Raft: https://jepsen.io/analyses/redis-raft-1b3fbf6
- RedisRaft README: https://github.com/RedisLabs/redisraft
- Memcached docs: https://docs.memcached.org/
- Dragonfly replication: https://www.dragonflydb.io/docs/managing-dragonfly/replication
- Dragonfly benchmark blog (vendor): https://www.dragonflydb.io/blog/scaling-performance-redis-vs-dragonfly
- KeyDB: https://github.com/Snapchat/KeyDB
- Garnet research page (vendor): https://www.microsoft.com/en-us/research/project/garnet/
- Garnet replication docs: https://microsoft.github.io/garnet/docs/cluster/replication
- Aerospike consistency: https://aerospike.com/docs/server/architecture/consistency
- Jepsen on Aerospike: https://jepsen.io/analyses/aerospike-3-99-0-3
- Hazelcast CP Subsystem: https://docs.hazelcast.com/hazelcast/latest/cp-subsystem/cp-subsystem
- etcd API guarantees: https://etcd.io/docs/v3.5/learning/api_guarantees/
- etcd performance: https://etcd.io/docs/v3.5/op-guide/performance/
- FoundationDB performance: https://apple.github.io/foundationdb/performance.html

Local: README.md, idea.md, .planning/PROJECT.md, .planning/research/ARCHITECTURE_SPIKES.md, .planning/phases/02-lock-free-core/02-VERIFICATION.md.

Not verified (fetch failed or not attempted): TiKV (Raft per region, RocksDB, latency), Anna, Valkey specifics, the Garnet VLDB/SIGMOD paper, Raft thesis section 6.4 text, Redis single-threaded execution and memcached threading/slab details, Hazelcast and Aerospike latency numbers, Dragonfly persistence and failover specifics beyond the replication page.
