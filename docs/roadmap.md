# Roadmap

## Implemented in 0.2.0

The database has durable atomic documents and binary vectors, exact and centroid ANN search, metadata postings, BM25, background index generations, a pending-change overlay, and safe index/namespace cleanup. It includes online backups, verified restore, v1 migration, quotas, scoped tokens, metrics, local/S3 configurations, and reproducible benchmarks.

Distributed deployment supports a gateway, fixed namespace shards, active/standby workers, monotonic-time lease observation, and SlateDB fencing. All namespace queries go to their active owner. The default test suite and opt-in S3/cluster suite exercise the stated correctness and recovery paths.

These are initial implementations. Index generation still rebuilds an entire namespace in memory, the active worker serves reads and writes as well as building indexes, and a namespace cannot span workers. Build budgets and namespace quotas do not establish a hard RSS limit or a maximum useful dataset size. See [architecture](architecture.md), [indexing](indexing.md), and [validation](validation.md) for the current behavior and evidence.

## First application workload

Use private project-document search to exercise GM with real data: one namespace per project, one document per extracted text chunk, semantic and keyword queries, and metadata filters. Start with modest collections, low query concurrency, and batched updates. The application owns import, extraction, chunking, embeddings, result fusion, and the interface; those components are not currently shipped with GM.

An initial demo should import a folder, preserve links to its original files, search by meaning or terms, propagate edits/deletions, and recover after restart. It should also provide a repeatable corpus and query set for evaluating relevance and performance. This is a proposed application, not an implemented folder-watching feature.

## Engine priorities

The order below reflects current bottlenecks. These milestones have no assigned release dates or version numbers; benchmark evidence can change their order. Keep SlateDB responsible for durable storage while developing search structures and coordination above it. Reconsider storage layout boundaries only when measured read/write amplification justifies the work.

### 1. Incremental indexing and bounded-memory builds

Replace full namespace rebuilds with updates to affected vector clusters and metadata/text posting blocks. Add cluster splits/merges and a build path that can spill to disk rather than retaining the complete namespace and output in RAM. Preserve durable publication, pending-change visibility, and snapshot-safe cleanup.

Completion evidence: small update batches avoid rebuilding the whole namespace; record bytes rewritten, indexing lag, and peak memory as the corpus grows. Crash/retry tests must preserve acknowledged documents and prevent old vectors or terms from reappearing after updates/deletes.

### 2. Filter-aware hierarchical ANN and quantization

Replace flat centroid routing with a hierarchy. Use compressed filter bitmaps and cluster summaries to choose clusters containing eligible documents, avoiding materialization of every matching ID for broad filters. Then add quantized candidate scoring with full-precision reranking. Current binary blocks still store f32 components; quantization is a separate feature. Add specialized distance kernels only when profiling identifies a CPU bottleneck.

Completion evidence: compare latency, bytes fetched, and memory at the same recall target on real embeddings, including selective filters and filters poorly correlated with vector proximity. Retain exact search as an oracle and exercise each supported metric.

### 3. Compressed postings and BM25 block skipping

Encode postings compactly and maintain blocks incrementally. Store conservative score bounds and add MAXSCORE or Block-Max WAND execution so common terms do not require scoring all their matches. Index statistics and bounds must remain valid while pending updates/deletes are overlaid.

Completion evidence: scores and top-K agree with exhaustive BM25 for the same snapshot, while common-term queries visit fewer postings and transfer fewer bytes. Measure index size and update amplification alongside query latency.

### 4. Independent compute and fair concurrency

Introduce durable index jobs, separate build workers, and coordinated publication. A second SlateDB writer on the same prefix fences the first: an external indexer must return updates or artifacts through an ownership-aware publication protocol. Independent query workers need explicit committed revisions and a way to include all changes required by their selected snapshot.

Improve commit concurrency and scheduling without weakening atomicity. The current commit mutex spans every namespace in a shard and is held through durability; a slow write can delay other writes and new query snapshots. Separate resource budgets for queries, indexing, and tenants, and establish safe commit/snapshot boundaries before reducing that serialization.

Completion evidence: concurrent ingestion and indexing have measured effects on query tail latency; job retries and worker failures cannot publish incomplete indexes; reads after acknowledged writes remain fresh. Additional query workers must demonstrate useful throughput gains. Current standbys provide takeover capacity.

### 5. Sharding within a namespace

Partition a namespace's documents across workers, fan out queries, and merge global top-K results. Define a shared commit/snapshot protocol before changing placement: independent shard snapshots alone cannot preserve current namespace-wide batch semantics. Distributed BM25 also needs consistent corpus statistics. Data movement and eventual online rebalancing require an explicit migration protocol.

Completion evidence: distributed exact vector search and BM25 agree with a single-worker reference at the same namespace revision, including updates/deletes; measure distributed ANN recall against exact results. Document failure behavior when any shard is unavailable and verify that moving data does not silently lose, duplicate, or resurrect results.

## Measurement and recovery gates

Extend the [benchmark methodology](benchmarks.md) with real document embeddings, mixed reads/writes, cold caches, remote object storage, and datasets larger than cache. Track recall, p50/p99 latency, indexing lag, peak memory, object requests/bytes, and sustained compaction amplification. The existing synthetic harness needs extensions for real-corpus replay and concurrent mutation workloads.

Record measured results in `docs/benchmark/<version>.md`, matching `Cargo.toml`, with environment, commands, data distribution, and limitations. Retain previous version reports. Planned workloads and expected improvements are not benchmark results.

Expand recovery validation with network partitions between different participants, disk exhaustion, crashes at each large-index publication stage, long GC/compaction runs, rolling upgrades, and repeated restore drills. New concurrency and distribution protocols require fault tests covering their specific invariants.

## Product and operations

The API has no conditional document update, patch-by-filter, exactly-once retry ledger, hybrid rank fusion, SQL, embedding generation, or public historical snapshot management. Schema changes require a new namespace or a planned migration. Tokens are static environment credentials; there is no identity provider, automatic rotation, or tenant billing/control plane.

Backup export/restore is implemented. Scheduling, off-site storage, retention automation, and routine restore drills remain deployment responsibilities. Autoscaling, cross-namespace transactions, richer text analyzers/phrase search, and the other API features above are outside the current milestones.
