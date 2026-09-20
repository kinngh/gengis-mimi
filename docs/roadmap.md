# Current scope and remaining work

## v0.2 capabilities

The database has durable atomic documents and binary vectors, exact and centroid ANN search, metadata postings, BM25, background index generations, a pending-change overlay, and safe index/namespace cleanup. It includes online backups, verified restore, v1 migration, quotas, scoped tokens, metrics, local/S3 configurations, and reproducible benchmarks.

Distributed deployment supports a gateway, fixed namespace shards, active/standby workers, monotonic-time lease observation, and SlateDB fencing. All namespace queries go to their active owner. The default test suite and opt-in S3/cluster suite exercise the stated correctness and recovery paths.

## Search scale

Current ANN uses a flat centroid index and whole-namespace rebuilds. Next steps, driven by recall/I/O benchmarks, are incremental cluster maintenance, bounded external-memory construction, hierarchical routing, quantization, and specialized vector kernels. Common filter sets are materialized as ID sets; compressed postings/bitmaps and better cardinality planning would reduce memory and I/O. BM25 reads term postings but has no block-max skipping or phrase/proximity analysis.

Build input/output checks and namespace quotas bound configured work; they do not establish a hard RSS limit or a universal maximum useful dataset size. The current benchmark is a small baseline, not a production capacity claim.

## Distributed scale

There is no intra-namespace sharding, online rebalancing, independent read-replica tier, external index-build queue, autoscaling controller, or cross-shard transaction. These require explicit snapshot/freshness and publication protocols. Adding machines to the worker list provides standby capacity, not additional read throughput for one shard.

## Product and operations

The API has no conditional document update, patch-by-filter, exactly-once retry ledger, hybrid rank fusion, SQL, embedding generation, or public historical snapshot management. Schema changes require a new namespace or a planned migration. Tokens are static environment credentials; there is no identity provider, automatic rotation, or tenant billing/control plane.

Backup export/restore is implemented. Scheduling, off-site storage, retention automation, and routine restore drills remain deployment responsibilities. Further fault testing should include network partitions of different participants, disk exhaustion, large-index crashes at each publication stage, long GC/compaction soak runs, and rolling upgrades.
