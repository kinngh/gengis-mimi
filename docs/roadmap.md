# Roadmap

## v0.1: current implementation

One Rust binary; SlateDB on local filesystem or S3; namespaced documents; durable atomic writes; exact vector search; metadata predicates; optional disk cache; recovery and HTTP tests.

This is intended for development, correctness experiments, and small datasets. Exact search costs O(N × dimensions) per query and can download the full namespace when cold. JSON vectors and full-document decoding favor simplicity over space and bandwidth efficiency. No performance numbers are promised yet.

## Next: establish a baseline

- Add repeatable ingestion and cold/warm query benchmarks on local storage and MinIO.
- Measure resident memory, object-store requests/bytes, and compaction amplification.
- Exercise storage failures, prolonged outages, writer fencing, and long-running compaction/GC.
- Define consistent exports, checkpoint retention, and a restore procedure for live backups.

## Then: centroid ANN

- Design compact vector blocks and cluster key ranges.
- Build a basic centroid index and configurable candidate probing.
- Publish index changes atomically with an indexing watermark.
- Keep updates and deletes immediately visible while indexing lags.
- Compare recall against exact search, including selective filters.

The index publication and unindexed-write semantics need their own design; SlateDB does not implement them for us. Incremental split/merge rebalancing follows only after the simpler index is measurable and correct.

## Later

Full-text indexing and BM25, attribute indexes, quantization, distributed namespace placement, separate indexers/readers, per-namespace authorization, metrics, and operational tooling.

There is currently no ANN, full-text ranking, SQL, embedding generation, namespace deletion, conditional document update, exactly-once retry ledger, cross-namespace transaction, public historical snapshot API, automatic migration, or automatic failover. Add these against concrete needs rather than expanding the core speculatively.
