# Development

## Local checks

```sh
./scripts/check.sh
# With credentials and an existing test bucket:
./scripts/check-s3.sh
cargo build --release --locked --bins --examples
```

The check scripts keep Cargo downloads in `.cargo-home` and temporary files in `.tmp`, both ignored. They run formatting, Clippy with warnings denied, all default tests, and Rustdoc. S3/cluster tests are opt-in. Native dependencies require C/C++ build tools and CMake as appropriate for your platform.

Development is verified with Rust 1.98.1 on macOS ARM64. Cargo declares a Rust 1.89 baseline, which has not separately been verified. The exact SlateDB version and lockfile are pinned. Use a recent stable compiler for the documented checks.

## Modules

| Module | Responsibility |
| --- | --- |
| `main.rs` | Serve/backup/restore/migrate/reindex CLI, startup/shutdown |
| `config.rs` | Typed configuration and backend builder |
| `engine.rs` | Commit serialization, snapshots, namespace lifecycle, quotas, index publication |
| `binary.rs` | Versioned, checksummed vector blocks |
| `index.rs` | Centroid training, block/posting construction, tokenization |
| `search.rs` | Exact/ANN plans, pending-change overlay, filters, BM25 |
| `backup.rs` | Consistent export, validation, restore, resumable migration |
| `cluster.rs` | Gateway placement, lease ownership, standby takeover |
| `api.rs` | HTTP contract, scoped tokens, admission, deadlines |
| `metrics.rs` | Prometheus exposition over SlateDB/application metrics |
| `examples/benchmark.rs` | Seeded ingestion/search/recall measurements |

The library exposes `Engine` and API types. `Engine::open_with_store` supports embedded/custom stores and deterministic fault injection. The embedded API does not automatically start an indexer: call `run_indexer` with a stop watch receiver or explicitly rebuild. The server CLI starts the indexer automatically. Drain callers, stop background work, and call `close` explicitly.

## Tests and invariants

Tests focus on behavior that could lose data or return incorrect search results:

- Atomic batches, namespace isolation, durable reopen, and actual process kill recovery.
- A stalled WAL cannot acknowledge; a cancelled writer retains commit serialization until durability.
- Concurrent queries never observe a partial application batch.
- All-cluster ANN equals exact results, filters narrow candidates, updates/deletes remain visible during rebuilding, and indexed results survive reopen.
- BM25 statistics and scores agree before and after rebuilding across mutations.
- Quota rejection leaves the whole batch unchanged.
- Snapshot backups retain the captured version despite later writes/cleanup; corrupt files leave restore destinations untouched.
- Migration resumes with a mixture of legacy JSON and converted binary vectors.
- Vector decoding rejects malformed/checksum-invalid/nonfinite data.
- S3 fencing, compaction progress, offline GC, gateway authorization, and standby recovery.

Filesystem tests create and clean unique directories under `target/test-data`. Process tests bind loopback and clean up their child processes. S3 fixtures intentionally retain unique object prefixes for inspection. A passing suite establishes these tested behaviors; it is not a proof against all distributed failure schedules.

## Design rules

Keep application mutations in the durable batch with their change record and counters. Preserve the snapshot boundary across index metadata, candidate retrieval, pending changes, and result hydration. Publish indexes only after all referenced blocks are durable. Retire generations through SlateDB writes, preserving snapshot protection.

Keep exact search as a reference when changing approximate retrieval. Measure recall, cold/warm latency, transfer volume, and memory before adding optimization layers. Keep externally visible behavior and format changes explicit; avoid hiding data movement behind a topology change or silently adopting an older schema.

Changing the SlateDB dependency requires recovery/restore testing on a copy of data as well as compilation. GM's explicit v1-to-v2 migration covers its own document encoding; it does not guarantee arbitrary storage-library compatibility.
