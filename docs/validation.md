# Validation record

Development environment: macOS ARM64, Rust 1.98.1, GM 0.2.0, SlateDB 0.16.0. Local and MinIO storage tests use isolated prefixes/directories. MinIO was built locally from tag `RELEASE.2025-09-07T16-13-09Z` with Go 1.24.6 and bound to loopback for validation.

The local suite covers vector block integrity, atomicity/isolation, process-kill recovery, cancellation during stalled WAL storage, exact/ANN/filter agreement, BM25 statistics across updates, concurrent index publication, quota enforcement, snapshot backup corruption, restore, and migration resumption. The S3 suite verifies writer fencing, compaction progress, offline GC/reopen, gateway permissions and placement, and standby takeover after killing the active worker.

Commands are reproducible through `scripts/check.sh` and `scripts/check-s3.sh`. The final 0.2.0 run passed all 15 default tests and both opt-in S3 tests against MinIO. Formatting, Clippy with warnings denied, Rustdoc, and locked release builds of the server and examples also passed. All nine example configuration files validated.

Additional release CLI/API smoke checks exercised the documented example documents, explicit indexing, filtered ANN, BM25, metrics, online backup, and offline restore into a fresh database. During a MinIO process outage, a submitted write returned HTTP 504 after its request deadline without a premature success acknowledgement. After MinIO restarted, the write completed atomically, and a GM restart recovered both its changes and previously acknowledged data. A timeout remains an unknown write outcome; callers must read back or retry the intended upserts/deletes.

Measured results are in [benchmarks for 0.2.0](benchmark/0.2.0.md), with [methodology](benchmarks.md). Cloud-region latency, large production datasets, disk exhaustion, and exhaustive partition/failure schedules have not been established by these checks.
