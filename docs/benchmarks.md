# Reproducible benchmarks

The Rust benchmark creates a unique database prefix, generates seeded vectors/documents, ingests them in batches, builds indexes, closes the writer, removes only its own uniquely named cache, and reopens. It records the first query after reopening, explicitly warmed queries, exact top-10 references, a selective filter, and BM25. It retains its database prefix for inspection.

```sh
cargo build --release --locked --example benchmark
./target/release/examples/benchmark \
  --config configs/benchmark.toml --documents 5000 --dimensions 32 \
  --queries 100 --probes 8 --metrics target/local.metrics \
  > target/local-benchmark.json

# Existing MinIO bucket and exported AWS credentials:
./target/release/examples/benchmark \
  --config configs/minio.toml --documents 5000 --dimensions 32 \
  --queries 100 --probes 8 --metrics target/minio.metrics \
  > target/minio-benchmark.json
```

The local config uses disposable files under `target`; normal application data is separate. S3 runs use a fresh `.../bench-<uuid>` prefix. Give benchmarks their own storage/cache resources when measuring independently of an application workload. Stop any local server sharing the configured durable root; filesystem ownership is exclusive.

For memory and process CPU measurements, prefix the command with `/usr/bin/time -l` on macOS or `/usr/bin/time -v` on Linux. Record the compiler/build profile, hardware, object store, network distance, data distribution, dimensions, filters, and concurrency with results.

## Interpret the output

- `documents_per_second` and `index_seconds` measure durable ingestion and a complete index build.
- `reopen_seconds` measures opening/recovery with an empty GM disk cache. Startup itself may load data; report it alongside `cold_query_ms`.
- `cold_query_ms` is the first query after reopening. It does **not** flush the operating system or object server's caches and is not a cold cloud-region benchmark.
- `warm_p50_ms` / `warm_p99_ms` follow one explicit warmup for each query. Small sample counts make p99 effectively the slowest observation, not a service-level prediction.
- `recall_at_10` is the mean fraction of exact top-10 IDs returned by ANN. The data and query vectors are deterministic uniform f32 components in [-1, 1], seed 42. This synthetic distribution is not a proxy for a particular embedding model.
- `mean_candidates` counts vectors/IDs visited by ANN; it is not a remote request count.
- `exact_p50_ms` times primary-data exact search as a reference.
- `filtered_candidates` / `filtered_plan` demonstrate filter-first exact scoring for one percent of documents.
- `ingest_index_io` includes opening, ingestion, indexing, cleanup, and the closing flush. `query_io` includes reopen, warmups, ANN, exact reference queries, filtering, and BM25 together.

`gm_object_store_read_bytes` counts payload bytes delivered at the object-store boundary below SlateDB caches; `gm_object_store_written_bytes` counts accepted uploads/parts. These exclude HTTP headers, provider-internal retry traffic, and server-side copying. `gm_object_store_calls` counts raw API calls below caches; one range/list/multipart operation can expand to several provider requests. The detailed metrics also contain SlateDB request/error counts, cache hits, WAL flushes, and compaction bytes. Some expected missing-object probes count as errors during creation/recovery.

The metrics artifact has two labelled sections for separate database handles, whose counters restart. Do not ingest the concatenated file as one live Prometheus scrape. The server's `/v1/metrics` endpoint is a normal single-handle exposition.

## Compare settings

Repeat with probes 4, 8, 16, and at least the cluster count. More probing should converge to exact recall; it also approaches full vector scanning. Compare realistic clustered embeddings and selective filters before selecting defaults. Vary namespace size, dimensions, write/query concurrency, and index lag. Measure both request count and bytes: an SST/cache-part fetch can be larger than a logical vector block.

Compaction amplification needs sustained repeated updates, not only a fresh bulk load. Compare cumulative accepted object bytes with application bytes written over the same observation window, and inspect compactor counters. The S3 integration test forces compaction progress and verifies recovery after GC, but its short duration is not a long-running soak test.

## Planned workload coverage

The current example generates synthetic vectors and runs sequential queries. The following scenarios require harness extensions; they have not been established by the 0.2.0 report.

| Workload | Evidence to collect |
| --- | --- |
| Project-document search | Record extraction/chunking rules, embedding model and dimensions, corpus size, and fixed queries. Measure ANN recall against exact retrieval; separately judge whether results answer the query. |
| Ongoing edits and deletions | Replay mutations during indexing and queries. Record commit latency, indexing lag, bytes rewritten, query p99, and stale-result checks at known revisions. |
| Filtered vectors and common text terms | Vary filter selectivity and its correlation with vector proximity; include frequent BM25 terms. Record candidate/posting counts, recall, memory, and bytes read. |
| Multiple callers and larger-than-cache data | Exercise HTTP and embedded paths separately, including multiple namespaces. Record queueing, CPU/RSS, errors, object-store distance, and exactly which caches were cleared or remained warm. |

Keep dataset and recall targets fixed when comparing index changes. Include any warmup, rebuilding, or cache population outside the timed query window in the report. These measurements should guide the [engine priorities](roadmap.md#engine-priorities) before making capacity or latency claims.

Store release measurements in `docs/benchmark/<version>.md`, matching the package version in `Cargo.toml`. The [0.2.0 report](benchmark/0.2.0.md) records measured local and MinIO latency, recall, memory, throughput, and I/O, including a full-probe control. Keep earlier reports when adding a new version so changes remain comparable. These are development references rather than capacity promises; correctness checks are recorded separately in [validation](validation.md).
