# Gengis Mimi

An object-storage database for documents, vectors, and full-text search. Written in Rust on [SlateDB](https://slatedb.io/).

GM acknowledges writes after durable storage commits them. It supports exact vector search, centroid ANN, indexed metadata filtering, and BM25. Background indexing preserves the visibility of new writes and deletes. Run one server locally, or deploy a gateway over S3 shards with active/standby workers.

This is an early implementation with tested recovery paths and measurable limits. Indexes currently rebuild a namespace in memory; large-scale performance depends on your data, filters, probing settings, and storage. See [current boundaries](docs/roadmap.md) and [benchmarks](docs/benchmarks.md).

## Where GM fits

The first target workload is private search over a project's notes and documentation: a modest collection, mostly reads, and occasional batched updates. Semantic search can find explanations with different wording; BM25 finds relevant terms; metadata filters narrow results by document type or modification date. One namespace per project keeps each collection independently searchable.

Run locally with filesystem storage, use your own S3/MinIO deployment, or [embed the Rust engine](docs/development.md#modules) directly in an application. Local mode can operate offline after installation when the application also processes documents and generates embeddings locally. MIT-licensed source and control over deployment, indexing, and cache settings make GM useful for applications that need to own their search stack.

The application supplies file import, text extraction/chunking, embeddings, and a search interface. Combining vector and keyword rankings also happens in the application today. These form a useful first demo; GM currently supplies the storage and retrieval engine.

Collection size needs validation with real document chunks and embedding dimensions. The [0.2.0 measurements](docs/benchmark/0.2.0.md) cover 5,000 synthetic 32-dimensional vectors on one host. They establish a development baseline; they do not establish production capacity or a performance advantage over other databases.

## Start locally

```sh
cargo run --release --locked -- --config configs/local.toml
```

A current Rust toolchain and native build tools are required. Development is verified with Rust 1.98.1. The server listens on `127.0.0.1:7878`; durable data goes in `./data`. All relative paths use the current working directory.

```sh
curl --fail-with-body -X PUT http://127.0.0.1:7878/v1/namespaces/demo \
  -H 'Content-Type: application/json' \
  -d '{"dimensions":3,"metric":"cosine","text_fields":["title"]}'

curl --fail-with-body http://127.0.0.1:7878/v1/namespaces/demo/write \
  -H 'Content-Type: application/json' --data-binary @examples/documents.json

# An explicit rebuild makes the example deterministic. The server also indexes
# pending writes automatically every five seconds.
curl --fail-with-body -X POST http://127.0.0.1:7878/v1/namespaces/demo/index

curl --fail-with-body http://127.0.0.1:7878/v1/namespaces/demo/query \
  -H 'Content-Type: application/json' --data-binary @examples/query.json

curl --fail-with-body http://127.0.0.1:7878/v1/namespaces/demo/search \
  -H 'Content-Type: application/json' \
  -d '{"field":"title","text":"Rust storage","top_k":2}'
```

Vectors in the examples are illustrative; applications provide embeddings. Namespace schemas are immutable. If `demo` already exists with a different schema, use a new namespace name.

## Included

- Atomic document upserts/deletes, namespaces, pagination, and snapshot reads.
- Checksummed binary vectors and centroid blocks; cosine, dot product, and squared Euclidean scores.
- Exact search as a recall reference; ANN with configurable cluster probes.
- Equality/numeric postings, selective-filter planning, and BM25 text postings.
- Durable pending changes, atomic index publication, background rebuilds, and safe generation cleanup.
- Local filesystem or S3 storage, optional disk cache, compaction, and object GC.
- Checksummed online backups, empty-database restore, and resumable v1-to-v2 migration.
- Namespace quotas, scoped bearer tokens, request deadlines, Prometheus metrics, and graceful shutdown.
- Fixed namespace shards, a gateway, and active/standby worker takeover using object-store CAS and SlateDB fencing.

## S3 and deployment

[MinIO setup](docs/minio.md) uses an existing bucket and environment credentials. [Cluster deployment](docs/cluster.md) includes complete example configurations. Start with one server; cluster mode requires S3 conditional writes and a fixed shard topology.

For a database created by v0.1, stop its server, make an offline copy, then run:

```sh
cargo run --release --locked -- --config configs/local.toml migrate
```

The object prefix remains unchanged; the format marker inside the database selects the encoding. See [backup and migration](docs/backups.md).

## Documentation

| Guide | Contents |
| --- | --- |
| [API](docs/api.md) | Routes, payloads, query plans, filters, ranking, limits |
| [Architecture](docs/architecture.md) | Durability, snapshots, keyspace, and storage responsibilities |
| [Indexing](docs/indexing.md) | Binary blocks, centroid training, publication, overlays, BM25 |
| [Operations](docs/operations.md) | Configuration, authentication, quotas, metrics, recovery |
| [Backups](docs/backups.md) | Online export, verified restore, retention, format migration |
| [Cluster](docs/cluster.md) | Placement, leases, failover, gateway behavior, limitations |
| [MinIO / S3](docs/minio.md) | Credentials, connection setup, backend validation |
| [Benchmarks](docs/benchmarks.md) | Reproducible ingestion, latency, recall, I/O measurements |
| [0.2.0 results](docs/benchmark/0.2.0.md) | Measured local/MinIO benchmarks, environment, commands, and limitations |
| [Development](docs/development.md) | Module map and local validation |
| [Roadmap](docs/roadmap.md) | Implemented capabilities and remaining engineering work |

## Validate locally

```sh
./scripts/check.sh
```

Tests cover crash recovery, stalled storage, cancelled writes, concurrent index publication, ranking, filters, BM25 corpus updates, backup corruption, migration, and quotas. Opt-in S3 tests additionally exercise fencing, compaction/GC, gateway routing, authorization, and standby takeover after a process kill.

Build the server with `cargo build --release --locked`. The executable is `target/release/gengis-mimi`. MIT licensed; see [LICENSE](LICENSE).
