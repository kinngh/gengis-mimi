# Gengis Mimi

An object-storage-backed document and vector database, written in Rust and built on [SlateDB](https://slatedb.io/).

Gengis Mimi runs as one HTTP server. SlateDB stores its WAL, manifests, and immutable tables in your object store. The server acknowledges writes only after they reach durable storage. RAM and the optional disk cache can be discarded.

**This is an early, working foundation:** atomic document batches, namespaces, consistent reads, and exact vector search. Search currently scans the namespace; ANN, full-text search, and distributed serving are future work. 

## Run locally

Use a recent stable Rust toolchain and a native C/C++ build toolchain for dependencies. Development is currently verified with Rust 1.98.1.

```sh
cargo run --release --locked -- --config configs/local.toml
```

The server listens at `http://127.0.0.1:7878`. Durable files go in `./data`. Running without `--config` uses the same defaults. All relative paths resolve from your working directory.

In another terminal, from this folder:

```sh
# Create a namespace with three-dimensional cosine vectors.
curl --fail-with-body -X PUT http://127.0.0.1:7878/v1/namespaces/demo \
  -H 'Content-Type: application/json' \
  -d '{"dimensions":3,"metric":"cosine"}'

# Atomically insert three documents.
curl --fail-with-body http://127.0.0.1:7878/v1/namespaces/demo/write \
  -H 'Content-Type: application/json' \
  --data-binary @examples/documents.json

# Search with attribute filters.
curl --fail-with-body http://127.0.0.1:7878/v1/namespaces/demo/query \
  -H 'Content-Type: application/json' \
  --data-binary @examples/query.json

# Retrieve, scan, and delete documents.
curl --fail-with-body http://127.0.0.1:7878/v1/namespaces/demo/documents/rust
curl --fail-with-body 'http://127.0.0.1:7878/v1/namespaces/demo/documents?limit=2'
curl --fail-with-body -X DELETE http://127.0.0.1:7878/v1/namespaces/demo/documents/cooking
```

The example vectors are hand-written for demonstration. Applications supply their own embeddings. Stop with Ctrl-C; restart with the same config to reopen the database.

## What works

- Immutable namespace configuration, with optional vector dimensions.
- Full-document upserts and deletes in one atomic batch.
- Document retrieval and cursor-based scans.
- Exact cosine, dot-product, and squared Euclidean ranking.
- Equality and inclusive numeric-range filters applied before ranking.
- Durable reads and a consistent snapshot for each query or scan page.
- Local filesystem storage with `fsync`, or S3-compatible object storage.
- Optional bounded disk cache, background compaction, and storage cleanup through SlateDB.
- Bounded query concurrency, request-size limits, optional bearer authentication, and graceful shutdown.

## S3 and MinIO

`configs/minio.toml` is ready for a MinIO server at `127.0.0.1:9000` and a pre-created `gengis-mimi` bucket. `configs/s3.toml` is the AWS example. Credentials come from the environment or the object-store client's credential provider; they do not belong in TOML.

See [the MinIO guide](docs/minio.md) for setup, credentials, and the opt-in integration test. A MinIO instance is not included or started by this project.

## Documentation

| Guide | Contents |
| --- | --- |
| [API](docs/api.md) | Routes, payloads, filters, scores, pagination, limits, errors |
| [Architecture](docs/architecture.md) | Storage layout, durability, snapshots, SlateDB responsibilities |
| [Configuration and operations](docs/operations.md) | Settings, cache ownership, authentication, recovery, backups |
| [MinIO / S3](docs/minio.md) | Connection setup and optional backend test |
| [Development](docs/development.md) | Code map, checks, integration tests, design rules |
| [Roadmap](docs/roadmap.md) | Boundaries of v0.1 and next milestones |

## Development

```sh
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked --all-targets
cargo doc --locked --no-deps
```

Tests cover recovery after an abrupt process kill, empty-cache reopening, atomic batches, namespace isolation, durable visibility, concurrent queries/writes, ranking, and HTTP behavior. S3 tests are opt-in and require a bucket.

For normal deployment, build with `cargo build --release --locked` and run `target/release/gengis-mimi`. Run one server per database prefix. The local data directory is the durable backend; deleting it deletes the database. With S3, only the separately configured cache is disposable.

MIT licensed. See [LICENSE](LICENSE).
