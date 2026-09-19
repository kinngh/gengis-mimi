# Development

## Build and check

Use a current stable Rust toolchain; this implementation was built with Rust 1.98.1. Cargo declares a 1.89 language baseline, but that older toolchain is not currently tested. Native dependencies may need a C/C++ compiler, CMake, and platform build tools. macOS development uses Xcode Command Line Tools; Linux builds typically use build-essential, CMake, and pkg-config.

```sh
cargo build --locked
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked --all-targets
cargo doc --locked --no-deps
```

`cargo build --release --locked` produces `target/release/gengis-mimi`. Run from this directory to use the example configs unchanged. The first build downloads and compiles SlateDB's dependency tree.

To keep Cargo's downloads and temporary build files inside the repository as well:

```sh
mkdir -p .cargo-home .tmp
CARGO_HOME="$PWD/.cargo-home" TMPDIR="$PWD/.tmp" cargo test --locked --all-targets
```

Both directories are ignored by Git, as are `target`, local data, and cache files. Cargo.lock is checked in because this is a runnable database service.

## Code map

| File | Responsibility |
| --- | --- |
| `src/main.rs` | CLI, logging, startup, listener, shutdown |
| `src/config.rs` | TOML schema, defaults, validation |
| `src/engine.rs` | Backend setup, durable writes, namespaces, snapshots, scans |
| `src/model.rs` | API data types, ID/vector/filter validation |
| `src/search.rs` | Exact distance kernels and bounded top-K heap |
| `src/api.rs` | HTTP routes, bearer auth, request admission |
| `src/error.rs` | Application errors and HTTP mapping |

The library exports `Engine`, `Config` through `config`, and document/query types through `model`. The HTTP layer calls those same operations. Close the engine explicitly after draining its callers.

## Tests worth maintaining

The test suite focuses on database guarantees:

- Reads and searches hide updates and tombstones until the WAL is durable.
- Acknowledged writes survive an actual child-process kill and restart with an empty cache.
- Invalid batches do not partly apply.
- Namespace key ranges and pagination never spill into neighboring namespaces.
- Updates, deletes, filters, tie-breaking, and each distance metric affect ranking correctly.
- Queries stay internally consistent while whole-document batches change concurrently.
- Conflicting namespace creation has one winner; local directories have one owner.
- HTTP authentication, limits, serialization, and status codes match the documented contract.

Local tests use unique directories under `target/test-data` and clean them up. The process test launches the built binary on loopback, writes through HTTP, uses `Child::kill`, removes only its disposable cache, and checks recovery over HTTP. This proves the exercised acknowledged-write path survives abrupt process termination; it is not an exhaustive distributed-systems fault campaign.

The S3 test is ignored by default and must be invoked explicitly with an existing bucket. See [MinIO](minio.md).

## Implementation rules

Keep one package until a module needs an independent boundary. Prefer SlateDB and standard-library facilities to duplicate storage machinery. Do not introduce a second WAL or hand-edit SlateDB objects. Keep vectors validated at the write boundary, and ensure new query paths honor tombstones and the same snapshot as their source documents.

Before adding ANN, retain exact search as an oracle and measure recall, cold/warm latency, bytes fetched, and object-store requests. Before adding read replicas, specify how they catch up to acknowledged writes. A cached or asynchronously refreshed reader is not automatically a strongly consistent query node.

## Upgrades

SlateDB is pinned to an exact version. Read its release notes, inspect API/format changes, then test recovery on a copied database before updating the pin and lockfile. Do not claim a storage migration is supported solely because the code compiles.

Changes to keys, document serialization, or namespace configuration require an explicit format decision. `meta/format` rejects unrecognized formats. There is no migration command today.
