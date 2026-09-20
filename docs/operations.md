# Configuration and operations

Run `gengis-mimi --config <file> [serve]`. Without a file, GM uses local `./data` on `127.0.0.1:7878`. Unknown TOML fields are rejected. Relative paths resolve from the working directory.

## Settings

| Setting | Default | Meaning |
| --- | --- | --- |
| `server.bind` | `127.0.0.1:7878` | Listen address |
| `server.max_concurrent_queries` | 4 | Search/export slots, 1..64 |
| `server.request_timeout_ms` | 30000 | Handler deadline, 100..3600000 |
| `database.prefix` | `gengis-mimi/v1` | Stable object-store prefix |
| `database.wal_flush_ms` | 100 | WAL flush interval, 1..60000 |
| `database.l0_sst_bytes` | 16777216 | Memtable/L0 target, 64 KiB..256 MiB |
| `database.cache_dir` | absent | Optional disposable disk cache |
| `database.cache_bytes` | 536870912 | Disk-cache budget, minimum 4 MiB |
| `database.migrate` | false | Permit v1 migration; prefer the CLI command |
| `index.interval_ms` | 5000 | Rebuild dirty namespaces; zero disables automatic indexing |
| `index.clusters` | 32 | Centroid count, limited by vector count, 1..1024 |
| `index.iterations` | 4 | Lloyd training passes, 1..50 |
| `index.max_build_bytes` | 536870912 | Build budget; input reserves 8× headroom, output checked separately |
| `index.filter_scan_threshold` | 4096 | Maximum indexed matching IDs for exact filter-first scoring |
| `limits.max_documents` | 1000000 | Live documents per namespace |
| `limits.max_namespace_bytes` | 17179869184 | Encoded primary document/vector bytes per namespace |
| `limits.max_pending_documents` | 50000 | Distinct pending change records per namespace |

The 64-handler admission limit returns 503 on saturation. Queries use separate permits, retained by an outstanding CPU batch after cancellation. Streaming backups retain a query permit until completion/disconnect. Application commits remain serialized through durability after a caller disconnects. Deadlines limit waiting clients; they do not cancel already-admitted commits or rebuilds.

Quota counters persist in the same batch as document changes. They describe live primary data, not WAL/history/index physical bytes. Index backlog consumes the pending-ID quota and returns 429 when full. Restore preserves counters; migration reconstructs them.

Build/cache settings are not hard process memory limits. Documents, posting sets, clustering scratch space, concurrent queries, SST buffers, compaction, and allocator overhead also use memory. Use the benchmark, monitor RSS, and configure process/container limits with headroom.

## Authentication

`GENGIS_MIMI_API_TOKEN` gives administrative access. Tokens must be nonempty printable ASCII without whitespace. Without it, a standalone server's API is open to callers who can reach it. Cluster workers require an admin token.

Scoped tokens come from environment variables:

```toml
[[auth.grants]]
namespace = "articles"
token_env = "ARTICLES_READ_TOKEN"
write = false

[[auth.grants]]
namespace = "articles"
token_env = "ARTICLES_WRITE_TOKEN"
write = true
```

A read grant permits namespace inspection, document reads/scans, stats/index inspection, and vector/BM25 queries. A write grant also permits namespace creation/deletion, writes, and rebuilding its index. Global listing, metrics, and backup require the admin token. Grants require an admin token to exist. Reload tokens/configuration by restarting the process.

Tokens are bearer credentials. For network deployment, terminate TLS at your ingress and restrict worker endpoints to the gateway/operators. The gateway uses its configured upstream token to contact workers; client namespace permissions are checked before forwarding. This is static token authorization, not a tenant identity or key-management service.

## Metrics and health

`GET /v1/metrics` exposes SlateDB's object-store request/byte/error/latency metrics, cache counters, WAL/memtable activity, and compactor throughput in Prometheus text. The `gm_object_store_calls`, `gm_object_store_read_bytes`, and `gm_object_store_written_bytes` counters measure raw API calls and payload bytes below caches, excluding transport overhead/provider-internal retries. API requests have status-class counters and duration histograms; `gm_durable_sequence` reports the durable SlateDB sequence. HTTP timings cover admitted handler work to response headers. Inspect `/stats` and `/index` for namespace revision, pending count, and indexing lag.

`/healthz` proves the HTTP process responds. Standalone `/readyz` checks the database status and format marker; it can be served from cache and is not an object-store reachability probe. Worker readiness additionally checks current lease admission; gateway readiness checks every shard's active owner. A storage outage can still permit useful cached reads in standalone mode; writes cannot be acknowledged without durability.

## Storage, shutdown, and recovery

Use one standalone writer per database prefix. For multiple processes, follow [cluster mode](cluster.md). Local data roots and cache directories take exclusive locks. A cache identity binds it to its backend/prefix, and overlapping cache/durable directories are rejected.

Ctrl-C/SIGTERM drains HTTP requests, stops background indexing, and closes SlateDB. SIGKILL recovery replays durable storage; already-acknowledged writes do not depend on the cache. After a crash, restart with the same data configuration. A failed write may still have committed: inspect application state before retrying older operations.

Index errors retain the last published generation and pending changes. Reduce write load, inspect logs and memory/storage limits, then rebuild. Interrupted namespace deletion resumes on reopen. Interrupted migration/restore is deliberately unreadable; follow [the recovery procedures](backups.md).

S3 uses SlateDB's normal GC and retention policy. Local mode keeps old manifest/compaction metadata because conditional overwrite is unavailable, while eligible WAL/SST cleanup remains enabled. Never delete selected SlateDB objects manually. Completed obsolete index generations are retired through SlateDB tombstones and its snapshot-aware cleanup.
