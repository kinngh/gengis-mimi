# Configuration and operations

Run from the project root, or use absolute paths in TOML. All settings are optional except the fields required by the selected storage backend. Unknown settings fail startup.

```sh
target/release/gengis-mimi --config configs/local.toml
target/release/gengis-mimi --help
target/release/gengis-mimi --version
```

## Settings

| Setting | Default | Meaning |
| --- | --- | --- |
| `server.bind` | `127.0.0.1:7878` | Numeric IP address and port |
| `server.max_concurrent_queries` | `4` | Exact searches at once, 1–64 |
| `storage.type` | `local` | `local` or `s3` |
| `storage.path` | `./data` in the default local config | Local durable object directory |
| `storage.bucket` | Required for S3 | Existing bucket |
| `storage.region` | `us-east-1` | S3 signing region |
| `storage.endpoint` | AWS endpoint, or client environment override | Optional S3-compatible API endpoint |
| `storage.allow_http` | `false` | Permit unencrypted object-store HTTP, e.g. local MinIO |
| `database.prefix` | `gengis-mimi/v1` | Database object prefix; components use the same character rules as IDs |
| `database.wal_flush_ms` | `100` | WAL flush interval, 1–60000 ms |
| `database.cache_dir` | Disabled | Optional disposable raw-object disk cache |
| `database.cache_bytes` | `536870912` | Approximate disk-cache limit; at least 4 MiB |

Use `storage.path` only with `local`. Use `bucket`, `region`, `endpoint`, and `allow_http` only with `s3`. When a `[storage]` table is supplied for local storage, `path` is required.

The HTTP request body limit is 8 MiB and there are at most 64 admitted API handlers. Saturation returns 503. SlateDB applies write backpressure at 64 MiB of unflushed data; the target memtable/L0 file threshold is 16 MiB. These thresholds and the cache are not a hard process RSS limit: runtime buffers, indexes, in-flight requests, and compaction also consume memory.

A shorter WAL interval generally reduces write latency and increases object-store request frequency. Success still waits for storage I/O after the flush is initiated. There is no fixed latency or throughput guarantee.

## Credentials and authentication

For simple S3/MinIO credentials, supply `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` in the process environment. Temporary credentials can also use `AWS_SESSION_TOKEN`. The S3 client supports additional credential-provider mechanisms; verify your deployment against the pinned `object_store` version.

Configuration overrides the client environment for bucket, region, explicit endpoint, path-style addressing, HTTP allowance, and conditional PUT mode. When no endpoint is configured, the client can take it from its environment; that effective endpoint is included in cache ownership metadata.

Set `GENGIS_MIMI_API_TOKEN` to require a bearer token on `/v1`. It must be nonempty printable ASCII without spaces. The server reads it at startup. Configuration files contain no token or S3 secrets, and the program does not load `.env` automatically.

```sh
# With GENGIS_MIMI_API_TOKEN already set in your environment:
curl --fail-with-body http://127.0.0.1:7878/v1/namespaces \
  -H "Authorization: Bearer $GENGIS_MIMI_API_TOKEN"
```

This is a single shared token, with access to every namespace. There are no per-namespace permissions or built-in TLS. The default loopback binding is suitable for local development. Put TLS and appropriate access controls in front of a remotely reachable deployment.

## Ownership and caching

Run one application server per SlateDB database prefix. Starting a second writer against an S3 prefix is a takeover: SlateDB fences the previous writer. This version does not coordinate rolling replicas or distribute namespaces across nodes.

Local mode holds an exclusive OS lock for the whole data directory. Each configured disk cache also has an exclusive lock. A cache records the backend identity and database prefix; reusing it for a different database fails startup. Use a separate cache directory for every database. Cache and local durable directories must not overlap.

Stop the server before clearing its cache. Deleting only the configured cache is safe and makes subsequent reads cold. **Do not delete `storage.path`**: that is the database in local mode. Do not point external cleanup tools or bucket lifecycle expiration at active SlateDB objects.

Local mode retains old manifest and compactor-metadata files because conditional overwrite is unavailable. These files accumulate over time. Do not manually remove selected metadata objects; use local mode for development and testing, and S3/MinIO for exercising the full cleanup behavior. WAL/SST cleanup still runs when files are eligible. S3 mode uses SlateDB's default GC retention periods.

## Shutdown, failure, and recovery

Ctrl-C and SIGTERM stop new serving work, drain HTTP requests, and close SlateDB. The server has no application-level shutdown deadline; an operator may forcibly stop it if remote I/O is stuck. A forced stop can leave unacknowledged requests with unknown outcomes, but successful writes must survive reopening.

Reopen with the same bucket/path and prefix. SlateDB recovers before requests are served. An OS lock file remaining after a crash is normal; it should no longer be locked. Never delete a live lock file to force a second local writer.

`/healthz` checks process liveness. `/readyz` reads through the open database and fails if it is closed/fenced, but cached reads can succeed during a temporary object-store outage. It is not an S3 connectivity probe.

Set `RUST_LOG` to control logs:

```sh
RUST_LOG=gengis_mimi=info,slatedb=info target/release/gengis-mimi --config configs/local.toml
```

Request logs include method, status, and elapsed time, without bodies or authorization headers. Metrics export and distributed tracing are future work.

## Backups and upgrades

There is no live-backup API yet. A conservative offline backup procedure is:

1. Stop the only server and let it close successfully.
2. Copy the complete durable directory or database object prefix while no writer/compactor is running.
3. Restore into a separate directory or prefix, using an empty cache.
4. Reopen with the same application version and verify counts and sample documents.

A live recursive copy can mix storage generations and miss required files. Future live backups should use SlateDB checkpoints and their retention rules. Bucket versioning alone is not an application restore procedure.

Keep the binary version, Cargo.lock, and config alongside backups, with credentials stored separately. Before dependency or format upgrades, test against a restored copy. v0.1 has no automatic migration mechanism. Paginated document scans are useful for application exports while writes are paused; pagination across active writes is not a consistent backup.
