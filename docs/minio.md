# MinIO and S3

GM's S3 backend is compiled into the binary. Use an existing MinIO instance or AWS bucket; normal startup does not provision storage.

## Connect

1. Create a bucket such as `gengis-mimi`.
2. Grant credentials bucket listing and object read/write/delete permissions for the configured prefix, including multipart operations needed by your service.
3. Edit `configs/minio.toml` with the S3 API endpoint, bucket, and signing region. Use the API port (commonly 9000), not the console port.
4. Export credentials and start GM:

```sh
export AWS_ACCESS_KEY_ID='replace-with-your-access-key'
export AWS_SECRET_ACCESS_KEY='replace-with-your-secret-key'
export AWS_REGION='us-east-1'
cargo run --release --locked -- --config configs/minio.toml
```

The local example permits HTTP and uses path-style bucket addressing. Use HTTPS and your normal credential provider for remote deployment. An explicit endpoint in TOML overrides the client's endpoint environment settings. For AWS, use `configs/s3.toml` and normally omit the endpoint.

The backend must support conditional creates/replaces with correct ETag semantics, along with the consistency requirements of SlateDB. A proxy must preserve conditional headers. Ordinary GET/PUT compatibility alone does not establish those guarantees.

Changing storage configs does not transfer local data to S3. Use [backup and restore](backups.md) to move a v2 database.

## Run backend validation

```sh
export GENGIS_MIMI_TEST_BUCKET='gengis-mimi'
export GENGIS_MIMI_TEST_ENDPOINT='http://127.0.0.1:9000'
./scripts/check-s3.sh
```

For AWS, omit `GENGIS_MIMI_TEST_ENDPOINT` and provide the bucket, region, and credentials. The script runs:

- Durable writes, index publication, close/reopen, and old-writer fencing.
- Repeated updates with small SST thresholds; the test requires observed compaction progress.
- Offline GC with shortened retention under a unique test prefix, followed by another reopen/query verification.
- Two independent shards, a standby, and a gateway; namespace permissions and takeover after killing an owner process.

Tests create unique `gengis-mimi-tests/...` and `gm-cluster-tests/...` prefixes. They retain objects for inspection and never target application prefixes. The shortened GC retention is confined to the inactive test database; production uses SlateDB defaults. Remove test prefixes with your object-store tooling once all test processes have stopped.

Local tests also inject a stalled WAL store, cancel a waiting writer, and confirm no premature acknowledgment or partial snapshot occurs. The backend/cluster tests were exercised against a local MinIO server built from tag `RELEASE.2025-09-07T16-13-09Z`. This verifies the exercised paths, not every failure schedule or every S3-compatible implementation.

## Troubleshooting

| Symptom | Check |
| --- | --- |
| Connection refused | API address, port, instance availability |
| HTTP transport rejected | Intentional local endpoint needs `allow_http = true` |
| Signature/access error | Keys, signing region, system clock, bucket permissions |
| Missing bucket | Create the bucket before startup |
| Conditional-write failures | Proxy headers, backend support, competing writers |
| Cache identity mismatch | Give a different backend/prefix its own cache directory |
| Worker remains standby | Another owner is renewing; check its readiness |
| Gateway topology mismatch | Shard IDs or storage identity changed; use a migration plan |
| Server returns 503 after another starts | Its writer may have been fenced; use cluster mode for shared ownership |

Reference: [SlateDB](https://slatedb.io/) and [S3 conditional writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html).
