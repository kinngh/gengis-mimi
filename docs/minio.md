# MinIO and S3

The S3 backend is compiled into the binary. No MinIO server is provisioned by Gengis Mimi. Start with the local filesystem backend until your instance is ready.

## Connect your MinIO instance

1. Create a bucket named `gengis-mimi` using your MinIO console or client.
2. Give the application credentials permission to list the bucket and read, write, and delete objects under its database prefix. Include the multipart permissions required by your deployment.
3. Edit `configs/minio.toml` to match the API endpoint, bucket, and signing region. The endpoint is the S3 API, usually port 9000, not the web console.
4. Supply credentials in the environment and start the server.

For a local development instance, the environment has this shape (replace the example values):

```sh
export AWS_ACCESS_KEY_ID='replace-with-your-access-key'
export AWS_SECRET_ACCESS_KEY='replace-with-your-secret-key'
cargo run --release --locked -- --config configs/minio.toml
```

The example enables `allow_http = true` for a local HTTP endpoint. Use HTTPS for remote deployments. Requests use path-style bucket addressing, which works with typical local MinIO configurations.

Use the same namespace/write/query commands from the [README](../README.md). Data goes under `gengis-mimi/v1` in the bucket; the optional cache goes under `./.cache/minio`. Local filesystem data is not automatically copied to MinIO when you change configurations.

## Verify the backend

The default tests use local storage and do not require S3. Once the bucket is ready, run the explicit integration test:

```sh
export GENGIS_MIMI_TEST_BUCKET='gengis-mimi'
export GENGIS_MIMI_TEST_ENDPOINT='http://127.0.0.1:9000'
export AWS_REGION='us-east-1'
cargo test --locked --test s3 -- --ignored --nocapture
```

The test writes a unique `gengis-mimi-tests/<timestamp>` prefix, closes the database, reopens it, and verifies retrieval and search. It retains those objects for inspection and prints the prefix; remove that test prefix through your object-store tooling only after the test finishes. It never deletes other prefixes.

This is an initial integration check, not a complete certification of an S3-compatible service. In particular, it does not exercise delayed metadata GC, every crash point, or concurrent writer takeover. The backend must implement the conditional-write and consistency semantics required by SlateDB.

## AWS S3

Use `configs/s3.toml`, replacing the bucket and region. Normally omit the custom endpoint. For the integration test, omit `GENGIS_MIMI_TEST_ENDPOINT` and provide the AWS bucket, region, and credentials.

Gengis Mimi configures ETag-based conditional PUT support for SlateDB's storage operations. S3 compatibility must include the required conditional create/replace semantics, not just ordinary GET and PUT. A reverse proxy must preserve conditional headers.

## Troubleshooting

| Symptom | Check |
| --- | --- |
| Connection refused | Instance running; API host/port reachable from the server |
| HTTP transport rejected | `allow_http = true` only for an intentional HTTP endpoint |
| Signature mismatch / access denied | Access key, secret, signing region, clock, permissions |
| Missing bucket | Create it first; application startup does not create buckets |
| Conditional write errors | MinIO version, proxy behavior, competing writer, object-store semantics |
| Cache ownership error | Use a new cache directory for a different endpoint/bucket/prefix |
| Previous server starts returning 503 | Another writer may have taken ownership of its SlateDB prefix |

If an explicit endpoint is in TOML it takes precedence over the client's endpoint environment settings. Credentials and identity details should never be committed to this repository.

References: [SlateDB S3 tutorial](https://slatedb.io/docs/tutorials/s3/) and [AWS conditional writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html).
