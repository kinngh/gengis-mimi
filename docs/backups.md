# Backups, restore, and migration

## Online backup

An admin can export one active database shard while writes continue:

```sh
export GENGIS_MIMI_API_TOKEN='your-admin-token'
./target/release/gengis-mimi backup \
  --url http://127.0.0.1:7878 --output ./backup.gmb
```

This calls `GET /v1/export`. Export pins one durable SlateDB snapshot, streams logical keys, and ends with a record count and SHA-256 checksum. The CLI downloads to a uniquely named partial file beside the output, validates the completed file, fsyncs it, and links it to the requested name without overwriting an existing file. Failed downloads remove their own partial file.

The binary format starts with `GMBAK` version 2 plus CR/LF. Records carry u32 key/value lengths followed by bytes. A u32-max sentinel introduces a u64 record count and 32-byte checksum. The checksum covers the header and all records. Truncation, extra bytes, reordered keys, unsupported format markers, and checksum mismatches are rejected.

Backups contain primary data, schemas, revisions, and derived indexes as of that snapshot. They may include unreachable staged keys, which normal index cleanup can reclaim. They do not contain credentials, local caches, or cluster coordination objects. Treat the backup as data with the same confidentiality requirements as its source.

A slow export pins historical data and occupies a query slot until completion/disconnect. Copy completed backups to a separate failure domain and establish retention appropriate for your application. GM provides export/restore primitives; it does not schedule backup retention or replicate backup files automatically.

## Restore

Create a standalone config pointing to a **new empty prefix** and a fresh cache directory:

```sh
./target/release/gengis-mimi --config configs/restore.toml \
  restore --input ./backup.gmb
```

The entire file is checked before any data mutation. Restore refuses a database containing keys beyond its format marker. During import, the format marker is `restoring`; only the final durable batch restores the readable v2 marker. If the process dies during import, the partial database stays unavailable. Start again with another empty prefix. Do not edit the marker to bypass this protection.

Verify document counts, representative exact/ANN/BM25 queries, and application data before switching clients. A cluster restore is performed per shard with workers stopped. Restore shard data into the chosen prefixes, start workers, then establish the gateway topology. Shard exports are individually consistent; their capture times are not a distributed transaction.

A practical restore drill is: export, restore into a fresh prefix, compare namespace stats and selected query results, reopen with an empty cache, and confirm deleted documents remain absent. `tests/backup.rs` exercises the core protocol, including concurrent updates and corrupt input.

## v1-to-v2 migration

Existing JSON-vector databases need the explicit offline migration command:

```sh
./target/release/gengis-mimi --config configs/local.toml migrate
```

Stop every writer/worker for that prefix first. Preserve an offline copy of the old object prefix or local data directory, along with its compatible binary/config/lockfile. The v2 online backup endpoint operates on v2 databases; use the old installation's offline-copy procedure before migration.

Migration marks the database as in progress, moves vectors into checksummed binary values, removes vectors from document JSON, reconstructs byte/count statistics, and creates pending index-change records. Only after completion does it publish the v2 marker. Rerunning `migrate` after interruption resumes safely: already-converted rows keep their existing binary vectors. The normal server rejects incomplete migration markers.

Namespace definitions and document IDs remain intact. Initial search uses exact scanning until the indexer builds a generation. The object prefix name does not change. This migrator covers the GM v1 document format; it is not a general SlateDB version-upgrade mechanism.
