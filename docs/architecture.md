# Architecture

GM has a durable document layer and derived search indexes, backed by one SlateDB database per shard. SlateDB owns the WAL, manifests, immutable SSTs, compaction, MVCC snapshots, raw-object caching, and object garbage collection. GM owns document schemas, search indexes, ranking, routing, and API semantics.

## A single shard

```text
HTTP API → validation / namespace quota → atomic SlateDB batch → durable WAL
                                              ↓
                                     documents + pending changes
                                              ↓
                               background index generation builder
                                              ↓
                               durable blocks → publish index pointer

Query → one durable snapshot → indexed candidates + pending changes → top K
```

Each application write holds a commit mutex through `await_durable()`. A cancelled HTTP request leaves that commit task running with the mutex held. Snapshot creation briefly acquires the same mutex, so a query reading several keys cannot mix values from a partially durable application commit. A request that began after a successful write sees that write; a request overlapping a write can observe the earlier snapshot.

Writes return both a SlateDB `sequence` and a namespace `revision`. The namespace revision increments once per accepted batch and is the search indexing watermark. These counters have different purposes and are not interchangeable.

## Logical keys, format 2

| Key | Value |
| --- | --- |
| `meta/format` | `gengis-mimi:2` or an offline operation marker |
| `n/<namespace>` | Namespace schema JSON |
| `s/<namespace>` | Revision, live document bytes/count, pending-change count |
| `d/<namespace>/<id>` | ID and attributes as JSON, without the vector |
| `v/<namespace>/<id>` | Checksummed binary vector |
| `c/<namespace>/<id>` | Latest pending change revision, including deletes |
| `i/<namespace>` | Published generation, indexing watermark, centroids, corpus statistics |
| `g/<namespace>/<generation>/...` | Immutable vector blocks and search postings |
| `j/<namespace>` | Interrupted index staging/cleanup marker |
| `z/<namespace>` | Namespace deletion intent |

These are SlateDB keys, not independently mutable S3 objects. SlateDB packs them into immutable SSTs. GM never edits or directly removes an SST.

The initial object prefix `gengis-mimi/v1` is retained for compatibility with existing configurations. That path is an identifier, not the data format version. `meta/format` governs the format.

## Source of truth and derived state

Attributes and vectors, pending changes, and quota counters commit in one batch. A full-document upsert replaces the vector and all attributes. An omitted vector removes an existing vector. Deletes remove primary data while retaining a change record until indexing covers it.

Index builds read a snapshot without blocking foreground writes for the duration of training. They write new generation keys, wait for every block to become durable, then publish one pointer. Queries use the pointer and primary data from their own snapshot. Newer changed IDs suppress stale index entries, and their current values participate in ranking.

[The indexing protocol](indexing.md) explains the publication and cleanup invariants. Exact mode bypasses derived indexes and provides a reference result for recall checks.

## Storage and cache

Local storage uses `LocalFileSystem` with `fsync` and an exclusive directory lock. It is durable data, not a cache. Local conditional overwrite is unavailable, so GM disables SlateDB's manifest and compaction-metadata GC while retaining WAL/SST GC. Historical metadata therefore accumulates locally.

S3 uses ETag conditional writes, path-style addressing, and the object-store credential provider. A successful write means remote durability, subject to the backend's own durability contract. Credentials are never included in TOML. Disk caches have exclusive locks and a storage-identity marker; a cache belonging to another bucket/prefix is rejected. Removing a closed server's cache is safe.

## Shards and reads

Standalone mode opens one writer. Cluster mode uses independently owned shard prefixes. The gateway places a whole namespace on a fixed shard and forwards reads to that shard's active writer. Each namespace query therefore obtains its snapshot at the owner, with no asynchronously refreshed reader replica involved.

A standby opens SlateDB only after winning the ownership CAS. The open fences the old writer before the standby starts serving. Worker HTTP admission also checks the lease deadline and SlateDB status. See [cluster deployment](cluster.md).

Atomicity is within a batch and namespace. There are no cross-shard transactions, and a paginated scan consists of separate snapshots. A [backup export](backups.md) pins one snapshot for its entire stream.
