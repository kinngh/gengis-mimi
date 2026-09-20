# Cluster deployment

Cluster mode separates a gateway from S3-backed database workers. It shards **between namespaces**: every document in a namespace belongs to one shard and one active writer. Several namespaces can share a shard. It does not split one namespace across machines.

## Topology

```text
                         gateway
                     /             \
            namespace shard a    namespace shard b
               active a1             active b1
               standby a2
                     \             /
                        S3 bucket
                   separate shard prefixes
```

The gateway uses stable rendezvous hashing of namespace names and shard IDs. Worker addresses do not affect placement. On startup it discovers each active shard's database identity and conditionally creates an immutable topology object. Subsequent starts must match the stored shard IDs and database identities. A changed shard list is rejected instead of silently moving namespace ownership.

Each shard has a separate SlateDB database, WAL, compactor, and cache budget. This gives independent writer ownership and storage scheduling. Quotas remain per namespace. There is no automatic resharding/data migration; changing the shard set requires an explicit export/move plan and a new gateway topology prefix. Never delete the topology record merely to bypass a mismatch.

## Start the example

Create the bucket and export S3 credentials as in [MinIO](minio.md). Example configs use loopback S3 port 9000 and bucket `gengis-mimi`. Edit them consistently for your environment. Run commands from the repository root.

On the workers, set the same private upstream credential:

```sh
export GENGIS_MIMI_API_TOKEN='replace-with-a-worker-token'
./target/release/gengis-mimi --config configs/cluster/worker-a.toml
# Other terminals with the same environment:
./target/release/gengis-mimi --config configs/cluster/worker-a-standby.toml
./target/release/gengis-mimi --config configs/cluster/worker-b.toml
```

On the gateway, use an admin token for clients and the worker credential upstream:

```sh
export GENGIS_MIMI_API_TOKEN='replace-with-a-gateway-admin-token'
export GENGIS_MIMI_CLUSTER_TOKEN='replace-with-a-worker-token'
./target/release/gengis-mimi --config configs/cluster/gateway.toml
```

Wait until one worker for each shard returns 200 from `/readyz`, then start the gateway. The example gateway listens on port 8780; worker ports are 8781, 8782, and 8783. Send namespace API operations to the gateway. A standby returns 503 for readiness and data operations while remaining healthy at `/healthz`.

Workers for one shard share the bucket, endpoint, region, and database prefix; they have unique node IDs and cache directories. Workers for different shards use different prefixes. Keep worker endpoints private. Gateways can have separate client token grants; the upstream token is never taken from a user-controlled URL. HTTP redirects are disabled.

## Ownership and failover

The lease object is `<database-prefix>/coordination/owner.json`. An absent object is created conditionally. Every renewal writes a fresh nonce with `If-Match` against the previous ETag. The lease carries a TTL, normally 9000 ms, bounded to 3000..300000 ms.

A standby must observe an **unchanged ETag for a full TTL using elapsed monotonic time** before attempting conditional replacement. The algorithm does not compare wall-clock timestamps between machines. Concurrent contenders cannot both replace the same ETag successfully. Once a contender wins, opening SlateDB fences the previous writer; only a completed open allows readiness and data requests. The lease continues renewing during WAL recovery.

The active worker's admission deadline is measured conservatively from the start of its successful renewal request. Expiration, a failed/uncertain renewal, or a closed/fenced SlateDB handle removes it from service. Indexing stops and the handle closes. A worker that was paused beyond its deadline cannot resume accepting requests under the old deadline. SlateDB fencing is the final protection for writes already in flight during takeover.

Renewals run roughly every TTL/3. Takeover latency includes a standby's full unchanged-ETag observation, poll alignment, WAL recovery, and gateway discovery. The gateway caches a discovered owner for up to one second, preserving cache affinity. Requests during transition may receive 503; this is not zero-downtime failover.

Gateway writes are forwarded **once**. A network error or deadline may occur after a commit; the gateway does not automatically replay the write to another node. Applications must resolve uncertain outcomes. Current read-after-write behavior comes from routing all reads to the active writer, whose queries pin durable application snapshots.

## Operations and boundaries

Namespace listing fans out across shard owners and merges ordered results. Each shard/page has its own snapshot; there is no global snapshot or cross-shard transaction. Query results within a namespace use one snapshot. Back up each shard directly at its active worker; the gateway's topology record should also be preserved with configuration.

The current implementation provides standby takeover and namespace-level distribution. It does not provide independent read replicas, online resharding, intra-namespace scatter/gather, a distributed search-index worker queue, automatic node provisioning, or replica-based read scaling. Index generation runs in the active worker's background task and CPU pool; SlateDB manages its own background compaction. These are explicit deployment limits rather than hidden assumptions about extra processes.

`tests/cluster.rs` starts two shards, one standby, and a gateway against an existing S3 backend. It checks placement, merged listing, read-only namespace permissions, standby non-readiness, recovery after killing the owner process, and fresh vector/BM25 results after takeover. Run it with `scripts/check-s3.sh`.
