# Search indexes and consistency

## Vector representation

Primary vectors and cluster vectors use `GMVB` version 1 blocks. The layout is five magic/version bytes, a little-endian u32 dimension count, a u32 row count, repeated u16 ID lengths / UTF-8 IDs / f32 components, then CRC32 over all preceding bytes. Reads validate version, length, dimensions, IDs, finite values, and checksum. The primary vector uses one row with the internal ID `v`; its document ID comes from the key.

A cluster block holds at most 128 vectors, reduced for high dimensions to target roughly 256 KiB. Attributes are separate. ANN scoring reads vector blocks and fetches attributes only for the final hits and pending changes. SlateDB may fetch a larger SST block or cache part than the logical value; the benchmark metrics reveal the actual I/O.

## Centroid ANN

The builder chooses deterministic initial centroids across ID order and runs configurable Lloyd iterations. Cosine vectors are normalized for training. Euclidean assignment groups vectors; query scoring ranks centroids using the namespace metric, reads the requested cluster blocks with up to eight concurrent reads, and exactly scores the candidates.

`probes` trades candidate volume for recall. Probing every cluster gives the same vector results as exact mode, including pending changes. Dot-product clustering is a baseline without a specialized maximum-inner-product transform. Recall must be measured on representative data for each metric.

This implementation rebuilds the whole namespace, with input and output budgets. It does not incrementally split/merge clusters, hierarchically cluster, quantize vectors, or use SIMD-specific kernels. The budget checks reserve headroom but are not a hard allocator/RSS limit. Namespace partitioning and configured memory limits bound the practical build size.

## Publication protocol

1. Capture a durable snapshot and its namespace revision `R`.
2. Read primary documents/vectors and construct a new generation `G`.
3. Persist a cleanup marker, then store `G` in bounded write batches, waiting for durability.
4. Under the application commit mutex, atomically publish `i/<namespace> = {G, R, ...}`.
5. Tombstone obsolete generation keys through SlateDB.
6. Remove change records whose current revision is at most `R`, updating pending counters atomically. Recheck each record under the commit mutex so a later update cannot be erased.
7. Delete the cleanup marker.

All index builds and namespace deletions share a maintenance mutex. Cancelling a rebuild request leaves the admitted build running. Indexing failure preserves the previous published index; pending changes continue to be searchable.

Each changed ID has one durable record, coalescing repeated changes. A query using generation `G` loads IDs changed after `R`, excludes every such ID from indexed candidates, and considers its current document from the same query snapshot. Missing primary documents represent deletes. This prevents obsolete vectors and text terms from reappearing during indexing lag.

If a crash occurs before publication, staged keys are unreachable. If it occurs afterward, the published generation is complete. The cleanup marker lets the indexer resume retirement even when no pending writes remain. With the automatic indexer disabled, an explicit rebuild also completes cleanup.

SlateDB snapshots protect old generation values after their keys are tombstoned. The application never directly deletes objects that an older query might still require.

## Metadata postings and planning

Every top-level attribute gets an equality posting list. Finite numeric values also get sortable f64 range keys, with zero canonicalized and sign-aware IEEE-754 ordering. Posting lists are split into 256-ID blocks. Equality uses SHA-256 keys over field names and canonical JSON values; exact JSON equality semantics still apply.

All filter predicates are ANDed. Queries intersect their posting sets. A matching set at or below `filter_scan_threshold` is scored exactly by ID. Larger sets constrain the candidates from probed clusters. Pending IDs are evaluated against their current attributes separately.

Materializing a broad filter's matching IDs uses memory proportional to its matches. There is no bitmap compression or statistics-based optimizer yet. Numeric range comparisons use f64; integers above 2^53 can lose precision.

## BM25

`text_fields` opt string attributes into indexing. Tokenization splits on non-alphanumeric Unicode characters and lowercases tokens; there are no stop words, stemming, phrase/proximity operators, or locale-specific analyzers.

Term postings carry document ID, frequency, and field length in blocks of 256. Per-field corpus statistics contain document count and total tokens. BM25 uses `k1 = 1.2`, `b = 0.75`, additive scoring across distinct query terms, and OR matching. Repeated query terms do not increase weight. Empty strings count as field-bearing documents with zero tokens; missing/non-string fields do not.

When an index lags, queries remove pending IDs from old term postings, subtract their indexed field lengths/document contributions, and add their current contributions. IDF is computed from the updated postings before applying attribute filters. This makes ranking agree with a rebuilt index for the same snapshot. Before the first index exists, BM25 scans current documents to construct equivalent statistics.

The text path reads postings for the requested terms; it does not implement Block-Max WAND, MAXSCORE, or hybrid vector/text score fusion. Very common terms can still touch much of the corpus.
