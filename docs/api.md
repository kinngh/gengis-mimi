# HTTP API

JSON requests use `Content-Type: application/json`. IDs contain 1–128 ASCII letters, digits, underscores, or hyphens. Set `GENGIS_MIMI_API_TOKEN` to require a bearer token; scoped namespace tokens are described in [operations](operations.md). Health probes remain public.

## Routes

| Method | Path | Operation |
| --- | --- | --- |
| GET | `/healthz` | Process health |
| GET | `/readyz` | Database/owner readiness |
| GET | `/v1/namespaces` | Paginated namespace listing |
| PUT / GET / DELETE | `/v1/namespaces/{name}` | Create, inspect, delete namespace |
| POST | `/v1/namespaces/{name}/write` | Atomic batch |
| GET | `/v1/namespaces/{name}/documents` | Paginated documents |
| GET / DELETE | `/v1/namespaces/{name}/documents/{id}` | Retrieve/delete document |
| POST | `/v1/namespaces/{name}/query` | Vector search |
| POST | `/v1/namespaces/{name}/search` | BM25 text search |
| GET | `/v1/namespaces/{name}/stats` | Revision, document/byte count, pending IDs |
| GET / POST | `/v1/namespaces/{name}/index` | Inspect / rebuild search indexes |
| GET | `/v1/metrics` | Prometheus text, on a worker or standalone server |
| GET | `/v1/export` | Consistent binary backup stream, on a worker or standalone server |

The gateway supports namespace routes and merged namespace listing. Connect directly to the active worker for metrics and backups. Cluster readiness requires all shards to have reachable owners.

## Namespaces and documents

```json
{"dimensions":3,"metric":"cosine","text_fields":["title","body"]}
```

`dimensions` may be omitted/null for document-only namespaces. Metrics are `cosine` (default), `dot`, and `squared_euclidean`. Dimensions are 1..4096. Text fields must be unique ID-shaped top-level attribute names, at most 16. Repeating creation with identical configuration succeeds; changing a schema returns 409. Namespace deletion is idempotent, hides the namespace durably, and removes its primary/derived keys; interrupted cleanup resumes before serving after restart.

```json
{
  "upsert":[{"id":"article-1","vector":[1,0,0],"attributes":{"title":"Rust storage","year":2026}}],
  "delete":["old-article"]
}
```

An upsert replaces the entire document. Omitting a vector removes a previous vector. Documents may have arbitrary JSON attribute values, with at most 64 attributes. Vectors require matching dimensions and finite f32 components; cosine rejects zero vectors. A document ID can occur only once in a batch, including across upserts/deletes. Deleting a missing document succeeds.

The complete request is validated before one atomic commit. Success returns:

```json
{"upserted":1,"deleted":1,"sequence":42,"revision":7}
```

`sequence` identifies the SlateDB write; `revision` is the namespace's indexing counter. A timeout or connection loss leaves the outcome uncertain. There is no exactly-once retry ledger or conditional document update; blindly retrying an old upsert can overwrite a later write.

## Vector queries

```json
{
  "vector":[1,0,0],
  "top_k":10,
  "mode":"auto",
  "probes":4,
  "filter":{"eq":{"category":"engineering"},"range":{"year":{"gte":2025,"lte":2026}}}
}
```

`mode` is `auto`, `ann`, or `exact`. Auto and ANN use a published index when present and fall back to exact scanning before the first generation exists. Exact always scans primary data. `probes` (1..1024, default 4) is the maximum number of centroid clusters searched. More probes generally improve recall and cost more. A selective indexed filter can choose exact scoring over matching IDs instead.

```json
{
  "matches":[{"id":"article-1","score":1.0,"attributes":{"title":"Rust storage","year":2026}}],
  "scanned_documents":12,
  "plan":"centroid_ann",
  "revision":7,
  "indexed_revision":6
}
```

Scores are descending: cosine similarity, dot product, or **negative** squared Euclidean distance. Equal scores sort by ID ascending. Results contain attributes but omit vectors; GET the document for its vector. `scanned_documents` reports visited candidates (including suppressed stale candidates/changed IDs), not S3 requests. Exact scans count primary documents, including those without a vector. `revision` identifies the query snapshot's namespace state.

Plans are `exact_scan`, `centroid_ann`, and `filtered_exact`. Fresh writes participate even when `indexed_revision` is behind. ANN can miss neighbors outside its candidates; use exact mode to evaluate recall.

## Filters

Equality uses exact JSON value equality on top-level attributes; missing differs from explicit null. Inclusive numeric ranges use `gte`, `lte`, or both. All predicates are ANDed. The same filters work with vector and text search. There are at most 64 predicates, finite numeric bounds must be ordered, and numeric comparisons use f64 (integers above 2^53 may lose precision). OR, NOT, nested paths, and substring operators are not supported.

## Text queries

```json
{"field":"title","text":"Rust storage","top_k":10,"filter":{"range":{"year":{"gte":2025}}}}
```

The field must be declared in `text_fields`. Queries contain at most 4096 bytes and 1..32 distinct tokens. Search ORs the lowercase alphanumeric tokens and adds their BM25 scores (`k1=1.2`, `b=0.75`). Attribute filters restrict results without changing corpus statistics. Return fields match vector queries; plans are `bm25_index` or `bm25_scan`. There are no stemming, phrase, proximity, or hybrid ranking operators. See [indexing](indexing.md).

## Pagination, quotas, and errors

Listings accept `?limit=100&after=last-id`. Default limit is 100; maximum 1000. `next_cursor` is null on the final page. IDs sort lexicographically. Each page has its own snapshot; use export for a consistent multi-page-sized backup.

Limits: 8 MiB request body and batch, 256 KiB serialized document, 1000 write operations, 100 top-K hits. Configurable namespace quotas cover document count, primary encoded bytes, and pending changed IDs. Deleting a document can still consume a pending-ID slot until indexing runs.

Errors use `{"error":{"code":"...","message":"..."}}`. Common statuses are 400 invalid request, 401 invalid credentials, 403 insufficient scope, 404 missing resource, 409 schema/restore conflict, 413 oversized body, 422 malformed JSON/schema fields, 429 quota/build budget, 503 saturated or unavailable storage/owner, and 504 deadline exceeded. Storage errors and timeouts can have an unknown write outcome.

The deadline covers handler work until response headers, not the transfer of a streaming backup. A rebuild exceeding that deadline continues in the background; inspect its index revision before requesting another rebuild.
