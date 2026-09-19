# HTTP API

Default base URL: `http://127.0.0.1:7878`. Send JSON with `Content-Type: application/json`. If `GENGIS_MIMI_API_TOKEN` is set, send `Authorization: Bearer <token>` on `/v1` requests. Health endpoints remain public.

## Routes

| Method | Path | Result |
| --- | --- | --- |
| GET | `/healthz` | Process liveness, name, version |
| GET | `/readyz` | Checks the open database with a durable read |
| PUT | `/v1/namespaces/{name}` | Create a namespace, or confirm identical configuration |
| GET | `/v1/namespaces/{name}` | Namespace configuration |
| GET | `/v1/namespaces` | Paginated namespaces |
| POST | `/v1/namespaces/{name}/write` | Atomic upsert/delete batch |
| GET | `/v1/namespaces/{name}/documents/{id}` | Full document |
| DELETE | `/v1/namespaces/{name}/documents/{id}` | Durable, idempotent deletion |
| GET | `/v1/namespaces/{name}/documents` | Paginated full documents |
| POST | `/v1/namespaces/{name}/query` | Exact vector search |

Successful responses use HTTP 200. Namespace and document IDs must contain 1–128 ASCII letters, digits, underscores, or hyphens. They are case-sensitive. Unknown fields in request payloads are rejected.

## Create a namespace

```json
{"dimensions": 3, "metric": "cosine"}
```

`dimensions` is optional. Omit it or use `null` for a document-only namespace. When provided, it must be between 1 and 4096. `metric` defaults to `cosine`; alternatives are `dot` and `squared_euclidean`.

Response:

```json
{"name": "demo", "dimensions": 3, "metric": "cosine"}
```

Namespace configuration cannot be changed. Repeating this PUT with identical configuration succeeds; different configuration returns 409. Namespaces are not created implicitly by writes. Namespace deletion is not implemented.

## Atomic writes

```json
{
  "upsert": [
    {"id": "article_1", "vector": [1, 0, 0], "attributes": {"category": "guide", "year": 2026}}
  ],
  "delete": ["article_2"]
}
```

Both arrays default to empty, but the batch must have at least one operation. An ID may appear only once across both arrays. A validation failure rejects the whole batch.

An upsert **replaces the entire document**. Omitted attributes become an empty object; an omitted vector removes the old vector. A vector-enabled namespace accepts documents without vectors. A document-only namespace rejects vectors. Attributes may contain arbitrary JSON; filter fields address top-level attributes only.

All vector components must be finite numbers representable as `f32`, with exactly the namespace's dimensions. Cosine vectors must be nonzero. Dot and squared Euclidean accept zero vectors.

Response:

```json
{"upserted": 1, "deleted": 1, "sequence": 42}
```

Counts are submitted operations: deleting an absent ID still counts as one. The response is sent after durability. The sequence applies to the whole batch and is shared across namespaces. Failed or disconnected requests can still commit; see [retry semantics](architecture.md#write-and-recovery-contract).

## Read and paginate

A document GET returns the stored `id`, optional `vector`, and `attributes`. A missing namespace or document returns 404.

Both list endpoints accept `limit` (default 100, maximum 1000) and `after` (an exclusive ID cursor):

```text
GET /v1/namespaces/demo/documents?limit=2&after=article_1
```

```json
{
  "items": [
    {"id": "article_2", "attributes": {"title": "Example"}},
    {"id": "article_3", "attributes": {}}
  ],
  "next_cursor": "article_3"
}
```

Ordering is ascending ASCII ID order. `next_cursor` is `null` when no more rows exist in that page's snapshot. Use the returned cursor as the next request's `after`; do not increment it. Namespace lists use names in the same way. Each page is consistent internally, but writes between pages can change later results.

## Query

```json
{
  "vector": [1, 0, 0],
  "top_k": 10,
  "filter": {
    "eq": {"category": "guide"},
    "range": {"year": {"gte": 2024, "lte": 2026}}
  }
}
```

`top_k` defaults to 10 and must be 1–100. `filter` is optional. All equality and range predicates are ANDed and evaluated before ranking. There are at most 64 predicates total.

- `eq` uses exact JSON equality. Strings are case-sensitive. A missing field does not match `null`. JSON integers and floating-point values can compare differently; use a consistent representation in attributes and filters.
- `range` accepts inclusive `gte`, `lte`, or both. At least one bound is required; lower cannot exceed upper. Missing and nonnumeric fields do not match. Values and bounds are evaluated as `f64`; do not use numeric ranges for exact comparisons of integers beyond 2^53.
- Field names are literal top-level keys; dots do not address nested objects.
- No OR/NOT, text matching, attribute indexes, or SQL expressions exist yet.

Response:

```json
{
  "matches": [{"id": "article_1", "score": 1.0, "attributes": {"category": "guide", "year": 2026}}],
  "scanned_documents": 3
}
```

Results omit vectors; GET a document to retrieve one. `scanned_documents` counts current documents visited, including documents rejected by filters or lacking vectors. A query can return fewer than K hits. Empty namespaces return an empty array.

Scores always sort descending; ties sort by ascending ID:

| Metric | Score | Best |
| --- | --- | --- |
| `cosine` | Dot product divided by both vector norms, clamped to [-1, 1] | 1 |
| `dot` | Raw dot product | Larger values |
| `squared_euclidean` | Negative sum of squared component differences | 0 |

Search is exact over the stored `f32` values. Computation accumulates in `f64`. There is no approximate index or embedding generation.

## Limits and errors

| Limit | Value |
| --- | --- |
| JSON request body | 8 MiB |
| Serialized document | 256 KiB |
| Operations per write batch | 1000 |
| Concurrent API handlers | 64 |
| Concurrent searches | 4 by default; configurable |

Errors normally use:

```json
{"error": {"code": "invalid_request", "message": "vector must have 3 finite f32 components"}}
```

| Status | Meaning |
| --- | --- |
| 400 | Invalid IDs, dimensions, filters, batch, query parameters, or malformed JSON |
| 401 | Missing/incorrect bearer token |
| 404 | Missing namespace/document/route |
| 405 | Unsupported method |
| 409 | Existing namespace has different configuration |
| 413 | Request exceeds the body limit |
| 415 | JSON content type missing/incorrect |
| 422 | JSON has the wrong shape, types, or unknown fields |
| 500 | Internal error or unreadable stored data |
| 503 | Storage unavailable or admission limit reached |

Low-level malformed URL/path extraction errors can use Axum's own response format. Storage errors are logged server-side; backend details are not returned to clients. A readiness check may be served from memory/cache and does not prove a fresh roundtrip to S3.
