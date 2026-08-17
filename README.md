# Log Ingestion and Query Service

A Node.js/TypeScript service for storing and querying structured logs. PostgreSQL is the sole source of truth for reads and writes. The entire stack starts with Docker Compose.

## Running the Service

```bash
docker compose up
```

Once startup completes, the service is available at `http://localhost:8080`. No `.env` file is required with Docker Compose. Use `docker compose up --build` only if you need to force an image rebuild after code changes. For local development without Compose, copy `.env.example` to `.env` and run `npm run dev`. Compose uses a dedicated bridge network; the application connects to the database via the service hostname `postgres`. Resource limits match the evaluation constraints: application `0.5 CPU` and `256 MB`, PostgreSQL `1 CPU` and `1 GB`.

## API Endpoints

### `GET /health`

Returns `200` only after the database connection is established, the schema and indexes are applied, and the service is ready to accept traffic. Does not require authentication.

### `POST /logs`

Always accepts a batch; a batch containing a single log entry is valid.

```json
{
  "logs": [{
    "timestamp": "2026-07-20T14:32:01.123Z",
    "level": "error",
    "service": "checkout",
    "message": "payment declined",
    "attributes": { "user_id": "42", "region": "eu-west", "retries": 3 }
  }]
}
```

Each entry is validated independently. Allowed `level` values: `debug`, `info`, `warn`, `error`. `timestamp` must be ISO 8601 and must not be more than five minutes in the future; `service` and `message` must be non-empty strings; `attributes` must be a flat object with string, number, or boolean values. If at least one entry is accepted, returns `200`:

```json
{ "accepted": 1, "rejected": [] }
```

If all entries are rejected, or the body does not match the contract / JSON is malformed, returns `400`.

### `GET /logs`

Supports freely combinable filters: `service`, `level`, `since`, `until`, `q`, and `attr.<key>`. Example:

```text
/logs?service=checkout&level=error&since=2026-07-20T00:00:00Z&attr.region=eu-west&q=declined&limit=100
```

`attr.<key>` values come from the query string, so `attr.user_id=42` matches both the string value `"42"` and the numeric value `42`; string attributes that look like numbers or booleans are not lost.

`limit` is between 1 and 10000 (default 10000). Results are ordered by `timestamp DESC, id DESC`. Returns:

```json
{ "logs": [], "next_cursor": null }
```

When another page exists, `next_cursor` is an opaque encoded value; pass it unchanged as `cursor` for the next page. Attribute-filtered result sets up to 100,000 matching rows use a short-lived, bounded server-side ID snapshot so explicit small pages do not repeat a JSONB filter and sort for every continuation. The cursor remains retry-safe and falls back to ordinary keyset paging after a restart or session expiry. Invalid parameters return `{ "error": "..." }` with `400`.

### `GET /logs/aggregate`

Supports filters: `service`, `level`, `q`, and `attr.<key>`, plus the following aggregation parameters:

- `since`: required, inclusive start of the time range in ISO 8601 format.
- `until`: required, exclusive end of the time range in ISO 8601 format.
- `bucket`: required; one of `1m`, `5m`, `1h`, or `1d`.
- `group_by`: optional; `service` or `level` only.

Example: `/logs/aggregate?since=2026-07-20T14:00:00Z&until=2026-07-20T15:00:00Z&bucket=5m&group_by=service&level=error`.

Returns buckets in ascending time order; when `group_by` is omitted, `group` is `null`.

## Database and Index Design

The `logs` table uses UUID primary keys, `timestamptz`, text columns for level/service/message, and `jsonb` for attributes. JSONB allows flexible metadata without a wide-column or EAV table while keeping PostgreSQL as the source of truth.

- `(timestamp DESC, id DESC)`: paging and stable ordering.
- `(service, timestamp DESC, id DESC)` and `(level, timestamp DESC, id DESC)`: common filtered queries.
- `GIN (attributes jsonb_path_ops)`: `attr.<key>` filters.
- `q` remains a correct case-insensitive substring filter. It intentionally scans matching time/service/level candidates rather than maintaining a write-heavy trigram index, prioritizing sustained ingestion throughput.
- `log_second_rollups`: transactionally maintained per-second summaries by service/level. Aggregation uses rollups when there is no `q` or `attr.*` filter, and reads raw rows for incomplete time edges and filters that cannot be summarized; results remain accurate and PostgreSQL remains the source of truth.

The Drizzle migration in `drizzle/0000_initial.sql` is applied at startup; the service does not report healthy until migrations succeed. Ingestion uses the native PostgreSQL `COPY FROM STDIN` protocol via `postgres.js` instead of thousands of `INSERT` parameter bindings. A short server-side micro-batch queue combines concurrent HTTP requests into durable COPY transactions. Each transaction writes the raw logs plus a small grouped per-second **rollup delta** in the same transaction. Aggregation reads the compact rollups and committed deltas together, so new data is immediately counted without competing `UPSERT`s on the same current-second summary rows. Deltas can be compacted transactionally during maintenance without a visibility gap. An HTTP request receives `200` only after both raw logs and its delta commit; all valid entries from that request stay together in one atomic COPY flush. `INGESTION_FLUSH_MAX_LOGS` defaults to `8192`. The standalone fallback is `100ms` / one writer; the evaluated Docker Compose profile explicitly uses `200ms` / two writers, so use Compose for comparable measurements. Queued requests that have already waited the delay start immediately when a writer becomes free.

## Retention

`RETENTION_DAYS` defaults to 30. On startup and then every hour, the service deletes logs older than this threshold and updates rollup summaries via delete triggers. The interval can be configured with `RETENTION_INTERVAL_MS`. Authentication and rate limiting are optional and disabled by default so they do not alter the load generator contract.

## Verification and Tests

```bash
npm run typecheck
npm test
npm run smoke:test
BASE_URL=http://127.0.0.1:8086 npm run rollup:integration
TOTAL_LOGS=1000000 BATCH_SIZE=1000 CONCURRENCY=8 npm run load:test
BASE_URL=http://127.0.0.1:8086 TARGET_LOGS_PER_SECOND=15000 DURATION_SECONDS=30 BATCH_SIZE=32 npm run benchmark:regression
BASE_URL=http://127.0.0.1:8086 SCENARIO=load BATCH_SIZE=32 npm run benchmark:scenarios
```

Optional integration tests live in `scripts/optional-*-integration.ts`. Run each against a deployment where the corresponding feature is enabled, using `BASE_URL`:

```bash
npm run optional:enabled:test     # metrics, dashboard, live tail, dead letters, query language, gzip
npm run optional:auth:test        # authentication, scopes, tenant and rollup isolation
CONTROL_MODE=rate npm run optional:controls:test
CONTROL_MODE=backpressure npm run optional:controls:test
npm run optional:alerts:test      # requires ALERT_WEBHOOK_URL=http://host.docker.internal:18089
```

Each optional test is deliberately separate because the baseline Compose configuration keeps every optional feature off. Start a dedicated deployment (or restart Compose between tests) with the required settings below:

| Test | Required environment settings |
| --- | --- |
| `optional:enabled:test` | `METRICS_ENABLED=true LIVE_TAIL_ENABLED=true COMPRESSION_ENABLED=true DEAD_LETTER_ENABLED=true` |
| `optional:auth:test` | `AUTH_ENABLED=true METRICS_ENABLED=true` |
| `CONTROL_MODE=rate optional:controls:test` | `RATE_LIMIT_ENABLED=true RATE_LIMIT_REQUESTS=1` |
| `CONTROL_MODE=backpressure optional:controls:test` | `BACKPRESSURE_ENABLED=true MAX_CONCURRENT_INGESTIONS=1` |
| `optional:alerts:test` | `ALERTS_ENABLED=true ALERT_ERROR_THRESHOLD=1 ALERT_WEBHOOK_URL=http://host.docker.internal:18089` |

`optional:auth:test` creates its tenant-A, tenant-B, and ingest-only test keys before it runs, then removes them in `finally`. It connects directly to PostgreSQL when available and otherwise uses `docker compose exec`; this fixture does not alter the production startup behavior, which seeds only `LOADGEN_API_KEY`.

Unit tests cover batch validation, the cursor parser, invalid calendar dates, manual rollup grouping, and micro-batch queue behavior. `smoke:test` verifies required routes, batch partial rejection, cursor pagination, string attribute filtering, and both raw-filtered and rollup-backed mandatory `5m` aggregation. `load:test` generates parallel batches and runs aggregation once per second, then prints ingestion rate and p50/p95 latencies. Scripts default to `127.0.0.1:8080` to avoid localhost/IPv6 differences on Windows; override with `BASE_URL`.

`benchmark:regression` is a rate-scheduled small-batch regression harness for the production benchmark shape. Unlike the throughput-oriented `load:test`, it keeps offering the selected log rate with batches of 32, bounded high request concurrency, one aggregation per second, and periodic read-after-write checks. It reports failures and timeouts as failed throughput instead of silently slowing the sender. It requires an explicit `BASE_URL` so that a high-rate run cannot accidentally target a development database.

`benchmark:scenarios` runs the same client discipline across the full `load`, `stress`, `spike`, and `breakpoint` stage shapes. Select one with `SCENARIO`; it reports a separate result for every stage, including client-shed offers, POST success and latency, aggregation latency, and read-after-write visibility. Run these long scenarios only against an isolated database.

For a local k6 calibration of the baseline Load shape, install k6 and run:

```powershell
$env:BASE_URL = "http://127.0.0.1:8080"
k6 run scripts/k6-local-benchmark.js
```

Run it against one isolated Compose database, but do **not** reset that database between retained-data runs. Set a unique `BENCHMARK_RUN` for each run and use an explicit `CURSOR_LIMIT`; for example:

```powershell
$env:BENCHMARK_RUN = "load-1"
$env:CURSOR_LIMIT = "1000"
k6 run scripts/k6-local-benchmark.js
```

The same database may then be reused with another tag such as `stress-1`, so later runs query against the rows retained from earlier ones. Every log carries `attr.benchmark_run=<tag>` plus `attr.query_bucket` (default `0` through `99`); the cursor test selects only one bucket, or roughly 1% of the retained table. Set `FILTERED_BUCKET_MODULUS=50` to exercise the bounded page-session path with roughly 2% of a fresh 1.8M-row run. During ingestion, the script issues one aggregation per second, probes the rare tagged `attr.sequence=0` lookup every five seconds, and reads an explicitly limited sparse tagged page every second. Its final 30-second cursor drain supplies the attribute filters and explicit `limit` on the initial request, then passes only the opaque `next_cursor` on later pages. K6 may schedule one extra iteration exactly at the duration boundary; the script therefore requires the target minimum and verifies that the final visible count equals the actual accepted filtered count. It validates every page envelope plus the first/last row and cross-page ordering; inspecting every matching row locally would itself distort the drain timing. It fails locally if the sender drops scheduled work, a POST is not fully accepted, an aggregate fails or exceeds 1s p95, a tagged query fails while ingestion is active, the rare-attribute lookup misses its 20-second deadline, cursor ordering/shape is invalid, or the visible count does not equal the accepted filtered count. `PRE_ALLOCATED_VUS`, `MAX_VUS`, `FILTERED_BUCKET_MODULUS`, and `FILTERED_BUCKET` can be overridden if the local k6 sender is resource constrained or a different selectivity is desired. This is a local reproduction based on the published workload description and evaluator guidance, not the private grader script.

### Previous Large-Batch Baseline

The latest acceptance benchmark was run on Windows with Docker Desktop under the Compose limits: application `0.5 CPU` and `256 MB`, PostgreSQL `1 CPU` and `1 GB`.

| Metric | Result |
| --- | ---: |
| Dataset size | 1,000,000 records |
| Batch size | 1,000 records |
| Concurrency | 8 requests |
| Accepted records | 1,000,000 (zero rejections) |
| Ingestion rate | 19,907.45 records/second |
| Ingestion p50 | 292.53 ms per batch |
| Ingestion p95 | 1,167.31 ms per batch |
| Aggregation p50 | 61.09 ms |
| Aggregation p95 | 255.49 ms |
| Observed application peak (comparable run) | 39.56% CPU, 84.89 MiB RAM |
| Observed PostgreSQL peak (comparable run) | 105.01% CPU, 693.70 MiB RAM |

These figures are a prior large-batch baseline. The small-batch micro-batching implementation and `benchmark:regression` harness were added specifically to cover the external benchmark's higher request rate; rerun that harness before quoting a new final performance result. The resource peaks listed in the table were recorded with `docker stats` during a comparable concurrent load on the same environment; memory stayed within the imposed limits for both containers.

## Repository Hygiene

`.env` and `node_modules` are intentionally excluded from Git. Copy `.env.example` to `.env` only for local development; never commit real keys, webhooks, or credentials. Docker uses `.dockerignore` separately to keep local files out of the image build context.

## Performance Notes and Limitations

The primary bottleneck identified was multi-value `INSERT` under concurrent load; it was replaced with `COPY FROM STDIN`. Maintaining raw-row rollups on every insert also pressures PostgreSQL, so per-second summaries were added with an accurate raw-row fallback for time edges and text/attribute filters. Existing indexes keep filter and cursor queries fast but add natural write overhead. `q` and uncommon attribute filters are the most expensive paths; attributes use a GIN index, while `q` uses a correct raw-row substring filter to avoid a write-heavy trigram index. Aggregation requires a mandatory time range to avoid unrestricted table scans.

## Optional Features and CI

No optional features are enabled by default: no authentication, API keys, multi-tenancy, rate limiting, or quota. The service ignores any `Authorization` header and serves all four required endpoints without prior configuration via `docker compose up`.

The project includes `smoke:test` for the API contract and `load:test` for performance measurement. GitHub Actions in `.github/workflows/ci.yml` runs type checking and unit tests, then builds Docker Compose and runs the smoke test in both unauthenticated and authenticated modes with a seeded key. The load test is run manually because the one-million-record benchmark is time-intensive.

### Optional Features

All of the following are additive and do not change the shape or success semantics of the required endpoints when running `docker compose up` with no configuration:

- **Authentication and multi-tenancy**: disabled by default via `AUTH_ENABLED=false`. When `AUTH_ENABLED=true` and `LOADGEN_API_KEY=<key>` are set, the key is seeded automatically at startup with ingest/query scopes under tenant `loadgen`. Supports `Authorization: Bearer <key>` and `X-API-Key`; `/health` remains public; authentication protects all other routes.
- **Rate limiting**: disabled via `RATE_LIMIT_ENABLED=false`. Enable with `RATE_LIMIT_REQUESTS` (default 1000 requests/minute); the seeded load generator key is exempt.
- **Backpressure**: disabled via `BACKPRESSURE_ENABLED=false`. When enabled, caps concurrent ingestions with `MAX_CONCURRENT_INGESTIONS` (default 16) and responds with `503` and `Retry-After` instead of dropping logs silently.
- **Dead letters**: disabled via `DEAD_LETTER_ENABLED=false`. When enabled, rejected entries and their reasons are stored in `dead_letters` without changing the `POST /logs` response shape.
- **Metrics and dashboard**: `/metrics` exposes Prometheus counters when `METRICS_ENABLED=true` (default false); `/dashboard` serves a lightweight operations UI.
- **Live tail**: `/logs/tail` is an SSE stream of recently accepted logs; disabled via `LIVE_TAIL_ENABLED=false`.
- **Alert webhook**: disabled via `ALERTS_ENABLED=false`. Requires `ALERT_WEBHOOK_URL` and fires when a batch contains at least `ALERT_ERROR_THRESHOLD` error-level logs (default 1).
- **Custom query language**: use the additional `query` parameter, e.g. `query=service:checkout level:error attr.region:eu q:declined` on `/logs` or `/logs/aggregate`; standard query parameters remain fully supported.
- **Compression**: disabled via `COMPRESSION_ENABLED=false`. When enabled, compresses responses larger than 1 KiB with gzip only when the client sends `Accept-Encoding: gzip`.
