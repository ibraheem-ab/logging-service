import { and, asc, desc, eq, gte, ilike, lt, lte, or, sql, type SQL } from "drizzle-orm";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import { randomUUID } from "node:crypto";
import type postgres from "postgres";
import { encodeCursor, type CursorFilterContext, type DecodedCursor } from "../cursor.js";
import type { AggregateQuery, LogQuery } from "../validation.js";
import { client, db } from "./index.js";
import { logs, type NewLog } from "./schema.js";
import type { AttributeValue, LogAttributes } from "../types.js";

export type LogWrite = { entry: NewLog; tenantId: string };

type PreparedLogWrite = LogWrite & {
  id: string;
  timestamp: string;
  bucketStart: string;
};

export type RollupWrite = {
  tenant_id: string;
  bucket_start: string;
  service: string;
  level: string;
  count: number;
};

// Shared/exclusive coordination between rollup-delta writes, compaction, and
// retention. Normal ingestions take a shared lock only for their very short
// append-and-commit section; retention takes the exclusive form.
const ROLLUP_MAINTENANCE_LOCK = 78_123_457;
// PostgreSQL accepts at most 65,535 bound parameters per statement. A grouped
// row uses five values, so leave ample room below that limit for an oversized
// but otherwise valid HTTP request.
const ROLLUP_UPSERT_MAX_ROWS = 10_000;
const ROLLUP_DELTA_DRAIN_ROWS = 10_000;
type TransactionSql = postgres.TransactionSql;
type StoredRollupDelta = Omit<RollupWrite, "bucket_start" | "count"> & {
  bucket_start: Date | string;
  count: number | string;
};

export async function insertLogs(entries: NewLog[], tenantId = "default") {
  await insertLogWrites(entries.map((entry) => ({ entry, tenantId })));
}

export async function insertLogWrites(entries: LogWrite[]) {
  if (entries.length === 0) return;
  const prepared = entries.map(prepareLogWrite);
  const copyRows = prepared.map(toCopyRow).join("");
  const rollups = rollupWrites(prepared);

  await client.begin(async (transaction) => {
    const copyStream = await transaction`
      COPY logs (id, timestamp, level, service, message, attributes, tenant_id)
      FROM STDIN
    `.writable();
    await pipeline(Readable.from([copyRows]), copyStream);

    // Append a transactionally-consistent rollup delta instead of contending on
    // the hot current-second summary row. Aggregate queries include deltas
    // immediately; the idle-time compactor merges them into the main summary.
    await transaction`SELECT pg_advisory_xact_lock_shared(${ROLLUP_MAINTENANCE_LOCK}::bigint)`;
    await appendRollupDeltas(transaction, rollups);
  });
}

async function appendRollupDeltas(transaction: TransactionSql, rollups: RollupWrite[]) {
  for (let offset = 0; offset < rollups.length; offset += ROLLUP_UPSERT_MAX_ROWS) {
    const chunk = rollups.slice(offset, offset + ROLLUP_UPSERT_MAX_ROWS);
    await transaction`
      INSERT INTO log_second_rollup_deltas ${transaction(chunk, ["tenant_id", "bucket_start", "service", "level", "count"])}
    `;
  }
}

async function upsertRollups(transaction: TransactionSql, rollups: RollupWrite[]) {
  for (let offset = 0; offset < rollups.length; offset += ROLLUP_UPSERT_MAX_ROWS) {
    const chunk = rollups.slice(offset, offset + ROLLUP_UPSERT_MAX_ROWS);
    await transaction`
      INSERT INTO log_second_rollups ${transaction(chunk, ["tenant_id", "bucket_start", "service", "level", "count"])}
      ON CONFLICT (tenant_id, bucket_start, service, level)
      DO UPDATE SET count = log_second_rollups.count + EXCLUDED.count
    `;
  }
}

async function compactOneRollupDeltaChunk(transaction: TransactionSql) {
  const rows = await transaction<StoredRollupDelta[]>`
    DELETE FROM log_second_rollup_deltas
    WHERE id IN (
      SELECT id FROM log_second_rollup_deltas
      ORDER BY id
      LIMIT ${ROLLUP_DELTA_DRAIN_ROWS}
      FOR UPDATE SKIP LOCKED
    )
    RETURNING tenant_id, bucket_start, service, level, count
  `;
  if (rows.length === 0) return false;
  const rollups = mergeRollupWrites(rows.map((row) => ({
    ...row,
    bucket_start: row.bucket_start instanceof Date ? row.bucket_start.toISOString() : row.bucket_start,
    count: Number(row.count),
  })));
  await upsertRollups(transaction, rollups);
  return rows.length === ROLLUP_DELTA_DRAIN_ROWS;
}

/** Move all accumulated deltas into the compact rollup table without a visibility gap. */
export async function flushRollupDeltas() {
  let hasMore = true;
  while (hasMore) {
    hasMore = await client.begin(async (transaction) => {
      await transaction`SELECT pg_advisory_xact_lock_shared(${ROLLUP_MAINTENANCE_LOCK}::bigint)`;
      return compactOneRollupDeltaChunk(transaction);
    });
  }
}

function escapeCopyField(value: string) {
  return value
    .replace(/\\/g, "\\\\")
    .replace(/\t/g, "\\t")
    .replace(/\n/g, "\\n")
    .replace(/\r/g, "\\r");
}

function prepareLogWrite(write: LogWrite): PreparedLogWrite {
  const timestamp = write.entry.timestamp instanceof Date
    ? new Date(write.entry.timestamp)
    : new Date(write.entry.timestamp ?? Date.now());
  const timestampText = timestamp.toISOString();
  return {
    ...write,
    id: uuidV7(timestamp.getTime()),
    timestamp: timestampText,
    bucketStart: new Date(Math.floor(timestamp.getTime() / 1_000) * 1_000).toISOString(),
  };
}

function toCopyRow({ entry, tenantId, id, timestamp }: PreparedLogWrite) {
  return [
    id,
    timestamp,
    entry.level,
    entry.service,
    entry.message,
    JSON.stringify(entry.attributes ?? {}), tenantId,
  ].map((field) => escapeCopyField(String(field))).join("\t") + "\n";
}

// PostgreSQL's random UUID default is excellent for uniqueness but scatters
// inserts across every UUID B-tree page. UUIDv7 preserves the UUID API while
// making IDs generated for current logs append-friendly in the primary and
// timestamp/id indexes.
export function uuidV7(milliseconds: number) {
  const timestamp = Math.floor(milliseconds).toString(16).padStart(12, "0").slice(-12);
  const random = randomUUID().replaceAll("-", "");
  const variant = (8 | (Number.parseInt(random[3], 16) & 0x3)).toString(16);
  return `${timestamp.slice(0, 8)}-${timestamp.slice(8)}-7${random.slice(0, 3)}-${variant}${random.slice(4, 7)}-${random.slice(7, 19)}`;
}

function rollupWrites(entries: PreparedLogWrite[]): RollupWrite[] {
  return mergeRollupWrites(entries.map(({ entry, tenantId, bucketStart }) => ({
    tenant_id: tenantId,
    bucket_start: bucketStart,
    service: entry.service,
    level: entry.level,
    count: 1,
  })));
}

function mergeRollupWrites(entries: RollupWrite[]): RollupWrite[] {
  const grouped = new Map<string, RollupWrite>();
  for (const entry of entries) {
    const { tenant_id, bucket_start, service, level, count } = entry;
    const key = JSON.stringify([tenant_id, bucket_start, service, level]);
    const existing = grouped.get(key);
    if (existing) {
      existing.count += count;
    } else {
      grouped.set(key, { tenant_id, bucket_start, service, level, count });
    }
  }
  return [...grouped.values()].sort((left, right) =>
    left.tenant_id.localeCompare(right.tenant_id)
    || left.bucket_start.localeCompare(right.bucket_start)
    || left.service.localeCompare(right.service)
    || left.level.localeCompare(right.level),
  );
}

// Kept as a pure helper so the transaction's summary semantics can be tested
// without a live PostgreSQL instance.
export function summarizeRollupWrites(entries: LogWrite[]): RollupWrite[] {
  return rollupWrites(entries.map(prepareLogWrite));
}

function attributeValueFromQuery(value: string): AttributeValue {
  if (value === "true") return true;
  if (value === "false") return false;
  if (/^-?(?:0|[1-9]\d*)(?:\.\d+)?$/.test(value)) return Number(value);
  return value;
}

function attributeCondition(key: string, value: string): SQL {
 
  const expectedValues = new Set([
    JSON.stringify({ [key]: value }),
    JSON.stringify({ [key]: attributeValueFromQuery(value) }),
  ]);
  const conditions = [...expectedValues].map((expected) => sql`${logs.attributes} @> ${expected}::jsonb`);
  return conditions.length === 1 ? conditions[0] : or(...conditions)!;
}

function nonAttributeFilterConditions(params: Omit<LogQuery, "limit" | "cursor">, untilExclusive = false): SQL[] {
  const conditions: SQL[] = [];
  if (params.level) conditions.push(eq(logs.level, params.level));
  if (params.service) conditions.push(eq(logs.service, params.service));
  if (params.since) conditions.push(gte(logs.timestamp, params.since));
  if (params.until) conditions.push(untilExclusive ? lt(logs.timestamp, params.until) : lte(logs.timestamp, params.until));
  if (params.q) conditions.push(ilike(logs.message, `%${params.q}%`));
  return conditions;
}

function filterConditions(params: Omit<LogQuery, "limit" | "cursor">, untilExclusive = false): SQL[] {
  const conditions = nonAttributeFilterConditions(params, untilExclusive);
  for (const [key, value] of Object.entries(params.attributes)) {
    conditions.push(attributeCondition(key, value));
  }
  return conditions;
}

function cursorCondition(cursor: DecodedCursor): SQL {
  // A tuple comparison is semantically the same as the expanded OR below,
  // but PostgreSQL can turn it into an index seek on
  // (timestamp DESC, id DESC). The OR form instead scans and filters every
  // already-seen row on later pages, making a full cursor walk quadratic.
  return sql`(${logs.timestamp}, ${logs.id}) < (${cursor.timestamp.toISOString()}::timestamptz, ${cursor.id}::uuid)`;
}

// Stay comfortably below PostgreSQL's 65,535 bind-parameter limit while
// allowing a 1%-scale retained-data filter to paginate through candidates.
const ATTRIBUTE_FAST_PATH_CANDIDATES = 20_000;
const ATTRIBUTE_DENSITY_SAMPLE_ROWS = 64;
const ATTRIBUTE_DENSE_SAMPLE_MATCHES = 8;

function publicLogProjection() {
  // Tenant identity is an internal authorization boundary, not part of the
  // required public log shape. Keep this projection explicit so future schema
  // columns cannot accidentally leak through the API either.
  return {
    id: logs.id,
    timestamp: logs.timestamp,
    level: logs.level,
    service: logs.service,
    message: logs.message,
    attributes: logs.attributes,
  };
}

function pageResult<T extends { id: string; timestamp: Date }>(
  rows: T[],
  limit: number,
  attributeCandidateMode = false,
  filterContext?: CursorFilterContext,
) {
  const hasNextPage = rows.length > limit;
  const resultLogs = hasNextPage ? rows.slice(0, limit) : rows;
  const lastLog = resultLogs[resultLogs.length - 1];
  return {
    logs: resultLogs,
    nextCursor: hasNextPage && lastLog
      ? encodeCursor({
        ...lastLog,
        ...(attributeCandidateMode ? { attributeCandidateMode: true as const } : {}),
        ...(filterContext ? { filterContext } : {}),
        limit,
      })
      : null,
  };
}

function filterContextFromParams(params: LogQuery): CursorFilterContext | undefined {
  const context: CursorFilterContext = {
    ...(params.level ? { level: params.level } : {}),
    ...(params.service ? { service: params.service } : {}),
    ...(params.since ? { since: params.since.toISOString() } : {}),
    ...(params.until ? { until: params.until.toISOString() } : {}),
    ...(params.q !== undefined ? { q: params.q } : {}),
    ...(Object.keys(params.attributes).length > 0 ? { attributes: { ...params.attributes } } : {}),
  };
  return Object.keys(context).length > 0 ? context : undefined;
}

type MaterializedPublicLog = {
  id: string;
  timestamp: Date | string;
  level: string;
  service: string;
  message: string;
  attributes: LogAttributes;
};

// An ordered scan is efficient when an attribute is common, because the
// timestamp/id index can stream the first page. For a result set that was
// proven small, materialize the GIN-filtered rows first and sort that bounded
// set instead. This avoids constructing a many-thousand-parameter `IN (...)`
// query on every cursor page while preserving the same order and filters.
async function getMaterializedAttributePage(conditions: SQL[], limit: number) {
  const result = await db.execute(sql`
    WITH filtered AS MATERIALIZED (
      SELECT "logs"."id", "logs"."timestamp", "logs"."level", "logs"."service", "logs"."message", "logs"."attributes"
      FROM "logs"
      WHERE ${and(...conditions)}
    )
    SELECT id, timestamp, level, service, message, attributes
    FROM filtered
    ORDER BY timestamp DESC, id DESC
    LIMIT ${limit + 1}
  `);
  const rows = result as unknown as MaterializedPublicLog[];
  return rows.map((row) => ({
    ...row,
    timestamp: row.timestamp instanceof Date ? row.timestamp : new Date(row.timestamp),
  }));
}

// Count no more than the configured cap without sending up to 20,001 UUIDs
// through the application just to decide which read plan to use.
async function boundedAttributeCandidateCount(conditions: SQL[]) {
  const result = await db.execute(sql`
    SELECT count(*)::integer AS count
    FROM (
      SELECT 1
      FROM "logs"
      WHERE ${and(...conditions)}
      LIMIT ${ATTRIBUTE_FAST_PATH_CANDIDATES + 1}
    ) AS candidates
  `);
  const rows = result as unknown as Array<{ count: number | string }>;
  return Number(rows[0]?.count ?? 0);
}

function matchesAttributeFilters(attributes: LogAttributes, requested: Record<string, string>) {
  return Object.entries(requested).every(([key, value]) => {
    const actual = attributes[key];
    return actual === value || actual === attributeValueFromQuery(value);
  });
}

async function attributeFiltersLookSparse(
  params: LogQuery,
  tenantId: string,
  nonAttributeConditions: SQL[],
) {
  const conditions = [...nonAttributeConditions, eq(logs.tenantId, tenantId)];
  if (params.cursor) conditions.push(cursorCondition(params.cursor));
  const sample = await db.select({ attributes: logs.attributes }).from(logs)
    .where(and(...conditions))
    .orderBy(desc(logs.timestamp), desc(logs.id))
    .limit(ATTRIBUTE_DENSITY_SAMPLE_ROWS);
  const matches = sample.reduce(
    (count, row) => count + (matchesAttributeFilters(row.attributes, params.attributes) ? 1 : 0),
    0,
  );
  return matches < ATTRIBUTE_DENSE_SAMPLE_MATCHES;
}

export async function getLogs(params: LogQuery, tenantId = "default") {
  const filterContext = filterContextFromParams(params);
  const nonAttributeConditions = nonAttributeFilterConditions(params);
  const baseConditions = filterConditions(params);
  baseConditions.push(eq(logs.tenantId, tenantId));
  const conditions = [...baseConditions];
  if (params.cursor) conditions.push(cursorCondition(params.cursor));

  // A timestamp/id seek is best for dense attributes, while a rare attr.*
  // lookup can otherwise scan millions of ordered rows before finding a
  // match. Classify only the first page from a tiny ordered sample. A cursor
  // marked by a proven-small first page keeps using the GIN/materialize path;
  // broad filters retain the ordinary keyset seek and never pay repeated GIN
  // work.
  const hasAttributes = Object.keys(params.attributes).length > 0;
  const useCandidateMode = hasAttributes && (
    params.cursor?.attributeCandidateMode === true
    || (!params.cursor && await attributeFiltersLookSparse(params, tenantId, nonAttributeConditions))
  );
  if (useCandidateMode) {
    // The first page establishes that the full result set is bounded. Later
    // cursor pages inherit that proof through the opaque cursor, so they can
    // run a single materialized query instead of repeatedly fetching IDs just
    // to rediscover the same bound.
    if (params.cursor?.attributeCandidateMode !== true) {
      const candidateCount = await boundedAttributeCandidateCount(conditions);
      if (candidateCount > ATTRIBUTE_FAST_PATH_CANDIDATES) {
        const rows = await db.select(publicLogProjection()).from(logs)
          .where(conditions.length > 0 ? and(...conditions) : undefined)
          .orderBy(desc(logs.timestamp), desc(logs.id))
          .limit(params.limit + 1);
        return pageResult(rows, params.limit, false, filterContext);
      }
      if (candidateCount === 0) return { logs: [], nextCursor: null };
    }
    const rows = await getMaterializedAttributePage(conditions, params.limit);
    return pageResult(rows, params.limit, true, filterContext);
  }

  const rows = await db.select(publicLogProjection()).from(logs)
    .where(conditions.length > 0 ? and(...conditions) : undefined)
    .orderBy(desc(logs.timestamp), desc(logs.id))
    .limit(params.limit + 1);
  return pageResult(rows, params.limit, false, filterContext);
}

type AggregateRow = { start: string; group: string | null; count: number };

function bucketExpression(timestamp: typeof logs.timestamp | SQL, bucket: AggregateQuery["bucket"]) {
  return bucket === "5m"
    ? sql<string>`date_bin('5 minutes'::interval, ${timestamp}, '1970-01-01T00:00:00Z'::timestamptz)`
    : bucket === "1m"
      ? sql<string>`date_trunc('minute', ${timestamp})`
      : bucket === "1h"
        ? sql<string>`date_trunc('hour', ${timestamp})`
        : sql<string>`date_trunc('day', ${timestamp})`;
}

function groupExpression(groupBy: AggregateQuery["groupBy"], service: typeof logs.service | SQL, level: typeof logs.level | SQL) {
  return groupBy === null ? sql<string | null>`NULL::text`
    : groupBy === "service" ? service
      : level;
}

async function getRawAggregate(params: AggregateQuery, since = params.since, until = params.until, tenantId = "default"): Promise<AggregateRow[]> {
  const effectiveParams = { ...params, since, until };
  const bucket = bucketExpression(logs.timestamp, params.bucket);
  const group = groupExpression(params.groupBy, logs.service, logs.level);
  const conditions = filterConditions(effectiveParams, true);
  conditions.push(eq(logs.tenantId, tenantId));
  const rows = await db.select({ start: bucket, group, count: sql<number>`count(*)::integer` }).from(logs)
    .where(conditions.length > 0 ? and(...conditions) : undefined)
    .groupBy(bucket, group).orderBy(asc(bucket), asc(group));
  return rows.map((row) => ({ start: new Date(row.start).toISOString(), group: row.group as string | null, count: Number(row.count) }));
}

function rollupWhere(
  timestamp: SQL,
  service: SQL,
  level: SQL,
  tenant: SQL,
  params: AggregateQuery,
  since: Date,
  until: Date,
  tenantId: string,
) {
  const conditions: SQL[] = [
    sql`${tenant} = ${tenantId}`,
    sql`${timestamp} >= ${since.toISOString()}::timestamptz`,
    sql`${timestamp} < ${until.toISOString()}::timestamptz`,
  ];
  if (params.service) conditions.push(sql`${service} = ${params.service}`);
  if (params.level) conditions.push(sql`${level} = ${params.level}`);
  return and(...conditions)!;
}

async function getRollupAggregate(params: AggregateQuery, since: Date, until: Date, tenantId: string): Promise<AggregateRow[]> {
  const mainWhere = rollupWhere(sql`t.bucket_start`, sql`t.service`, sql`t.level`, sql`t.tenant_id`, params, since, until, tenantId);
  const deltaWhere = rollupWhere(sql`d.bucket_start`, sql`d.service`, sql`d.level`, sql`d.tenant_id`, params, since, until, tenantId);
  const rollupTimestamp = sql`r.bucket_start`;
  const rollupService = sql`r.service`;
  const rollupLevel = sql`r.level`;
  const bucket = bucketExpression(rollupTimestamp, params.bucket);
  const group = groupExpression(params.groupBy, rollupService, rollupLevel);
  const result = await db.execute(sql`
    WITH rollup_rows AS (
      SELECT t.bucket_start, t.service, t.level, t.count
      FROM log_second_rollups AS t
      WHERE ${mainWhere}
      UNION ALL
      SELECT d.bucket_start, d.service, d.level, d.count
      FROM log_second_rollup_deltas AS d
      WHERE ${deltaWhere}
    )
    SELECT ${bucket} AS start, ${group} AS "group", sum(r.count)::integer AS count
    FROM rollup_rows AS r
    GROUP BY ${bucket}, ${group}
    ORDER BY ${bucket}, ${group}
  `);
  const rows = result as unknown as Array<{ start: Date | string; group: string | null; count: number | string }>;
  return rows.map((row) => ({ start: new Date(row.start).toISOString(), group: row.group, count: Number(row.count) }));
}

function mergeAggregateRows(rows: AggregateRow[]) {
  const combined = new Map<string, AggregateRow>();
  for (const row of rows) {
    const key = `${row.start}\u0000${row.group ?? ""}`;
    const existing = combined.get(key);
    if (existing) existing.count += row.count;
    else combined.set(key, { ...row });
  }
  return [...combined.values()].sort((left, right) => left.start.localeCompare(right.start) || (left.group ?? "").localeCompare(right.group ?? ""));
}

export async function getAggregate(params: AggregateQuery, tenantId = "default") {
  if (params.q || Object.keys(params.attributes).length > 0) return getRawAggregate(params, params.since, params.until, tenantId);

  const since = params.since!;
  const until = params.until!;
  const rollupSince = new Date(Math.ceil(since.getTime() / 1_000) * 1_000);
  const rollupUntil = new Date(Math.floor(until.getTime() / 1_000) * 1_000);
  if (rollupSince >= rollupUntil) return getRawAggregate(params, params.since, params.until, tenantId);

  const rows: AggregateRow[] = [];
  if (since < rollupSince) rows.push(...await getRawAggregate(params, since, rollupSince, tenantId));
  rows.push(...await getRollupAggregate(params, rollupSince, rollupUntil, tenantId));
  if (rollupUntil < until) rows.push(...await getRawAggregate(params, rollupUntil, until, tenantId));
  return mergeAggregateRows(rows);
}

export async function deleteExpiredLogs(cutoff: Date) {
  return client.begin(async (transaction) => {
    // Make every delta visible in the compact rollups before the delete trigger
    // subtracts expired rows. New ingestions hold the shared version of this
    // advisory lock until their raw rows and deltas commit.
    await transaction`SELECT pg_advisory_xact_lock(${ROLLUP_MAINTENANCE_LOCK}::bigint)`;
    let hasMore = true;
    while (hasMore) hasMore = await compactOneRollupDeltaChunk(transaction);
    const deleted = await transaction`DELETE FROM logs WHERE timestamp < ${cutoff.toISOString()}::timestamptz`;
    return deleted.count;
  });
}
