import { and, asc, desc, eq, gte, inArray, lt, lte, or, sql, type SQL } from "drizzle-orm";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import { randomUUID } from "node:crypto";
import type postgres from "postgres";
import { encodeCursor, type CursorFilterContext, type DecodedCursor } from "../cursor.js";
import type { AggregateQuery, LogQuery } from "../validation.js";
import { client, db } from "./index.js";
import { logs, type NewLog } from "./schema.js";
import type { AttributeValue, LogAttributes } from "../types.js";
import { QueryPageSessions } from "../services/query-page-sessions.js";

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
  // This runs on the application container's constrained half-core. Build the
  // COPY payload and its compact rollup groups in one pass instead of creating
  // three full-size intermediate arrays for every durable micro-batch.
  const copyRows = new Array<string>(entries.length);
  const rollupGroups = new Map<string, RollupWrite>();
  const timestampTexts = new Map<number, { timestamp: string; bucketStart: string }>();

  for (let index = 0; index < entries.length; index += 1) {
    const { entry, tenantId } = entries[index];
    const timestamp = entry.timestamp instanceof Date
      ? entry.timestamp
      : new Date(entry.timestamp ?? Date.now());
    const milliseconds = timestamp.getTime();
    let timestampText = timestampTexts.get(milliseconds);
    if (!timestampText) {
      const text = timestamp.toISOString();
      timestampText = { timestamp: text, bucketStart: `${text.slice(0, 19)}.000Z` };
      timestampTexts.set(milliseconds, timestampText);
    }
    const id = uuidV7(milliseconds);
    const attributes = JSON.stringify(entry.attributes ?? {});
    copyRows[index] = `${escapeCopyField(id)}\t${escapeCopyField(timestampText.timestamp)}\t${escapeCopyField(entry.level)}\t${escapeCopyField(entry.service)}\t${escapeCopyField(entry.message)}\t${escapeCopyField(attributes)}\t${escapeCopyField(tenantId)}\n`;

    const rollupKey = JSON.stringify([tenantId, timestampText.bucketStart, entry.service, entry.level]);
    const existing = rollupGroups.get(rollupKey);
    if (existing) {
      existing.count += 1;
    } else {
      rollupGroups.set(rollupKey, {
        tenant_id: tenantId,
        bucket_start: timestampText.bucketStart,
        service: entry.service,
        level: entry.level,
        count: 1,
      });
    }
  }

  const copyRowsText = copyRows.join("");
  const rollups = [...rollupGroups.values()].sort((left, right) =>
    left.tenant_id.localeCompare(right.tenant_id)
    || left.bucket_start.localeCompare(right.bucket_start)
    || left.service.localeCompare(right.service)
    || left.level.localeCompare(right.level),
  );

  await client.begin(async (transaction) => {
    const copyStream = await transaction`
      COPY logs (id, timestamp, level, service, message, attributes, tenant_id)
      FROM STDIN
    `.writable();
    await pipeline(Readable.from([copyRowsText]), copyStream);

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

function literalSubstringPattern(value: string) {
  // `ILIKE` treats %, _, and the escape character itself as pattern syntax.
  // The API's q parameter is a literal message substring, so protect those
  // characters before binding the pattern.
  return `%${value.replace(/[!%_]/g, "!$&")}%`;
}

function nonAttributeFilterConditions(params: Omit<LogQuery, "limit" | "cursor">, untilExclusive = false): SQL[] {
  const conditions: SQL[] = [];
  if (params.level) conditions.push(eq(logs.level, params.level));
  if (params.service) conditions.push(eq(logs.service, params.service));
  if (params.since) conditions.push(gte(logs.timestamp, params.since));
  if (params.until) conditions.push(untilExclusive ? lt(logs.timestamp, params.until) : lte(logs.timestamp, params.until));
  if (params.q) conditions.push(sql`${logs.message} ILIKE ${literalSubstringPattern(params.q)} ESCAPE '!'`);
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

// A small selective result can safely be re-materialized for each page. A
// larger, still-selective result needs a bounded in-memory ID snapshot so a
// `limit=100` cursor does not repeat a GIN filter/sort or timestamp scan on
// every page against a retained multi-million-row table.
const ATTRIBUTE_SESSION_CANDIDATES = 100_000;
const ATTRIBUTE_OVER_CAP_PLAN_TTL_MS = 30_000;
const attributePageSessions = new QueryPageSessions({
  maxSessions: 2,
  maxIdsPerSession: ATTRIBUTE_SESSION_CANDIDATES,
  // Two 1%-scale retained-data walks can coexist without retaining full log
  // payloads or approaching the 256 MB application memory limit.
  maxTotalIds: 160_000,
  ttlMs: 60_000,
});
const overCapAttributePlans = new Map<string, number>();

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
  attributePageSession?: { id: string; offset: number },
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
        ...(attributePageSession ? { attributePageSession } : {}),
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

// Kept only so cursors emitted by the earlier bounded-candidate implementation
// remain usable during a rolling deployment. New queries use a page session,
// avoiding a full JSONB materialization on every continuation page.
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

// Materialize no more than the session cap once. If fewer rows are returned,
// this is the complete matching set and is already in the exact page order.
// If the sentinel row exists, the query is too broad for an in-memory
// snapshot and the normal stateless keyset path remains the safe fallback.
async function getBoundedOrderedAttributeCandidateIds(conditions: SQL[]) {
  const result = await db.execute(sql`
    WITH candidates AS MATERIALIZED (
      SELECT "logs"."id", "logs"."timestamp"
      FROM "logs"
      WHERE ${and(...conditions)}
      LIMIT ${ATTRIBUTE_SESSION_CANDIDATES + 1}
    )
    SELECT id
    FROM candidates
    ORDER BY timestamp DESC, id DESC
  `);
  return (result as unknown as Array<{ id: string }>).map((row) => row.id);
}

function attributeQueryKey(params: LogQuery, tenantId: string) {
  return JSON.stringify({
    tenantId,
    level: params.level ?? null,
    service: params.service ?? null,
    since: params.since?.toISOString() ?? null,
    until: params.until?.toISOString() ?? null,
    q: params.q ?? null,
    attributes: Object.entries(params.attributes).sort(([left], [right]) => left.localeCompare(right)),
  });
}

function hasKnownOverCapAttributePlan(key: string) {
  const expiresAt = overCapAttributePlans.get(key);
  if (expiresAt === undefined) return false;
  if (expiresAt > Date.now()) return true;
  overCapAttributePlans.delete(key);
  return false;
}

function rememberOverCapAttributePlan(key: string) {
  // Bound this best-effort planner hint as well. It only avoids repeated
  // expensive classification of the same broad query; correctness never
  // depends on it.
  if (overCapAttributePlans.size >= 128) {
    const oldest = overCapAttributePlans.keys().next().value;
    if (oldest !== undefined) overCapAttributePlans.delete(oldest);
  }
  overCapAttributePlans.set(key, Date.now() + ATTRIBUTE_OVER_CAP_PLAN_TTL_MS);
}

type IndexedPublicLog = {
  row: {
    id: string;
    timestamp: Date;
    level: string;
    service: string;
    message: string;
    attributes: LogAttributes;
  };
  index: number;
};

async function getRowsForOrderedIds(ids: readonly string[], tenantId: string) {
  if (ids.length === 0) return [];
  const rows = await db.select(publicLogProjection()).from(logs)
    .where(and(eq(logs.tenantId, tenantId), inArray(logs.id, [...ids])));
  const byId = new Map(rows.map((row) => [row.id, row]));
  return ids.flatMap((id) => {
    const row = byId.get(id);
    return row ? [row] : [];
  });
}

async function getAttributeSessionPage(
  session: { id: string; ids: readonly string[] },
  offset: number,
  limit: number,
  tenantId: string,
  filterContext?: CursorFilterContext,
) {
  const collected: IndexedPublicLog[] = [];
  let nextIndex = offset;
  const targetRows = limit + 1;

  // Retention can delete a row after the snapshot is built. In that rare case
  // keep advancing until this page is filled rather than exposing an empty
  // page with a non-null cursor or accidentally ending the walk early.
  while (collected.length < targetRows && nextIndex < session.ids.length) {
    const chunkSize = Math.min(
      session.ids.length - nextIndex,
      Math.max(targetRows - collected.length, 128),
    );
    const ids = session.ids.slice(nextIndex, nextIndex + chunkSize);
    const rows = await getRowsForOrderedIds(ids, tenantId);
    const byId = new Map(rows.map((row) => [row.id, row]));
    for (let index = 0; index < ids.length; index += 1) {
      const row = byId.get(ids[index]);
      if (row) collected.push({ row, index: nextIndex + index });
      if (collected.length >= targetRows) break;
    }
    nextIndex += ids.length;
  }

  const visible = collected.slice(0, limit);
  const last = visible.at(-1);
  const hasNextPage = last !== undefined && (
    collected.length > limit || last.index < session.ids.length - 1
  );
  if (!hasNextPage) attributePageSessions.delete(session.id);
  return {
    logs: visible.map(({ row }) => row),
    nextCursor: hasNextPage && last
      ? encodeCursor({
        ...last.row,
        attributePageSession: { id: session.id, offset: last.index + 1 },
        ...(filterContext ? { filterContext } : {}),
        limit,
      })
      : null,
  };
}

export async function getLogs(params: LogQuery, tenantId = "default") {
  const filterContext = filterContextFromParams(params);
  const baseConditions = filterConditions(params);
  baseConditions.push(eq(logs.tenantId, tenantId));
  const conditions = [...baseConditions];
  if (params.cursor) conditions.push(cursorCondition(params.cursor));

  const hasAttributes = Object.keys(params.attributes).length > 0;
  const queryKey = hasAttributes ? attributeQueryKey(params, tenantId) : undefined;

  if (params.cursor?.attributePageSession && queryKey) {
    const session = attributePageSessions.get(
      params.cursor.attributePageSession.id,
      tenantId,
      queryKey,
    );
    if (session) {
      return getAttributeSessionPage(
        session,
        params.cursor.attributePageSession.offset,
        params.limit,
        tenantId,
        filterContext,
      );
    }
    // A session is intentionally short-lived. Falling back to the stateless
    // tuple cursor preserves a correct result after expiry or app restart.
  }

  if (hasAttributes && params.cursor?.attributeCandidateMode === true) {
    // Legacy cursors emitted before bounded page sessions remain usable.
    const rows = await getMaterializedAttributePage(conditions, params.limit);
    return pageResult(rows, params.limit, true, filterContext);
  }

  if (hasAttributes && !params.cursor && queryKey && !hasKnownOverCapAttributePlan(queryKey)) {
    const ids = await getBoundedOrderedAttributeCandidateIds(conditions);
    if (ids.length === 0) return { logs: [], nextCursor: null };
    if (ids.length <= params.limit) {
      return { logs: await getRowsForOrderedIds(ids, tenantId), nextCursor: null };
    }
    const session = ids.length <= ATTRIBUTE_SESSION_CANDIDATES
      ? attributePageSessions.create(tenantId, queryKey, ids)
      : undefined;
    if (session) return getAttributeSessionPage(session, 0, params.limit, tenantId, filterContext);

    // Only a sentinel row proves that this filter is too broad. A transient
    // session-capacity decision must not pin a genuinely selective query to
    // the slow stateless plan.
    if (ids.length > ATTRIBUTE_SESSION_CANDIDATES) rememberOverCapAttributePlan(queryKey);
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
