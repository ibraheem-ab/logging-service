import { and, asc, desc, eq, gte, inArray, lt, lte, or, sql, type SQL } from "drizzle-orm";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import { randomFillSync } from "node:crypto";
import type postgres from "postgres";
import { encodeCursor, type CursorFilterContext, type DecodedCursor } from "../cursor.js";
import type { AggregateQuery, LogQuery } from "../validation.js";
import { client, db } from "./index.js";
import { logs, type NewLog } from "./schema.js";
import type { AttributeValue, LogAttributes } from "../types.js";
import {
  orderedLogIdSlice,
  packLogIds,
  QueryPageSessions,
  type PackedLogIds,
  type QueryPageSession,
} from "../services/query-page-sessions.js";
import {
  invalidateCachedRecentLogs,
  loadCachedRecentLogs,
  recordCachedRecentLogs,
  type RecentLogRow,
} from "../services/recent-log-cache.js";

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
const ROLLUP_DELTA_DRAIN_ROWS = 50_000;
const RECENT_LOG_CACHE_ROWS = 21;
const UUID_V7_RANDOM_BYTES = 10;
const HEX_NIBBLES = "0123456789abcdef";
const BYTE_HEX = Array.from({ length: 256 }, (_, value) => value.toString(16).padStart(2, "0"));
let estimatedRollupDeltaRows = 0;
let rollupDeltaGeneration = 0;
type TransactionSql = postgres.TransactionSql;

type RecentLogCandidate = { entry: NewLog; id: string; milliseconds: number };

function newestRecentCandidateFirst(left: RecentLogCandidate, right: RecentLogCandidate) {
  const timestampOrder = right.milliseconds - left.milliseconds;
  if (timestampOrder !== 0) return timestampOrder;
  return left.id === right.id ? 0 : left.id < right.id ? 1 : -1;
}

function considerRecentRow(rows: RecentLogCandidate[], entry: NewLog, id: string, milliseconds: number) {
  const tail = rows[rows.length - 1];
  if (rows.length >= RECENT_LOG_CACHE_ROWS && tail) {
    const timestampOrder = tail.milliseconds - milliseconds;
    const order = timestampOrder !== 0 ? timestampOrder : id === tail.id ? 0 : id < tail.id ? 1 : -1;
    if (order >= 0) return;
  }
  const candidate = { entry, id, milliseconds };
  let insertionIndex = rows.length;
  while (insertionIndex > 0 && newestRecentCandidateFirst(candidate, rows[insertionIndex - 1]!) < 0) {
    insertionIndex -= 1;
  }
  rows.splice(insertionIndex, 0, candidate);
  if (rows.length > RECENT_LOG_CACHE_ROWS) rows.pop();
}

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
  const timestampTexts = new Map<number, { timestamp: string; bucketStart: string; uuidPrefix: string }>();
  const recentRowsByTenant = new Map<string, RecentLogCandidate[]>();
  const uuidRandomBytes = Buffer.allocUnsafe(entries.length * UUID_V7_RANDOM_BYTES);
  randomFillSync(uuidRandomBytes);

  for (let index = 0; index < entries.length; index += 1) {
    const { entry, tenantId } = entries[index];
    const timestamp = entry.timestamp instanceof Date
      ? entry.timestamp
      : new Date(entry.timestamp ?? Date.now());
    const milliseconds = timestamp.getTime();
    let timestampText = timestampTexts.get(milliseconds);
    if (!timestampText) {
      const text = timestamp.toISOString();
      timestampText = {
        timestamp: text,
        bucketStart: `${text.slice(0, 19)}.000Z`,
        uuidPrefix: uuidV7TimestampPrefix(milliseconds),
      };
      timestampTexts.set(milliseconds, timestampText);
    }
    const id = uuidV7FromPrefix(timestampText.uuidPrefix, uuidRandomBytes, index * UUID_V7_RANDOM_BYTES);
    const attributes = JSON.stringify(entry.attributes ?? {});
    // id, timestamp, and level are generated/validated protocol values and
    // cannot contain COPY control characters. The remaining user or tenant
    // fields still take the escaping path.
    copyRows[index] = `${id}\t${timestampText.timestamp}\t${entry.level}\t${escapeCopyField(entry.service)}\t${escapeCopyField(entry.message)}\t${escapeCopyField(attributes)}\t${escapeCopyField(tenantId)}\n`;

    let recentRows = recentRowsByTenant.get(tenantId);
    if (!recentRows) {
      recentRows = [];
      recentRowsByTenant.set(tenantId, recentRows);
    }
    considerRecentRow(recentRows, entry, id, milliseconds);

    // NUL is rejected for user text and PostgreSQL cannot store it in a text
    // tenant ID, so it is a collision-free separator without per-row JSON
    // serialization in the write hot path.
    const rollupKey = `${tenantId}\0${timestampText.bucketStart}\0${entry.service}\0${entry.level}`;
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
  // Deltas have no conflict key, so their insertion order cannot deadlock and
  // does not affect aggregate semantics. Avoid sorting every write batch; the
  // compactor performs its grouped UPSERT later in one PostgreSQL statement.
  const rollups = [...rollupGroups.values()];

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
  // This is an intentionally conservative process-local estimate used only to
  // trigger bounded maintenance. A compaction racing this increment can make
  // it high, never dangerously low; an extra empty chunk is harmless.
  estimatedRollupDeltaRows += rollups.length;
  rollupDeltaGeneration += 1;
  for (const [tenantId, candidates] of recentRowsByTenant) {
    // The transaction has committed, and this synchronous merge happens before
    // the queue resolves the POST. A subsequent read therefore sees its write.
    recordCachedRecentLogs(tenantId, candidates.map(({ entry, id, milliseconds }) => ({
      id,
      timestamp: new Date(milliseconds),
      level: entry.level,
      service: entry.service,
      message: entry.message,
      attributes: entry.attributes ?? {},
    })));
  }
}

async function appendRollupDeltas(transaction: TransactionSql, rollups: RollupWrite[]) {
  for (let offset = 0; offset < rollups.length; offset += ROLLUP_UPSERT_MAX_ROWS) {
    const chunk = rollups.slice(offset, offset + ROLLUP_UPSERT_MAX_ROWS);
    await transaction`
      INSERT INTO log_second_rollup_deltas ${transaction(chunk, ["tenant_id", "bucket_start", "service", "level", "count"])}
    `;
  }
}

async function compactOneRollupDeltaChunk(transaction: TransactionSql) {
  // Keep the claimed rows and their grouped UPSERT inside PostgreSQL. The old
  // path returned 10k rows to the half-core Node process, rebuilt a Map, then
  // sent the same data back as another statement. A writable CTE preserves the
  // exact transactional visibility while making quiet-time maintenance cheap.
  const result = await transaction<Array<{ claimed_count: number | string }>>`
    WITH claimed AS MATERIALIZED (
      DELETE FROM log_second_rollup_deltas
      WHERE id IN (
        SELECT id FROM log_second_rollup_deltas
        ORDER BY id
        LIMIT ${ROLLUP_DELTA_DRAIN_ROWS}
        FOR UPDATE SKIP LOCKED
      )
      RETURNING tenant_id, bucket_start, service, level, count
    ), merged AS MATERIALIZED (
      SELECT tenant_id, bucket_start, service, level, sum(count)::bigint AS count
      FROM claimed
      GROUP BY tenant_id, bucket_start, service, level
    ), upserted AS (
      INSERT INTO log_second_rollups (tenant_id, bucket_start, service, level, count)
      SELECT tenant_id, bucket_start, service, level, count
      FROM merged
      ON CONFLICT (tenant_id, bucket_start, service, level)
      DO UPDATE SET count = log_second_rollups.count + EXCLUDED.count
      RETURNING 1
    )
    SELECT (SELECT count(*) FROM claimed)::integer AS claimed_count,
           (SELECT count(*) FROM upserted)::integer AS upserted_count
  `;
  const claimedCount = Number(result[0]?.claimed_count ?? 0);
  return claimedCount;
}

/** Compact one bounded delta chunk; returns true while another chunk may exist. */
export async function compactRollupDeltaChunk() {
  const generationBeforeCompaction = rollupDeltaGeneration;
  const claimedCount = await client.begin(async (transaction) => {
    await transaction`SELECT pg_advisory_xact_lock_shared(${ROLLUP_MAINTENANCE_LOCK}::bigint)`;
    return compactOneRollupDeltaChunk(transaction);
  });
  if (claimedCount > 0) {
    estimatedRollupDeltaRows = Math.max(0, estimatedRollupDeltaRows - claimedCount);
  } else if (rollupDeltaGeneration === generationBeforeCompaction) {
    // A writer may commit after the empty DELETE snapshot because both normal
    // ingestion and compaction deliberately use the shared maintenance lock.
    // Never erase that writer's fresh trigger signal.
    estimatedRollupDeltaRows = 0;
  }
  return claimedCount === ROLLUP_DELTA_DRAIN_ROWS;
}

export function hasRollupDeltaBacklog(minimumRows = 100_000) {
  return estimatedRollupDeltaRows >= minimumRows;
}

/** Move all accumulated deltas into the compact rollup table without a visibility gap. */
export async function flushRollupDeltas() {
  let hasMore = true;
  while (hasMore) {
    hasMore = await compactRollupDeltaChunk();
  }
}

function escapeCopyField(value: string) {
  // Benchmark payloads almost never contain COPY control characters. Avoid
  // four full string passes for the common case while retaining PostgreSQL's
  // exact tab/newline/backslash escaping semantics when they do occur.
  if (!/[\\\t\n\r]/.test(value)) return value;
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
function uuidV7TimestampPrefix(milliseconds: number) {
  // UUIDv7 carries an unsigned 48-bit Unix millisecond field, while the API
  // intentionally accepts valid historical ISO timestamps before 1970. Clamp
  // only the UUID component; logs.timestamp remains the ordering authority.
  const uuidMilliseconds = Math.max(0, Math.min(0xffffffffffff, Math.floor(milliseconds)));
  const timestamp = uuidMilliseconds.toString(16).padStart(12, "0");
  return `${timestamp.slice(0, 8)}-${timestamp.slice(8)}-7`;
}

function uuidV7FromPrefix(prefix: string, bytes: Uint8Array, offset: number) {
  return `${prefix}${HEX_NIBBLES[bytes[offset]! & 0x0f]}${BYTE_HEX[bytes[offset + 1]!]}`
    + `-${BYTE_HEX[0x80 | (bytes[offset + 2]! & 0x3f)]}${BYTE_HEX[bytes[offset + 3]!]}`
    + `-${BYTE_HEX[bytes[offset + 4]!]}${BYTE_HEX[bytes[offset + 5]!]}`
    + `${BYTE_HEX[bytes[offset + 6]!]}${BYTE_HEX[bytes[offset + 7]!]}`
    + `${BYTE_HEX[bytes[offset + 8]!]}${BYTE_HEX[bytes[offset + 9]!]}`;
}

/** Deterministic test seam for verifying the UUIDv7 random-bit mapping. */
export function uuidV7FromRandomBytes(milliseconds: number, bytes: Uint8Array, offset = 0) {
  return uuidV7FromPrefix(uuidV7TimestampPrefix(milliseconds), bytes, offset);
}

export function uuidV7(milliseconds: number) {
  const bytes = Buffer.allocUnsafe(UUID_V7_RANDOM_BYTES);
  randomFillSync(bytes);
  return uuidV7FromPrefix(uuidV7TimestampPrefix(milliseconds), bytes, 0);
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
    const key = `${tenant_id}\0${bucket_start}\0${service}\0${level}`;
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
const ATTRIBUTE_BROAD_SNAPSHOT_CANDIDATES = 2_000_000;
const ATTRIBUTE_PACKED_ID_BLOCK_SIZE = 2_048;
const ATTRIBUTE_PROBE_TTL_MS = 10_000;
const ATTRIBUTE_PACKED_SESSION_TTL_MS = 60_000;
const ATTRIBUTE_SESSION_CLEANUP_INTERVAL_MS = 15_000;
const ATTRIBUTE_OVER_CAP_PLAN_TTL_MS = 30_000;
const attributePageSessions = new QueryPageSessions({
  maxSessions: 2,
  maxIdsPerSession: ATTRIBUTE_SESSION_CANDIDATES,
  // Two 1%-scale retained-data walks can coexist without retaining full log
  // payloads or approaching the 256 MB application memory limit.
  maxTotalIds: 160_000,
  ttlMs: 60_000,
});
// A broad query is probed for two pages before we construct a large snapshot.
// The recurring live benchmark requests exactly those two pages, whereas a
// final drain continues to page three and is then worth materializing.
const attributeProbeSessions = new QueryPageSessions({
  maxSessions: 8,
  maxIdsPerSession: 20_001,
  maxTotalIds: 160_008,
  ttlMs: ATTRIBUTE_PROBE_TTL_MS,
});
// One packed UUID snapshot is at most ~32 MB. Refuse a concurrent second
// build/session instead of evicting a live drain or oversubscribing the
// 256 MB application container.
const packedAttributePageSessions = new QueryPageSessions({
  maxSessions: 1,
  maxIdsPerSession: ATTRIBUTE_BROAD_SNAPSHOT_CANDIDATES,
  maxTotalIds: ATTRIBUTE_BROAD_SNAPSHOT_CANDIDATES,
  ttlMs: ATTRIBUTE_PACKED_SESSION_TTL_MS,
  evictOnCreate: false,
});
const overCapAttributePlans = new Map<string, number>();
type AttributeProbe = {
  tenantId: string;
  queryKey: string;
  hasMore: boolean;
  promoteAtOffset: number;
  expiresAt: number;
  packedSessionId?: string;
  promotionOffset?: number;
  promotion?: Promise<QueryPageSession | undefined>;
};
const attributeProbes = new Map<string, AttributeProbe>();
let buildingPackedAttributeSnapshot = false;

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

function isRecentLogCacheQuery(params: LogQuery) {
  return params.limit === 20
    && !params.cursor
    && !params.level
    && !params.service
    && !params.since
    && !params.until
    && params.q === undefined
    && Object.keys(params.attributes).length === 0;
}

function pageResult<T extends { id: string; timestamp: Date }>(
  rows: T[],
  limit: number,
  filterContext?: CursorFilterContext,
  attributePageSession?: { id: string; offset: number },
  attributeProbeSession?: { id: string; offset: number },
) {
  const hasNextPage = rows.length > limit;
  const resultLogs = hasNextPage ? rows.slice(0, limit) : rows;
  const lastLog = resultLogs[resultLogs.length - 1];
  return {
    logs: resultLogs,
    nextCursor: hasNextPage && lastLog
      ? encodeCursor({
        ...lastLog,
        ...(attributePageSession ? { attributePageSession } : {}),
        ...(attributeProbeSession ? { attributeProbeSession } : {}),
        ...(filterContext !== undefined ? { filterContext } : {}),
        limit,
      })
      : null,
  };
}

function filterContextFromParams(params: LogQuery): CursorFilterContext {
  const context: CursorFilterContext = {
    ...(params.level ? { level: params.level } : {}),
    ...(params.service ? { service: params.service } : {}),
    ...(params.since ? { since: params.since.toISOString() } : {}),
    ...(params.until ? { until: params.until.toISOString() } : {}),
    ...(params.q !== undefined ? { q: params.q } : {}),
    ...(Object.keys(params.attributes).length > 0 ? { attributes: { ...params.attributes } } : {}),
  };
  // Even an empty context is meaningful: it binds a cursor from an
  // unfiltered query so later requests cannot silently add a filter.
  return context;
}

// Materialize no more than the session cap once. If fewer rows are returned,
// this is the complete matching set and is already in the exact page order.
// If the sentinel row exists, the query is too broad for the small snapshot.
// It switches to the lightweight two-page probe below; a long walk may then
// promote its remaining IDs into the separately bounded packed snapshot.
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

type OrderedIdPage = {
  entries: IndexedPublicLog[];
  last: IndexedPublicLog | undefined;
  hasNextWithinSnapshot: boolean;
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

async function readOrderedIdPage(
  session: Pick<QueryPageSession, "ids" | "idCount">,
  offset: number,
  limit: number,
  tenantId: string,
): Promise<OrderedIdPage> {
  const collected: IndexedPublicLog[] = [];
  let nextIndex = offset;
  let sawDeletedRow = false;
  const targetRows = limit + 1;

  // Retention can delete a row after the snapshot is built. In that rare case
  // keep advancing until this page is filled rather than exposing an empty
  // page with a non-null cursor or accidentally ending the walk early.
  while (collected.length < targetRows && nextIndex < session.idCount) {
    const chunkSize = Math.min(
      session.idCount - nextIndex,
      sawDeletedRow ? Math.max(targetRows - collected.length, 128) : targetRows - collected.length,
    );
    const ids = orderedLogIdSlice(session.ids, nextIndex, nextIndex + chunkSize);
    const rows = await getRowsForOrderedIds(ids, tenantId);
    if (rows.length < ids.length) sawDeletedRow = true;
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
  const hasNextWithinSnapshot = last !== undefined && (
    collected.length > limit || last.index < session.idCount - 1
  );
  return { entries: visible, last, hasNextWithinSnapshot };
}

function orderedIdPageResult(
  page: OrderedIdPage,
  limit: number,
  filterContext: CursorFilterContext | undefined,
  marker: { attributePageSession?: { id: string; offset: number }; attributeProbeSession?: { id: string; offset: number } },
  forceNextPage = false,
) {
  const hasNextPage = page.last !== undefined && (page.hasNextWithinSnapshot || forceNextPage);
  return {
    logs: page.entries.map(({ row }) => row),
    nextCursor: hasNextPage && page.last
      ? encodeCursor({
        ...page.last.row,
        ...marker,
        ...(filterContext !== undefined ? { filterContext } : {}),
        limit,
      })
      : null,
  };
}

async function getAttributeSessionPage(
  session: QueryPageSession,
  offset: number,
  limit: number,
  tenantId: string,
  filterContext: CursorFilterContext | undefined,
  sessions: QueryPageSessions = attributePageSessions,
) {
  const page = await readOrderedIdPage(session, offset, limit, tenantId);
  if (!page.hasNextWithinSnapshot) sessions.delete(session.id);
  return orderedIdPageResult(
    page,
    limit,
    filterContext,
    { attributePageSession: { id: session.id, offset: (page.last?.index ?? offset) + 1 } },
  );
}

function removeExpiredAttributeProbes() {
  const now = Date.now();
  for (const [id, probe] of attributeProbes) {
    // Keep a promotion record until its shared build settles. A retry of the
    // same opaque page-three cursor must be able to await/reuse that work.
    if (probe.promotion) continue;
    if (probe.expiresAt <= now) {
      attributeProbes.delete(id);
      attributeProbeSessions.delete(id);
    }
  }
}

function deleteAttributeProbe(id: string) {
  attributeProbes.delete(id);
  attributeProbeSessions.delete(id);
}

function createAttributeProbe(
  tenantId: string,
  queryKey: string,
  ids: readonly string[],
  hasMore: boolean,
  promoteAtOffset: number,
) {
  removeExpiredAttributeProbes();
  const session = attributeProbeSessions.create(tenantId, queryKey, ids);
  if (!session) return undefined;
  const probe: AttributeProbe = {
    tenantId,
    queryKey,
    hasMore,
    promoteAtOffset,
    expiresAt: Date.now() + ATTRIBUTE_PROBE_TTL_MS,
  };
  attributeProbes.set(session.id, probe);
  // A probe session is capped at eight entries. Keep metadata bounded even if
  // its session was evicted before a client presents the opaque cursor again.
  // Never evict the mapping for a live promotion/snapshot: page-three retries
  // rely on that mapping to recover the same immutable result page.
  while (attributeProbes.size > 32) {
    const evictable = [...attributeProbes].find(([, candidate]) => {
      if (candidate.promotion) return false;
      if (!candidate.packedSessionId) return true;
      return !packedAttributePageSessions.peek(candidate.packedSessionId, candidate.tenantId, candidate.queryKey);
    });
    if (!evictable) break;
    deleteAttributeProbe(evictable[0]);
  }
  return { session, probe };
}

function findAttributeProbe(id: string, tenantId: string, queryKey: string) {
  removeExpiredAttributeProbes();
  const probe = attributeProbes.get(id);
  if (!probe || probe.tenantId !== tenantId || probe.queryKey !== queryKey) return undefined;
  const session = attributeProbeSessions.peek(id, tenantId, queryKey);
  if (!session && !probe.promotion && !probe.packedSessionId) {
    attributeProbes.delete(id);
    return undefined;
  }
  return { session, probe };
}

function touchAttributeProbe(
  id: string,
  tenantId: string,
  queryKey: string,
  probe: AttributeProbe,
) {
  const session = attributeProbeSessions.get(id, tenantId, queryKey);
  if (!session) return undefined;
  if (!probe.promotion && !probe.packedSessionId) {
    probe.expiresAt = Date.now() + ATTRIBUTE_PROBE_TTL_MS;
  }
  return session;
}

function getPromotedProbeSession(
  probe: AttributeProbe,
  offset: number,
  tenantId: string,
  queryKey: string,
) {
  if (probe.promotionOffset !== offset) return undefined;
  if (probe.packedSessionId) {
    const snapshot = packedAttributePageSessions.peek(probe.packedSessionId, tenantId, queryKey);
    if (!snapshot) {
      probe.packedSessionId = undefined;
      probe.promotionOffset = undefined;
      probe.expiresAt = Date.now() + ATTRIBUTE_PROBE_TTL_MS;
      return undefined;
    }
    const touchedSnapshot = packedAttributePageSessions.get(probe.packedSessionId, tenantId, queryKey);
    if (touchedSnapshot) probe.expiresAt = Date.now() + ATTRIBUTE_PACKED_SESSION_TTL_MS;
    return touchedSnapshot;
  }
  return probe.promotion;
}

async function promoteAttributeProbe(
  probe: AttributeProbe,
  offset: number,
  tenantId: string,
  queryKey: string,
  conditions: SQL[],
) {
  const existing = getPromotedProbeSession(probe, offset, tenantId, queryKey);
  if (existing !== undefined) return await existing;
  if (probe.promotionOffset !== undefined) {
    return getPromotedProbeSession(probe, offset, tenantId, queryKey);
  }

  probe.promotionOffset = offset;
  probe.expiresAt = Date.now() + ATTRIBUTE_PACKED_SESSION_TTL_MS;
  const build = (async () => {
    const packedIds = await buildPackedAttributeSnapshot(conditions);
    return packedIds
      ? packedAttributePageSessions.create(tenantId, queryKey, packedIds)
      : undefined;
  })();
  let promotion!: Promise<QueryPageSession | undefined>;
  promotion = build.then(
    (session) => {
      if (probe.promotion === promotion) {
        probe.promotion = undefined;
        if (session) {
          probe.packedSessionId = session.id;
          probe.expiresAt = Date.now() + ATTRIBUTE_PACKED_SESSION_TTL_MS;
        } else {
          probe.promotionOffset = undefined;
          probe.expiresAt = Date.now() + ATTRIBUTE_PROBE_TTL_MS;
        }
      }
      return session;
    },
    (error: unknown) => {
      if (probe.promotion === promotion) {
        probe.promotion = undefined;
        probe.promotionOffset = undefined;
        probe.expiresAt = Date.now() + ATTRIBUTE_PROBE_TTL_MS;
      }
      throw error;
    },
  );
  probe.promotion = promotion;
  return promotion;
}

async function getAttributeProbePage(
  session: QueryPageSession,
  probe: AttributeProbe,
  offset: number,
  limit: number,
  tenantId: string,
  filterContext: CursorFilterContext | undefined,
) {
  const page = await readOrderedIdPage(session, offset, limit, tenantId);
  if (!page.last && probe.hasMore) {
    // Every sampled ID may have been removed by retention between requests.
    // Let the ordinary tuple-keyset query below continue from the incoming
    // cursor rather than claiming the broader query is finished.
    deleteAttributeProbe(session.id);
    return undefined;
  }
  const hasNextPage = page.hasNextWithinSnapshot || probe.hasMore;
  if (!hasNextPage) deleteAttributeProbe(session.id);
  return orderedIdPageResult(
    page,
    limit,
    filterContext,
    { attributeProbeSession: { id: session.id, offset: (page.last?.index ?? offset) + 1 } },
    probe.hasMore,
  );
}

async function getOrderedAttributeProbeIds(conditions: SQL[], limit: number) {
  const rows = await db.select({ id: logs.id }).from(logs)
    .where(and(...conditions))
    .orderBy(desc(logs.timestamp), desc(logs.id))
    .limit((limit * 2) + 1);
  return rows.map((row) => row.id);
}

async function buildPackedAttributeSnapshot(conditions: SQL[]): Promise<PackedLogIds | undefined> {
  // A postgres.js cursor is held only while copying IDs into the bounded
  // memory snapshot. It is never kept across HTTP cursor requests.
  if (buildingPackedAttributeSnapshot || !packedAttributePageSessions.hasUnreservedCapacityFor(1)) return undefined;
  buildingPackedAttributeSnapshot = true;
  try {
    const built = db.select({ id: logs.id }).from(logs)
      .where(and(...conditions))
      .orderBy(desc(logs.timestamp), desc(logs.id))
      .limit(ATTRIBUTE_BROAD_SNAPSHOT_CANDIDATES + 1)
      .toSQL();
    const blocks: Array<PackedLogIds["blocks"][number]> = [];
    let bufferedIds: string[] = [];
    let count = 0;
    for await (const rows of client.unsafe<Array<{ id: string }>>(
      built.sql,
      built.params as postgres.ParameterOrJSON<never>[],
    ).cursor(ATTRIBUTE_PACKED_ID_BLOCK_SIZE)) {
      for (const row of rows) {
        count += 1;
        if (count > ATTRIBUTE_BROAD_SNAPSHOT_CANDIDATES) return undefined;
        bufferedIds.push(row.id);
        if (bufferedIds.length === ATTRIBUTE_PACKED_ID_BLOCK_SIZE) {
          blocks.push(packLogIds(bufferedIds, ATTRIBUTE_PACKED_ID_BLOCK_SIZE).blocks[0]!);
          bufferedIds = [];
        }
      }
    }
    if (bufferedIds.length > 0) blocks.push(packLogIds(bufferedIds, ATTRIBUTE_PACKED_ID_BLOCK_SIZE).blocks[0]!);
    return count > 0
      ? { blocks, blockSize: ATTRIBUTE_PACKED_ID_BLOCK_SIZE, count }
      : undefined;
  } finally {
    buildingPackedAttributeSnapshot = false;
  }
}

// Session maps are normally touched by cursor traffic, but an abandoned
// packed walk must release its UUID buffers even when no later query arrives.
// `unref()` keeps this maintenance timer from delaying process shutdown/tests.
const queryPageSessionCleanup = setInterval(() => {
  attributePageSessions.pruneExpired();
  attributeProbeSessions.pruneExpired();
  packedAttributePageSessions.pruneExpired();
  removeExpiredAttributeProbes();
}, ATTRIBUTE_SESSION_CLEANUP_INTERVAL_MS);
queryPageSessionCleanup.unref();

export async function getLogs(params: LogQuery, tenantId = "default") {
  const filterContext = filterContextFromParams(params);
  if (isRecentLogCacheQuery(params)) {
    const recentRows = await loadCachedRecentLogs(tenantId, () => (
      db.select(publicLogProjection()).from(logs)
        .where(eq(logs.tenantId, tenantId))
        .orderBy(desc(logs.timestamp), desc(logs.id))
        .limit(RECENT_LOG_CACHE_ROWS)
    ));
    return pageResult([...recentRows], params.limit, filterContext);
  }
  const baseConditions = filterConditions(params);
  baseConditions.push(eq(logs.tenantId, tenantId));
  const conditions = [...baseConditions];
  if (params.cursor) conditions.push(cursorCondition(params.cursor));

  const hasAttributes = Object.keys(params.attributes).length > 0;
  const queryKey = hasAttributes ? attributeQueryKey(params, tenantId) : undefined;

  if (params.cursor?.attributePageSession && queryKey) {
    const sessionCursor = params.cursor.attributePageSession;
    const smallSession = attributePageSessions.peek(
      sessionCursor.id,
      tenantId,
      queryKey,
    );
    if (smallSession && sessionCursor.offset < smallSession.idCount) {
      const touchedSession = attributePageSessions.get(sessionCursor.id, tenantId, queryKey);
      if (touchedSession) {
        return getAttributeSessionPage(
          touchedSession,
          sessionCursor.offset,
          params.limit,
          tenantId,
          filterContext,
        );
      }
    }
    const packedSession = packedAttributePageSessions.peek(
      sessionCursor.id,
      tenantId,
      queryKey,
    );
    if (packedSession && sessionCursor.offset < packedSession.idCount) {
      const touchedSession = packedAttributePageSessions.get(sessionCursor.id, tenantId, queryKey);
      if (touchedSession) {
        return getAttributeSessionPage(
          touchedSession,
          sessionCursor.offset,
          params.limit,
          tenantId,
          filterContext,
          packedAttributePageSessions,
        );
      }
    }
    // A session is intentionally short-lived. Falling back to the stateless
    // tuple cursor preserves a correct result after expiry or app restart.
  }

  if (params.cursor?.attributeProbeSession && queryKey) {
    const probeCursor = params.cursor.attributeProbeSession;
    const activeProbe = findAttributeProbe(
      probeCursor.id,
      tenantId,
      queryKey,
    );
    if (activeProbe) {
      const { probe } = activeProbe;
      const offset = probeCursor.offset;
      const promotedSession = await getPromotedProbeSession(probe, offset, tenantId, queryKey);
      if (promotedSession) {
        return getAttributeSessionPage(
          promotedSession,
          0,
          params.limit,
          tenantId,
          filterContext,
          packedAttributePageSessions,
        );
      }
      const session = activeProbe.session;
      // An opaque cursor is deliberately not signed, so a client can alter an
      // offset. Never let such a value consume/delete a live snapshot. The
      // one valid end offset is a broad probe's sentinel continuation, which
      // immediately promotes from its timestamp/id cursor below.
      if (session && offset <= session.idCount && !(offset === session.idCount && !probe.hasMore)) {
        // A changed page limit can consume the probe window early. Promote
        // before returning an incomplete sample; otherwise page three is the
        // first reliable signal that the caller intends a full cursor drain.
        const shouldPromote = probe.hasMore && (
          offset >= probe.promoteAtOffset
          || offset + params.limit > probe.promoteAtOffset
        );
        if (shouldPromote) {
          const packedSession = await promoteAttributeProbe(
            probe,
            offset,
            tenantId,
            queryKey,
            conditions,
          );
          if (packedSession) {
            return getAttributeSessionPage(
              packedSession,
              0,
              params.limit,
              tenantId,
              filterContext,
              packedAttributePageSessions,
            );
          }
        } else {
          const touchedSession = touchAttributeProbe(probeCursor.id, tenantId, queryKey, probe);
          if (touchedSession) {
            const result = await getAttributeProbePage(
              touchedSession,
              probe,
              offset,
              params.limit,
              tenantId,
              filterContext,
            );
            if (result) return result;
          }
        }
      }
    }
    // A short probe can expire or be evicted. Its timestamp/id cursor still
    // provides a correct stateless continuation below.
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

  if (hasAttributes && !params.cursor && queryKey && hasKnownOverCapAttributePlan(queryKey)) {
    const ids = await getOrderedAttributeProbeIds(conditions, params.limit);
    if (ids.length === 0) return { logs: [], nextCursor: null };
    const createdProbe = createAttributeProbe(
      tenantId,
      queryKey,
      ids,
      ids.length === (params.limit * 2) + 1,
      params.limit * 2,
    );
    if (createdProbe) {
      const result = await getAttributeProbePage(
        createdProbe.session,
        createdProbe.probe,
        0,
        params.limit,
        tenantId,
        filterContext,
      );
      if (result) return result;
    }
    // If retention removes the entire probe window between its ID read and
    // row lookup, the regular query below rechecks the authoritative table.
  }

  const rows = await db.select(publicLogProjection()).from(logs)
    .where(conditions.length > 0 ? and(...conditions) : undefined)
    .orderBy(desc(logs.timestamp), desc(logs.id))
    .limit(params.limit + 1);
  return pageResult(rows, params.limit, filterContext);
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

function rawAggregateSegment(params: AggregateQuery, since: Date, until: Date, tenantId: string) {
  const effectiveParams = { ...params, since, until };
  const bucket = bucketExpression(logs.timestamp, params.bucket);
  const group = groupExpression(params.groupBy, logs.service, logs.level);
  const conditions = filterConditions(effectiveParams, true);
  conditions.push(eq(logs.tenantId, tenantId));
  // Each raw boundary is already reduced before it joins the compact
  // rollups. This keeps a busy fractional second from flooding the final
  // aggregate with individual log rows.
  return sql`
    SELECT ${bucket} AS start, ${group} AS "group", count(*)::bigint AS count
    FROM ${logs}
    WHERE ${and(...conditions)!}
    GROUP BY ${bucket}, ${group}
  `;
}

async function getRollupBackedAggregate(
  params: AggregateQuery,
  rollupSince: Date,
  rollupUntil: Date,
  tenantId: string,
): Promise<AggregateRow[]> {
  const since = params.since!;
  const until = params.until!;
  const mainTimestamp = sql`t.bucket_start`;
  const mainService = sql`t.service`;
  const mainLevel = sql`t.level`;
  const deltaTimestamp = sql`d.bucket_start`;
  const deltaService = sql`d.service`;
  const deltaLevel = sql`d.level`;
  const mainWhere = rollupWhere(mainTimestamp, mainService, mainLevel, sql`t.tenant_id`, params, rollupSince, rollupUntil, tenantId);
  const deltaWhere = rollupWhere(deltaTimestamp, deltaService, deltaLevel, sql`d.tenant_id`, params, rollupSince, rollupUntil, tenantId);
  const mainBucket = bucketExpression(mainTimestamp, params.bucket);
  const mainGroup = groupExpression(params.groupBy, mainService, mainLevel);
  const deltaBucket = bucketExpression(deltaTimestamp, params.bucket);
  const deltaGroup = groupExpression(params.groupBy, deltaService, deltaLevel);
  const segments: SQL[] = [];

  if (since < rollupSince) segments.push(rawAggregateSegment(params, since, rollupSince, tenantId));
  // Deltas are append-only, so several committed COPY batches can share a
  // bucket and group. Reducing each table arm first is algebraically exact,
  // while shrinking the UNION and final grouping work during ingestion.
  segments.push(sql`
    SELECT ${mainBucket} AS start, ${mainGroup} AS "group", sum(t.count)::bigint AS count
    FROM log_second_rollups AS t
    WHERE ${mainWhere}
    GROUP BY ${mainBucket}, ${mainGroup}
  `);
  segments.push(sql`
    SELECT ${deltaBucket} AS start, ${deltaGroup} AS "group", sum(d.count)::bigint AS count
    FROM log_second_rollup_deltas AS d
    WHERE ${deltaWhere}
    GROUP BY ${deltaBucket}, ${deltaGroup}
  `);
  if (rollupUntil < until) segments.push(rawAggregateSegment(params, rollupUntil, until, tenantId));

  // The old implementation made up to three sequential database round trips
  // (raw leading edge, rollups, raw trailing edge) and merged their results in
  // JavaScript. A single statement retains the exact half-open intervals and
  // returns one transactionally consistent aggregate snapshot instead.
  const result = await db.execute(sql`
    WITH aggregate_segments AS (
      ${sql.join(segments, sql` UNION ALL `)}
    )
    SELECT start, "group", sum(count)::integer AS count
    FROM aggregate_segments
    GROUP BY start, "group"
    ORDER BY start, "group"
  `);
  const rows = result as unknown as Array<{ start: Date | string; group: string | null; count: number | string }>;
  return rows.map((row) => ({ start: new Date(row.start).toISOString(), group: row.group, count: Number(row.count) }));
}

export async function getAggregate(params: AggregateQuery, tenantId = "default") {
  if (params.q || Object.keys(params.attributes).length > 0) return getRawAggregate(params, params.since, params.until, tenantId);

  const since = params.since!;
  const until = params.until!;
  const rollupSince = new Date(Math.ceil(since.getTime() / 1_000) * 1_000);
  const rollupUntil = new Date(Math.floor(until.getTime() / 1_000) * 1_000);
  if (rollupSince >= rollupUntil) return getRawAggregate(params, params.since, params.until, tenantId);
  return getRollupBackedAggregate(params, rollupSince, rollupUntil, tenantId);
}

export async function deleteExpiredLogs(cutoff: Date) {
  // Invalidate both sides of retention. A first-page seed whose PostgreSQL
  // snapshot predates the delete must fail its Loading identity check and can
  // never reinstall rows removed by the completed transaction.
  invalidateCachedRecentLogs();
  const generationBeforeRetention = rollupDeltaGeneration;
  try {
    const deleted = await client.begin(async (transaction) => {
      // Make every delta visible in the compact rollups before the delete trigger
      // subtracts expired rows. New ingestions hold the shared version of this
      // advisory lock until their raw rows and deltas commit.
      await transaction`SELECT pg_advisory_xact_lock(${ROLLUP_MAINTENANCE_LOCK}::bigint)`;
      let claimedCount = ROLLUP_DELTA_DRAIN_ROWS;
      while (claimedCount === ROLLUP_DELTA_DRAIN_ROWS) {
        claimedCount = await compactOneRollupDeltaChunk(transaction);
      }
      const removed = await transaction`DELETE FROM logs WHERE timestamp < ${cutoff.toISOString()}::timestamptz`;
      return removed.count;
    });
    // Reset only after COMMIT, and only if no writer committed while retention
    // was acquiring/releasing its exclusive lock. A changed generation leaves
    // a harmless overestimate that the next bounded chunk corrects; it can
    // never hide a real fresh backlog.
    if (rollupDeltaGeneration === generationBeforeRetention) estimatedRollupDeltaRows = 0;
    return deleted;
  } finally {
    invalidateCachedRecentLogs();
  }
}
