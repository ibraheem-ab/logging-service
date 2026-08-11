import { and, asc, desc, eq, gte, ilike, lt, lte, or, sql, type SQL } from "drizzle-orm";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import { encodeCursor, type DecodedCursor } from "../cursor.js";
import type { AggregateQuery, LogQuery } from "../validation.js";
import { client, db } from "./index.js";
import { logs, type NewLog } from "./schema.js";
import type { AttributeValue } from "../types.js";

export async function insertLogs(entries: NewLog[], tenantId = "default") {
  const copyStream = await client`
    COPY logs (timestamp, level, service, message, attributes, tenant_id)
    FROM STDIN
  `.writable();

  await pipeline(Readable.from(entries.map((entry) => toCopyRow(entry, tenantId))), copyStream);
}

function escapeCopyField(value: string) {
  return value
    .replace(/\\/g, "\\\\")
    .replace(/\t/g, "\\t")
    .replace(/\n/g, "\\n")
    .replace(/\r/g, "\\r");
}

function toCopyRow(entry: NewLog, tenantId: string) {
  const timestamp = entry.timestamp instanceof Date ? entry.timestamp.toISOString() : new Date(entry.timestamp ?? Date.now()).toISOString();
  return [
    timestamp,
    entry.level,
    entry.service,
    entry.message,
    JSON.stringify(entry.attributes ?? {}), tenantId,
  ].map((field) => escapeCopyField(String(field))).join("\t") + "\n";
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

function filterConditions(params: Omit<LogQuery, "limit" | "cursor">, untilExclusive = false): SQL[] {
  const conditions: SQL[] = [];
  if (params.level) conditions.push(eq(logs.level, params.level));
  if (params.service) conditions.push(eq(logs.service, params.service));
  if (params.since) conditions.push(gte(logs.timestamp, params.since));
  if (params.until) conditions.push(untilExclusive ? lt(logs.timestamp, params.until) : lte(logs.timestamp, params.until));
  if (params.q) conditions.push(ilike(logs.message, `%${params.q}%`));
  for (const [key, value] of Object.entries(params.attributes)) {
    conditions.push(attributeCondition(key, value));
  }
  return conditions;
}

function cursorCondition(cursor: DecodedCursor): SQL {
  return or(
    lt(logs.timestamp, cursor.timestamp),
    and(eq(logs.timestamp, cursor.timestamp), lt(logs.id, cursor.id)),
  )!;
}

export async function getLogs(params: LogQuery, tenantId = "default") {
  const conditions = filterConditions(params);
  conditions.push(eq(logs.tenantId, tenantId));
  if (params.cursor) conditions.push(cursorCondition(params.cursor));
  const rows = await db.select().from(logs)
    .where(conditions.length > 0 ? and(...conditions) : undefined)
    .orderBy(desc(logs.timestamp), desc(logs.id))
    .limit(params.limit + 1);
  const hasNextPage = rows.length > params.limit;
  const resultLogs = hasNextPage ? rows.slice(0, params.limit) : rows;
  const lastLog = resultLogs[resultLogs.length - 1];
  return { logs: resultLogs, nextCursor: hasNextPage && lastLog ? encodeCursor(lastLog) : null };
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

async function getRollupAggregate(params: AggregateQuery, since: Date, until: Date, tenantId: string): Promise<AggregateRow[]> {
  const rollupTimestamp = sql`t.bucket_start`;
  const rollupService = sql`t.service`;
  const rollupLevel = sql`t.level`;
  const bucket = bucketExpression(rollupTimestamp, params.bucket);
  const group = groupExpression(params.groupBy, rollupService, rollupLevel);
  const conditions: SQL[] = [
    sql`t.tenant_id = ${tenantId}`,
    sql`${rollupTimestamp} >= ${since.toISOString()}::timestamptz`,
    sql`${rollupTimestamp} < ${until.toISOString()}::timestamptz`,
  ];
  if (params.service) conditions.push(sql`${rollupService} = ${params.service}`);
  if (params.level) conditions.push(sql`${rollupLevel} = ${params.level}`);
  const where = and(...conditions)!;
  const result = await db.execute(sql`
    SELECT ${bucket} AS start, ${group} AS "group", sum(t.count)::integer AS count
    FROM log_second_rollups AS t
    WHERE ${where}
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
  const deleted = await client`DELETE FROM logs WHERE timestamp < ${cutoff.toISOString()}::timestamptz`;
  return deleted.count;
}
