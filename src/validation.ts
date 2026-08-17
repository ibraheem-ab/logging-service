import { decodeCursor, type CursorFilterContext, type DecodedCursor } from "./cursor.js";
import { ApiError } from "./errors.js";
import type { NewLog } from "./db/schema.js";
import { logLevels, type LogAttributes, type LogLevel } from "./types.js";

const MAX_LIMIT = 10_000;
const ISO_TIMESTAMP = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.\d+)?(Z|[+-]\d{2}:\d{2})$/;

export type LogQuery = {
  limit: number;
  level?: LogLevel;
  service?: string;
  since?: Date;
  until?: Date;
  q?: string;
  attributes: Record<string, string>;
  cursor?: DecodedCursor;
};

export type AggregateQuery = Omit<LogQuery, "limit" | "cursor"> & {
  bucket: "1m" | "5m" | "1h" | "1d";
  groupBy: "service" | "level" | null;
};

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value) && Object.getPrototypeOf(value) === Object.prototype;
}

function isValidIsoTimestamp(value: string) {
  const match = ISO_TIMESTAMP.exec(value);
  if (!match) return false;

  const [, yearText, monthText, dayText, hourText, minuteText, secondText, timezone] = match;
  const year = Number(yearText);
  const month = Number(monthText);
  const day = Number(dayText);
  const hour = Number(hourText);
  const minute = Number(minuteText);
  const second = Number(secondText);
  const maxDay = month === 2
    ? (year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0) ? 29 : 28)
    : [4, 6, 9, 11].includes(month) ? 30 : 31;

  if (month < 1 || month > 12 || day < 1 || day > maxDay || hour > 23 || minute > 59 || second > 59) {
    return false;
  }
  if (timezone !== "Z") {
    const [, offsetHour, offsetMinute] = timezone.match(/^[+-](\d{2}):(\d{2})$/)!;
    if (Number(offsetHour) > 23 || Number(offsetMinute) > 59) return false;
  }
  return true;
}

function parseTimestamp(value: unknown, field: string, requireIsoFormat: boolean): Date {
  if (typeof value !== "string" || (requireIsoFormat && !isValidIsoTimestamp(value))) {
    throw new ApiError(`${field} must be a valid ISO 8601 timestamp`);
  }
  const timestamp = new Date(value);
  if (Number.isNaN(timestamp.getTime())) throw new ApiError(`${field} must be a valid ISO 8601 timestamp`);
  return timestamp;
}

function validateAttributes(value: unknown): LogAttributes {
  if (!isPlainObject(value)) throw new ApiError("attributes must be a flat object");
  const attributes: LogAttributes = {};
  for (const [key, attributeValue] of Object.entries(value)) {
    if (!key || typeof attributeValue === "object" || typeof attributeValue === "undefined" || !["string", "number", "boolean"].includes(typeof attributeValue)) {
      throw new ApiError("attributes values must be strings, numbers, or booleans");
    }
    if (typeof attributeValue === "number" && !Number.isFinite(attributeValue)) {
      throw new ApiError("attributes values must be finite numbers");
    }
    attributes[key] = attributeValue as string | number | boolean;
  }
  return attributes;
}

function validateLog(value: unknown): NewLog {
  if (!isPlainObject(value)) throw new ApiError("log entry must be an object");
  const timestamp = parseTimestamp(value.timestamp, "timestamp", true);
  if (timestamp.getTime() > Date.now() + 5 * 60 * 1000) throw new ApiError("timestamp must not be more than five minutes in the future");
  if (typeof value.level !== "string" || !logLevels.includes(value.level as LogLevel)) {
    throw new ApiError(`invalid level: '${String(value.level)}'`);
  }
  if (typeof value.service !== "string" || value.service.trim() === "") throw new ApiError("service must be a non-empty string");
  if (typeof value.message !== "string" || value.message.trim() === "") throw new ApiError("message must be a non-empty string");
  return {
    timestamp,
    level: value.level as LogLevel,
    service: value.service,
    message: value.message,
    attributes: value.attributes === undefined ? {} : validateAttributes(value.attributes),
  };
}

export function validateIngestionBatch(body: unknown) {
  if (!isPlainObject(body) || !Array.isArray(body.logs)) throw new ApiError("request body must be an object with a logs array");
  const validLogs: NewLog[] = [];
  const rejected: Array<{ index: number; reason: string }> = [];
  body.logs.forEach((entry, index) => {
    try {
      validLogs.push(validateLog(entry));
    } catch (error) {
      rejected.push({ index, reason: error instanceof Error ? error.message : "invalid log entry" });
    }
  });
  return { validLogs, rejected };
}

function scalar(query: Record<string, unknown>, name: string): string | undefined {
  const value = query[name];
  if (value === undefined) return undefined;
  if (typeof value !== "string") throw new ApiError(`${name} must be a single string`);
  return value;
}

function validateAttributeKey(key: string) {
  if (!key || key.length > 128) throw new ApiError("attribute filter key must be between 1 and 128 characters");
}

function parseFilters(query: Record<string, unknown>, aggregation = false) {
  const allowed = new Set(aggregation
    ? ["level", "service", "since", "until", "q", "query", "bucket", "group_by"]
    : ["limit", "level", "service", "since", "until", "q", "query", "cursor"]);
  const attributes: Record<string, string> = {};
  for (const [key, value] of Object.entries(query)) {
    if (key.startsWith("attr.")) {
      const attributeKey = key.slice(5);
      validateAttributeKey(attributeKey);
      if (typeof value !== "string") throw new ApiError(`${key} must be a single string`);
      attributes[attributeKey] = value;
    } else if (!allowed.has(key)) {
      throw new ApiError(`unsupported query parameter: ${key}`);
    }
  }

  let level = scalar(query, "level");
  if (level !== undefined && !logLevels.includes(level as LogLevel)) throw new ApiError("level must be one of: debug, info, warn, error");
  let service = scalar(query, "service");
  if (service !== undefined && service.trim() === "") throw new ApiError("service must be a non-empty string");
  const sinceValue = scalar(query, "since");
  const untilValue = scalar(query, "until");
  const since = sinceValue === undefined ? undefined : parseTimestamp(sinceValue, "since", true);
  const until = untilValue === undefined ? undefined : parseTimestamp(untilValue, "until", true);
  if (since && until && until < since) throw new ApiError("until must not be earlier than since");
  let q = scalar(query, "q");
  const expression = scalar(query, "query");
  if (expression !== undefined) {
    for (const token of expression.trim().split(/\s+/).filter(Boolean)) {
      const separator = token.indexOf(":");
      if (separator < 1) throw new ApiError("query terms must use key:value syntax");
      const key = token.slice(0, separator);
      const value = token.slice(separator + 1);
      if (!value) throw new ApiError("query values must not be empty");
      if (key === "service") service = value;
      else if (key === "level") level = value;
      else if (key === "q") q = value;
      else if (key.startsWith("attr.")) { validateAttributeKey(key.slice(5)); attributes[key.slice(5)] = value; }
      else throw new ApiError(`unsupported query term: ${key}`);
    }
  }
  if (level !== undefined && !logLevels.includes(level as LogLevel)) throw new ApiError("level must be one of: debug, info, warn, error");
  if (service !== undefined && service.trim() === "") throw new ApiError("service must be a non-empty string");
  if (q !== undefined && q.length > 500) throw new ApiError("q must be at most 500 characters");
  return { level: level as LogLevel | undefined, service, since, until, q, attributes };
}

type ParsedFilters = ReturnType<typeof parseFilters>;

function cursorContextQuery(context: CursorFilterContext): Record<string, unknown> {
  const query: Record<string, unknown> = {};
  for (const key of ["level", "service", "since", "until", "q"] as const) {
    if (context[key] !== undefined) query[key] = context[key];
  }
  for (const [key, value] of Object.entries(context.attributes ?? {})) {
    query[`attr.${key}`] = value;
  }
  return query;
}

function cursorFilterMismatch() {
  throw new ApiError("cursor does not match query filters");
}

// A cursor represents a continuation of one logical query. Carrying the
// original filters in the opaque cursor lets clients safely send only the
// cursor on later pages, while still accepting clients that repeat matching
// filters explicitly.
function mergeCursorFilters(requested: ParsedFilters, saved: ParsedFilters): ParsedFilters {
  const scalar = <T>(requestedValue: T | undefined, savedValue: T | undefined) => {
    if (requestedValue !== undefined && savedValue !== undefined && requestedValue !== savedValue) cursorFilterMismatch();
    if (requestedValue !== undefined && savedValue === undefined) cursorFilterMismatch();
    return savedValue ?? requestedValue;
  };
  const date = (requestedValue: Date | undefined, savedValue: Date | undefined) => {
    if (requestedValue !== undefined && savedValue !== undefined && requestedValue.getTime() !== savedValue.getTime()) cursorFilterMismatch();
    if (requestedValue !== undefined && savedValue === undefined) cursorFilterMismatch();
    return savedValue ?? requestedValue;
  };
  const attributes = { ...saved.attributes };
  for (const [key, value] of Object.entries(requested.attributes)) {
    if (saved.attributes[key] !== value) cursorFilterMismatch();
  }
  return {
    level: scalar(requested.level, saved.level) as LogLevel | undefined,
    service: scalar(requested.service, saved.service),
    since: date(requested.since, saved.since),
    until: date(requested.until, saved.until),
    q: scalar(requested.q, saved.q),
    attributes,
  };
}

export function parseLogsQuery(rawQuery: Record<string, unknown>): LogQuery {
  const requestedFilters = parseFilters(rawQuery);
  const cursorValue = scalar(rawQuery, "cursor");
  const cursor = cursorValue === undefined ? undefined : decodeCursor(cursorValue);
  const filters = cursor?.filterContext
    ? mergeCursorFilters(requestedFilters, parseFilters(cursorContextQuery(cursor.filterContext)))
    : requestedFilters;
  const limitValue = scalar(rawQuery, "limit");
  // A larger default keeps an unqualified cursor walk practical during the
  // eventual-consistency drain. The public contract leaves the default
  // implementation-defined and is documented in the README.
  const limit = limitValue === undefined ? (cursor?.limit ?? MAX_LIMIT) : Number(limitValue);
  if (!Number.isInteger(limit) || limit < 1 || limit > MAX_LIMIT) throw new ApiError(`limit must be an integer between 1 and ${MAX_LIMIT}`);
  return { ...filters, limit, cursor };
}

export function parseAggregateQuery(rawQuery: Record<string, unknown>): AggregateQuery {
  const filters = parseFilters(rawQuery, true);
  if (!filters.since) throw new ApiError("since is required for aggregation");
  if (!filters.until) throw new ApiError("until is required for aggregation");
  const bucket = scalar(rawQuery, "bucket");
  if (bucket !== "1m" && bucket !== "5m" && bucket !== "1h" && bucket !== "1d") {
    throw new ApiError("bucket must be one of: 1m, 5m, 1h, 1d");
  }
  const groupByInput = scalar(rawQuery, "group_by");
  if (groupByInput !== undefined && groupByInput !== "service" && groupByInput !== "level") {
    throw new ApiError("group_by must be service or level");
  }
  return { ...filters, bucket, groupBy: (groupByInput ?? null) as AggregateQuery["groupBy"] };
}
