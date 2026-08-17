import { ApiError } from "./errors.js";

const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const MAX_CURSOR_LIMIT = 10_000;

export type CursorFilterContext = {
  level?: string;
  service?: string;
  since?: string;
  until?: string;
  q?: string;
  attributes?: Record<string, string>;
};

// This is deliberately an implementation detail inside the opaque cursor. It
// records that the first page proved an attribute result set was small enough
// to use the bounded candidate strategy on subsequent pages.
export type DecodedCursor = {
  timestamp: Date;
  id: string;
  attributeCandidateMode?: true;
  filterContext?: CursorFilterContext;
  limit?: number;
};

export function encodeCursor(cursor: DecodedCursor) {
  return Buffer.from(JSON.stringify({
    timestamp: cursor.timestamp.toISOString(),
    id: cursor.id,
    ...(cursor.attributeCandidateMode ? { attribute_candidate_mode: true } : {}),
    ...(cursor.filterContext ? { filter_context: cursor.filterContext } : {}),
    ...(cursor.limit ? { limit: cursor.limit } : {}),
  })).toString("base64url");
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value) && Object.getPrototypeOf(value) === Object.prototype;
}

function decodeFilterContext(value: unknown): CursorFilterContext | undefined {
  if (value === undefined) return undefined;
  if (!isPlainObject(value)) throw new ApiError("invalid cursor");
  const allowed = new Set(["level", "service", "since", "until", "q", "attributes"]);
  if (Object.keys(value).some((key) => !allowed.has(key))) throw new ApiError("invalid cursor");

  const context: CursorFilterContext = {};
  for (const key of ["level", "service", "since", "until", "q"] as const) {
    const filterValue = value[key];
    if (filterValue !== undefined && typeof filterValue !== "string") throw new ApiError("invalid cursor");
    if (filterValue !== undefined) context[key] = filterValue;
  }
  if (value.attributes !== undefined) {
    if (!isPlainObject(value.attributes) || Object.values(value.attributes).some((attribute) => typeof attribute !== "string")) {
      throw new ApiError("invalid cursor");
    }
    context.attributes = value.attributes as Record<string, string>;
  }
  return context;
}

export function decodeCursor(value: string): DecodedCursor {
  if (!/^[A-Za-z0-9_-]+$/.test(value)) throw new ApiError("invalid cursor");
  try {
    const parsed: unknown = JSON.parse(Buffer.from(value, "base64url").toString("utf8"));
    if (typeof parsed !== "object" || parsed === null || !("timestamp" in parsed) || !("id" in parsed) || typeof parsed.timestamp !== "string" || typeof parsed.id !== "string") {
      throw new ApiError("invalid cursor");
    }
    const timestamp = new Date(parsed.timestamp);
    if (Number.isNaN(timestamp.getTime()) || !UUID_PATTERN.test(parsed.id)) throw new ApiError("invalid cursor");
    const attributeCandidateMode = "attribute_candidate_mode" in parsed
      ? parsed.attribute_candidate_mode
      : undefined;
    if (attributeCandidateMode !== undefined && attributeCandidateMode !== true) {
      throw new ApiError("invalid cursor");
    }
    const filterContext = decodeFilterContext("filter_context" in parsed ? parsed.filter_context : undefined);
    let limit: number | undefined;
    if ("limit" in parsed) {
      const rawLimit = parsed.limit;
      if (typeof rawLimit !== "number" || !Number.isInteger(rawLimit) || rawLimit < 1 || rawLimit > MAX_CURSOR_LIMIT) {
        throw new ApiError("invalid cursor");
      }
      limit = rawLimit;
    }
    return {
      timestamp,
      id: parsed.id,
      ...(attributeCandidateMode === true ? { attributeCandidateMode: true as const } : {}),
      ...(filterContext ? { filterContext } : {}),
      ...(limit !== undefined ? { limit } : {}),
    };
  } catch (error) {
    if (error instanceof ApiError) throw error;
    throw new ApiError("invalid cursor");
  }
}
