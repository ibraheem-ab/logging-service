import { ApiError } from "./errors.js";

const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const MAX_CURSOR_LIMIT = 10_000;
// A broad attribute drain can retain a compact, in-memory UUID snapshot.
// Keep the cursor bound aligned with that deliberately finite memory budget so
// a fabricated opaque cursor cannot express an unbounded application offset.
const MAX_ATTRIBUTE_SESSION_OFFSET = 2_000_000;

export type CursorFilterContext = {
  level?: string;
  service?: string;
  since?: string;
  until?: string;
  q?: string;
  attributes?: Record<string, string>;
};

// This legacy implementation detail is still parsed so cursors issued by an
// older deployment remain structurally valid. New query paths ignore it and
// safely use ordinary keyset pagination when no page session is available.
export type DecodedCursor = {
  timestamp: Date;
  id: string;
  attributeCandidateMode?: true;
  attributePageSession?: { id: string; offset: number };
  attributeProbeSession?: { id: string; offset: number };
  filterContext?: CursorFilterContext;
  limit?: number;
};

export function encodeCursor(cursor: DecodedCursor) {
  return Buffer.from(JSON.stringify({
    timestamp: cursor.timestamp.toISOString(),
    id: cursor.id,
    ...(cursor.attributeCandidateMode ? { attribute_candidate_mode: true } : {}),
    ...(cursor.attributePageSession ? {
      attribute_page_session: {
        id: cursor.attributePageSession.id,
        offset: cursor.attributePageSession.offset,
      },
    } : {}),
    ...(cursor.attributeProbeSession ? {
      attribute_probe_session: {
        id: cursor.attributeProbeSession.id,
        offset: cursor.attributeProbeSession.offset,
      },
    } : {}),
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
    let attributePageSession: { id: string; offset: number } | undefined;
    if ("attribute_page_session" in parsed) {
      const rawSession = parsed.attribute_page_session;
      if (
        !isPlainObject(rawSession)
        || typeof rawSession.id !== "string"
        || !UUID_PATTERN.test(rawSession.id)
        || typeof rawSession.offset !== "number"
        || !Number.isInteger(rawSession.offset)
        || rawSession.offset < 0
        || rawSession.offset > MAX_ATTRIBUTE_SESSION_OFFSET
      ) {
        throw new ApiError("invalid cursor");
      }
      attributePageSession = { id: rawSession.id, offset: rawSession.offset };
    }
    let attributeProbeSession: { id: string; offset: number } | undefined;
    if ("attribute_probe_session" in parsed) {
      const rawSession = parsed.attribute_probe_session;
      if (
        !isPlainObject(rawSession)
        || typeof rawSession.id !== "string"
        || !UUID_PATTERN.test(rawSession.id)
        || typeof rawSession.offset !== "number"
        || !Number.isInteger(rawSession.offset)
        || rawSession.offset < 0
        || rawSession.offset > MAX_ATTRIBUTE_SESSION_OFFSET
      ) {
        throw new ApiError("invalid cursor");
      }
      attributeProbeSession = { id: rawSession.id, offset: rawSession.offset };
    }
    if (
      (attributeCandidateMode === true && (attributePageSession || attributeProbeSession))
      || (attributePageSession && attributeProbeSession)
    ) {
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
      ...(attributePageSession ? { attributePageSession } : {}),
      ...(attributeProbeSession ? { attributeProbeSession } : {}),
      ...(filterContext ? { filterContext } : {}),
      ...(limit !== undefined ? { limit } : {}),
    };
  } catch (error) {
    if (error instanceof ApiError) throw error;
    throw new ApiError("invalid cursor");
  }
}
