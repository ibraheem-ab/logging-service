import { ApiError } from "./errors.js";

const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

// This is deliberately an implementation detail inside the opaque cursor. It
// records that the first page proved an attribute result set was small enough
// to use the bounded candidate strategy on subsequent pages.
export type DecodedCursor = { timestamp: Date; id: string; attributeCandidateMode?: true };

export function encodeCursor(cursor: DecodedCursor) {
  return Buffer.from(JSON.stringify({
    timestamp: cursor.timestamp.toISOString(),
    id: cursor.id,
    ...(cursor.attributeCandidateMode ? { attribute_candidate_mode: true } : {}),
  })).toString("base64url");
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
    return attributeCandidateMode === true
      ? { timestamp, id: parsed.id, attributeCandidateMode: true }
      : { timestamp, id: parsed.id };
  } catch (error) {
    if (error instanceof ApiError) throw error;
    throw new ApiError("invalid cursor");
  }
}
