import { randomUUID } from "node:crypto";
import { Buffer } from "node:buffer";

/**
 * A bounded snapshot of ordered log IDs for a selective attribute query.
 *
 * PostgreSQL's JSONB GIN index can find arbitrary attributes efficiently, but
 * it cannot also emit rows in timestamp order. Re-running the filter and sort
 * for every small cursor page is prohibitively expensive once a retained
 * database contains millions of rows. This deliberately stores only IDs,
 * never public log data; each page still reads the authoritative rows from
 * PostgreSQL.
 */
/**
 * UUIDs are 16 bytes on the wire. A large cursor walk must not retain a
 * JavaScript string/object for every ID: that would turn a 1–2M row query
 * into a container-memory risk. Blocks keep the snapshot bounded and make
 * extracting only one response page inexpensive.
 */
export type PackedLogIds = {
  readonly blocks: readonly Buffer[];
  readonly blockSize: number;
  readonly count: number;
};

export type OrderedLogIds = readonly string[] | PackedLogIds;

export type QueryPageSession = {
  readonly id: string;
  readonly ids: OrderedLogIds;
  readonly idCount: number;
};

type StoredQueryPageSession = QueryPageSession & {
  tenantId: string;
  queryKey: string;
  expiresAt: number;
};

export type QueryPageSessionsOptions = {
  maxSessions?: number;
  maxIdsPerSession?: number;
  maxTotalIds?: number;
  ttlMs?: number;
  now?: () => number;
  /** Refuse a new snapshot instead of evicting a live one at capacity. */
  evictOnCreate?: boolean;
};

export function isPackedLogIds(ids: OrderedLogIds): ids is PackedLogIds {
  return !Array.isArray(ids);
}

export function orderedLogIdCount(ids: OrderedLogIds) {
  return isPackedLogIds(ids) ? ids.count : ids.length;
}

function hexNibble(code: number) {
  if (code >= 48 && code <= 57) return code - 48;
  const lowercase = code | 32;
  if (lowercase >= 97 && lowercase <= 102) return lowercase - 87;
  return -1;
}

function packLogIdBlock(ids: readonly string[]) {
  const block = Buffer.allocUnsafe(ids.length * 16);
  let writeOffset = 0;
  for (const id of ids) {
    const expectedEnd = writeOffset + 16;
    let highNibble = -1;
    for (let readOffset = 0; readOffset < id.length; readOffset += 1) {
      const code = id.charCodeAt(readOffset);
      if (code === 45) continue;
      const nibble = hexNibble(code);
      if (nibble < 0) throw new TypeError("packed log IDs must be UUIDs");
      if (highNibble < 0) {
        highNibble = nibble;
      } else {
        block[writeOffset] = (highNibble << 4) | nibble;
        writeOffset += 1;
        highNibble = -1;
      }
    }
    if (highNibble >= 0 || writeOffset !== expectedEnd) throw new TypeError("packed log IDs must be UUIDs");
  }
  return block;
}

/** Build fixed-width UUID blocks; all blocks except the last are full. */
export function packLogIds(ids: readonly string[], blockSize = 2_048): PackedLogIds {
  if (!Number.isInteger(blockSize) || blockSize < 1) throw new RangeError("invalid packed ID block size");
  const blocks: Buffer[] = [];
  for (let offset = 0; offset < ids.length; offset += blockSize) {
    blocks.push(packLogIdBlock(ids.slice(offset, offset + blockSize)));
  }
  return { blocks, blockSize, count: ids.length };
}

export function packedLogIdAt(ids: PackedLogIds, index: number) {
  if (!Number.isInteger(index) || index < 0 || index >= ids.count) throw new RangeError("packed log ID index out of range");
  const blockIndex = Math.floor(index / ids.blockSize);
  const block = ids.blocks[blockIndex];
  const offset = (index % ids.blockSize) * 16;
  if (!block || offset + 16 > block.length) throw new RangeError("invalid packed log ID block");
  const hex = block.toString("hex", offset, offset + 16);
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

export function orderedLogIdSlice(ids: OrderedLogIds, start: number, end: number) {
  const from = Math.max(0, start);
  const to = Math.min(orderedLogIdCount(ids), Math.max(from, end));
  if (!isPackedLogIds(ids)) return ids.slice(from, to);
  const result = new Array<string>(to - from);
  for (let index = from; index < to; index += 1) {
    result[index - from] = packedLogIdAt(ids, index);
  }
  return result;
}

export class QueryPageSessions {
  private readonly sessions = new Map<string, StoredQueryPageSession>();
  private totalIds = 0;
  private readonly maxSessions: number;
  private readonly maxIdsPerSession: number;
  private readonly maxTotalIds: number;
  private readonly ttlMs: number;
  private readonly now: () => number;
  private readonly evictOnCreate: boolean;

  constructor(options: QueryPageSessionsOptions = {}) {
    this.maxSessions = options.maxSessions ?? 2;
    this.maxIdsPerSession = options.maxIdsPerSession ?? 100_000;
    this.maxTotalIds = options.maxTotalIds ?? 120_000;
    this.ttlMs = options.ttlMs ?? 60_000;
    this.now = options.now ?? Date.now;
    this.evictOnCreate = options.evictOnCreate ?? true;
  }

  create(tenantId: string, queryKey: string, ids: OrderedLogIds): QueryPageSession | undefined {
    const idCount = orderedLogIdCount(ids);
    if (idCount === 0 || idCount > this.maxIdsPerSession) return undefined;
    this.removeExpired();
    // `get()` moves a live cursor to the end of the Map. If capacity is
    // needed, evict its least-recently-used snapshot instead of poisoning a
    // new query with the stateless slow path for the rest of its drain. An
    // evicted opaque cursor still falls back correctly from timestamp/id.
    while (this.sessions.size >= this.maxSessions || this.totalIds + idCount > this.maxTotalIds) {
      if (!this.evictOnCreate) return undefined;
      const oldestId = this.sessions.keys().next().value;
      if (oldestId === undefined) break;
      this.remove(oldestId);
    }
    if (this.totalIds + idCount > this.maxTotalIds) return undefined;

    const session: StoredQueryPageSession = {
      id: randomUUID(),
      // Copy ordinary arrays so callers cannot mutate an active cursor. Packed
      // blocks are created solely for this session and are already immutable
      // by convention, avoiding an unnecessary second multi-megabyte copy.
      ids: Array.isArray(ids) ? [...ids] : ids,
      idCount,
      tenantId,
      queryKey,
      expiresAt: this.now() + this.ttlMs,
    };
    this.sessions.set(session.id, session);
    this.totalIds += session.idCount;
    return session;
  }

  get(id: string, tenantId: string, queryKey: string): QueryPageSession | undefined {
    this.removeExpired();
    const session = this.sessions.get(id);
    if (!session || session.tenantId !== tenantId || session.queryKey !== queryKey) return undefined;

    // A client may retry a page. Moving the entry to the end is LRU only; it
    // never advances state, so retries and out-of-order reads remain safe.
    session.expiresAt = this.now() + this.ttlMs;
    this.sessions.delete(id);
    this.sessions.set(id, session);
    return session;
  }

  /** Resolve a bound snapshot without treating the caller as active. */
  peek(id: string, tenantId: string, queryKey: string): QueryPageSession | undefined {
    this.removeExpired();
    const session = this.sessions.get(id);
    if (!session || session.tenantId !== tenantId || session.queryKey !== queryKey) return undefined;
    return session;
  }

  delete(id: string) {
    this.remove(id);
  }

  /** Eager maintenance hook for snapshots that would otherwise expire lazily. */
  pruneExpired() {
    this.removeExpired();
  }

  /**
   * Check capacity without evicting a live cursor. This is used by the one
   * large packed snapshot slot before doing its expensive database build.
   */
  hasUnreservedCapacityFor(idCount: number) {
    this.removeExpired();
    return Number.isInteger(idCount)
      && idCount > 0
      && idCount <= this.maxIdsPerSession
      && this.sessions.size < this.maxSessions
      && this.totalIds + idCount <= this.maxTotalIds;
  }

  private removeExpired() {
    const now = this.now();
    for (const [id, session] of this.sessions) {
      if (session.expiresAt <= now) this.remove(id);
    }
  }

  private remove(id: string) {
    const session = this.sessions.get(id);
    if (!session) return;
    this.sessions.delete(id);
    this.totalIds -= session.idCount;
  }
}
