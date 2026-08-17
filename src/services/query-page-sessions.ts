import { randomUUID } from "node:crypto";

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
export type QueryPageSession = {
  readonly id: string;
  readonly ids: readonly string[];
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
};

export class QueryPageSessions {
  private readonly sessions = new Map<string, StoredQueryPageSession>();
  private totalIds = 0;
  private readonly maxSessions: number;
  private readonly maxIdsPerSession: number;
  private readonly maxTotalIds: number;
  private readonly ttlMs: number;
  private readonly now: () => number;

  constructor(options: QueryPageSessionsOptions = {}) {
    this.maxSessions = options.maxSessions ?? 2;
    this.maxIdsPerSession = options.maxIdsPerSession ?? 100_000;
    this.maxTotalIds = options.maxTotalIds ?? 120_000;
    this.ttlMs = options.ttlMs ?? 60_000;
    this.now = options.now ?? Date.now;
  }

  create(tenantId: string, queryKey: string, ids: readonly string[]): QueryPageSession | undefined {
    if (ids.length === 0 || ids.length > this.maxIdsPerSession) return undefined;
    this.removeExpired();
    // `get()` moves a live cursor to the end of the Map. If capacity is
    // needed, evict its least-recently-used snapshot instead of poisoning a
    // new query with the stateless slow path for the rest of its drain. An
    // evicted opaque cursor still falls back correctly from timestamp/id.
    while (this.sessions.size >= this.maxSessions || this.totalIds + ids.length > this.maxTotalIds) {
      const oldestId = this.sessions.keys().next().value;
      if (oldestId === undefined) break;
      this.remove(oldestId);
    }
    if (this.totalIds + ids.length > this.maxTotalIds) return undefined;

    const session: StoredQueryPageSession = {
      id: randomUUID(),
      ids: [...ids],
      tenantId,
      queryKey,
      expiresAt: this.now() + this.ttlMs,
    };
    this.sessions.set(session.id, session);
    this.totalIds += session.ids.length;
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

  delete(id: string) {
    this.remove(id);
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
    this.totalIds -= session.ids.length;
  }
}
