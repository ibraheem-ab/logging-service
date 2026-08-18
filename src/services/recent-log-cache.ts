import type { LogAttributes, LogLevel } from "../types.js";

export type RecentLogRow = {
  id: string;
  timestamp: Date;
  level: LogLevel;
  service: string;
  message: string;
  attributes: LogAttributes;
};

type ReadyState = {
  kind: "ready";
  rows: RecentLogRow[];
  bytes: number;
};

type LoadingState = {
  kind: "loading";
  token: symbol;
  rows: RecentLogRow[];
  bytes: number;
  promise: Promise<readonly RecentLogRow[]>;
};

type RecentTenantState = ReadyState | LoadingState;

type RecentLogCacheOptions = {
  capacity?: number;
  maxTenants?: number;
  maxBytesPerTenant?: number;
  maxTotalBytes?: number;
};

function newestFirst(left: RecentLogRow, right: RecentLogRow) {
  const timestampOrder = right.timestamp.getTime() - left.timestamp.getTime();
  if (timestampOrder !== 0) return timestampOrder;
  return left.id === right.id ? 0 : left.id < right.id ? 1 : -1;
}

function estimatedRowBytes(row: RecentLogRow) {
  return 256 + (2 * Buffer.byteLength(
    row.id
    + row.timestamp.toISOString()
    + row.level
    + row.service
    + row.message
    + JSON.stringify(row.attributes),
  ));
}

/**
 * A bounded, tenant-isolated cache for exactly the authoritative first 21
 * unfiltered rows. Loading entries are single-flight and cannot be evicted;
 * commits racing the database seed are merged before it becomes readable.
 */
export class RecentLogCache {
  private readonly tenants = new Map<string, RecentTenantState>();
  private totalBytes = 0;
  private readonly capacity: number;
  private readonly maxTenants: number;
  private readonly maxBytesPerTenant: number;
  private readonly maxTotalBytes: number;

  constructor(options: RecentLogCacheOptions = {}) {
    this.capacity = options.capacity ?? 21;
    this.maxTenants = options.maxTenants ?? 128;
    this.maxBytesPerTenant = options.maxBytesPerTenant ?? 1024 * 1024;
    this.maxTotalBytes = options.maxTotalBytes ?? 8 * 1024 * 1024;
  }

  get(tenantId: string) {
    const state = this.tenants.get(tenantId);
    if (state?.kind !== "ready") return undefined;
    this.tenants.delete(tenantId);
    this.tenants.set(tenantId, state);
    return state.rows as readonly RecentLogRow[];
  }

  async getOrLoad(tenantId: string, loader: () => Promise<RecentLogRow[]>) {
    const existing = this.tenants.get(tenantId);
    if (existing?.kind === "ready") {
      this.tenants.delete(tenantId);
      this.tenants.set(tenantId, existing);
      return existing.rows as readonly RecentLogRow[];
    }
    if (existing?.kind === "loading") return existing.promise;

    const loading = {
      kind: "loading" as const,
      token: Symbol(tenantId),
      rows: [],
      bytes: 0,
      promise: undefined as unknown as Promise<readonly RecentLogRow[]>,
    };
    if (!this.admit(tenantId, loading)) return loader();

    loading.promise = (async () => {
      try {
        const databaseRows = await loader();
        // Retention, an oversized racing commit, or a later generation may
        // have invalidated this exact load. Return its own snapshot to the
        // current request, but never persist it as future authoritative state.
        const current = this.tenants.get(tenantId);
        if (current?.kind !== "loading" || current.token !== loading.token) return databaseRows;
        const merged = this.merge(databaseRows, current.rows);
        const ready: ReadyState = {
          kind: "ready",
          rows: merged,
          bytes: this.rowsBytes(merged),
        };
        return this.admit(tenantId, ready) ? ready.rows : databaseRows;
      } catch (error) {
        const current = this.tenants.get(tenantId);
        if (current?.kind === "loading" && current.token === loading.token) this.remove(tenantId);
        throw error;
      }
    })();
    return loading.promise;
  }

  /** Merge only rows from a transaction that has already committed. */
  record(tenantId: string, committedRows: readonly RecentLogRow[]) {
    if (committedRows.length === 0) return;
    const state = this.tenants.get(tenantId);
    // Commits alone cannot prove the table's complete first page. If no seed
    // is active/ready, the next eligible GET must load from PostgreSQL.
    if (!state) return;
    const rows = this.merge(state.rows, committedRows);
    const replacement: RecentTenantState = {
      ...state,
      rows,
      bytes: this.rowsBytes(rows),
    };
    // Failure to admit (most commonly one huge public row) removes the whole
    // tenant state. A loading promise then fails its identity check instead of
    // installing a snapshot that omitted this committed row.
    this.admit(tenantId, replacement);
  }

  clear() {
    this.tenants.clear();
    this.totalBytes = 0;
  }

  private merge(left: readonly RecentLogRow[], right: readonly RecentLogRow[]) {
    const unique = new Map<string, RecentLogRow>();
    for (const row of left) unique.set(row.id, row);
    for (const row of right) unique.set(row.id, row);
    return [...unique.values()].sort(newestFirst).slice(0, this.capacity);
  }

  private rowsBytes(rows: readonly RecentLogRow[]) {
    return rows.reduce((total, row) => total + estimatedRowBytes(row), 0);
  }

  private remove(tenantId: string) {
    const state = this.tenants.get(tenantId);
    if (!state) return;
    this.totalBytes -= state.bytes;
    this.tenants.delete(tenantId);
  }

  private evictOldestReady() {
    for (const [tenantId, state] of this.tenants) {
      if (state.kind !== "ready") continue;
      this.remove(tenantId);
      return true;
    }
    return false;
  }

  private admit(tenantId: string, state: RecentTenantState) {
    this.remove(tenantId);
    if (state.bytes > this.maxBytesPerTenant || state.bytes > this.maxTotalBytes) return false;
    while (this.tenants.size >= this.maxTenants || this.totalBytes + state.bytes > this.maxTotalBytes) {
      if (!this.evictOldestReady()) return false;
    }
    this.tenants.set(tenantId, state);
    this.totalBytes += state.bytes;
    return true;
  }
}

const recentLogs = new RecentLogCache();

export const loadCachedRecentLogs = (tenantId: string, loader: () => Promise<RecentLogRow[]>) => (
  recentLogs.getOrLoad(tenantId, loader)
);

export function recordCachedRecentLogs(tenantId: string, rows: readonly RecentLogRow[]) {
  try {
    recentLogs.record(tenantId, rows);
  } catch {
    // The durable transaction already committed. Cache bookkeeping must never
    // turn that successful acceptance into a 500 response and a duplicate retry.
    recentLogs.clear();
  }
}

export const invalidateCachedRecentLogs = () => recentLogs.clear();
