import type { LogWrite } from "../db/queries.js";
import type { NewLog } from "../db/schema.js";

type PendingIngestion = {
  tenantId: string;
  entries: NewLog[];
  enqueuedAt: number;
  resolve: () => void;
  reject: (error: unknown) => void;
};

export type IngestionWriter = (writes: LogWrite[]) => Promise<void>;
export type IngestionQueueOptions = {
  maxLogs: number;
  maxDelayMs: number;
  maxConcurrentFlushes?: number;
};

export function createIngestionQueue(
  writer: IngestionWriter,
  options: IngestionQueueOptions,
  onFlushError: (error: unknown) => void = (error) => console.error("Ingestion flush failed:", error),
) {
  let pending: PendingIngestion[] = [];
  let pendingHead = 0;
  let pendingLogs = 0;
  let flushTimer: ReturnType<typeof setTimeout> | undefined;
  const activeFlushes = new Set<Promise<void>>();
  const maxConcurrentFlushes = options.maxConcurrentFlushes ?? 1;

  function clearFlushTimer() {
    if (!flushTimer) return;
    clearTimeout(flushTimer);
    flushTimer = undefined;
  }

  function takeFlushBatch() {
    const batch: PendingIngestion[] = [];
    let logCount = 0;
    while (pendingHead < pending.length) {
      const next = pending[pendingHead]!;
      // Keep one HTTP request together. This retains the all-or-nothing behavior
      // of a request's valid entries even when multiple requests share one COPY.
      if (batch.length > 0 && logCount + next.entries.length > options.maxLogs) break;
      pendingHead += 1;
      pendingLogs -= next.entries.length;
      batch.push(next);
      logCount += next.entries.length;
    }
    // Avoid Array#shift(), which repeatedly reindexes a backlog of small HTTP
    // requests. Compact only after a batch so dequeueing stays O(1).
    if (pendingHead === pending.length) {
      pending = [];
      pendingHead = 0;
    } else if (pendingHead >= 1_024 && pendingHead * 2 >= pending.length) {
      pending = pending.slice(pendingHead);
      pendingHead = 0;
    }
    return batch;
  }

  function oldestPendingAgeMs() {
    const oldest = pending[pendingHead];
    return oldest ? Date.now() - oldest.enqueuedAt : 0;
  }

  function hasPending() {
    return pendingHead < pending.length;
  }

  function shouldFlush() {
    return pendingLogs >= options.maxLogs || oldestPendingAgeMs() >= options.maxDelayMs;
  }

  async function flushOneBatch() {
    const batch = takeFlushBatch();
    if (batch.length === 0) return;
    const writes: LogWrite[] = [];
    for (const request of batch) {
      for (const entry of request.entries) writes.push({ entry, tenantId: request.tenantId });
    }

    try {
      await writer(writes);
      for (const request of batch) request.resolve();
    } catch (error) {
      onFlushError(error);
      for (const request of batch) request.reject(error);
    }
  }

  function startFlushes(force = false) {
    if (!hasPending()) return;
    if (force || shouldFlush()) clearFlushTimer();

    while (
      hasPending()
      && activeFlushes.size < maxConcurrentFlushes
      && (force || shouldFlush())
    ) {
      let current!: Promise<void>;
      current = flushOneBatch().finally(() => {
        activeFlushes.delete(current);
        if (hasPending()) scheduleFlush();
      });
      activeFlushes.add(current);
    }
  }

  function scheduleFlush() {
    if (!hasPending()) return;
    startFlushes();
    if (!hasPending()) return;

    // Requests accumulated while every writer was busy have already spent
    // their batching wait. As soon as a writer frees up, start them instead
    // of making them wait a second full maxDelayMs interval.
    if (activeFlushes.size < maxConcurrentFlushes && shouldFlush()) {
      startFlushes(true);
      return;
    }

    if (
      hasPending()
      && activeFlushes.size < maxConcurrentFlushes
      && !flushTimer
    ) {
      flushTimer = setTimeout(() => {
        flushTimer = undefined;
        startFlushes(true);
      }, Math.max(0, options.maxDelayMs - oldestPendingAgeMs()));
    }
  }

/**
 * Resolves only when PostgreSQL has committed both the raw rows and their
 * rollup rows. Concurrent COPY work is allowed; the database serializes only
 * the short rollup-and-commit critical section.
 */
  function enqueue(tenantId: string, entries: NewLog[]) {
    return new Promise<void>((resolve, reject) => {
      pending.push({ tenantId, entries, enqueuedAt: Date.now(), resolve, reject });
      pendingLogs += entries.length;
      scheduleFlush();
    });
  }

/** Drain queued writes before database shutdown so accepted requests are never lost. */
  async function flushPending() {
    clearFlushTimer();
    while (hasPending() || activeFlushes.size > 0) {
      startFlushes(true);
      if (activeFlushes.size > 0) {
        await Promise.race(activeFlushes);
      }
    }
  }

  return { enqueue, flushPending };
}
