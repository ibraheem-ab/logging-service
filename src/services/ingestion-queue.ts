import type { LogWrite } from "../db/queries.js";
import type { NewLog } from "../db/schema.js";

type PendingIngestion = {
  tenantId: string;
  entries: NewLog[];
  enqueuedAt: number;
  resolve: () => void;
  reject: (error: unknown) => void;
};

type FlushOutcome =
  | { ok: true; batch: PendingIngestion[]; wasIsolated: boolean }
  | { ok: false; batch: PendingIngestion[]; error: unknown };

export type IngestionWriter = (writes: LogWrite[]) => Promise<void>;
export type IngestionQueueOptions = {
  maxLogs: number;
  maxDelayMs: number;
  maxConcurrentFlushes?: number;
};

// A producer which sends one request at a time should not have to pay the
// whole high-throughput batching window. Keeping this short still gives a
// second request time to join, while sustained traffic continues to use the
// configured maxDelayMs window.
const SPARSE_REQUEST_FLUSH_MAX_DELAY_MS = 20;
const PROVEN_SERIAL_REQUEST_FLUSH_MAX_DELAY_MS = 1;
type FlushTimerKind = "normal" | "sparse";

export function createIngestionQueue(
  writer: IngestionWriter,
  options: IngestionQueueOptions,
  onFlushError: (error: unknown) => void = (error) => console.error("Ingestion flush failed:", error),
) {
  let pending: PendingIngestion[] = [];
  let pendingHead = 0;
  let pendingLogs = 0;
  let flushTimer: ReturnType<typeof setTimeout> | undefined;
  let flushTimerKind: FlushTimerKind | undefined;
  let flushTimerGeneration = 0;
  let hasProvenSequentialProducer = false;
  const activeFlushes = new Set<Promise<void>>();
  const maxConcurrentFlushes = options.maxConcurrentFlushes ?? 1;

  function clearFlushTimer() {
    if (flushTimer) clearTimeout(flushTimer);
    flushTimer = undefined;
    flushTimerKind = undefined;
    flushTimerGeneration += 1;
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

  function pendingRequestCount() {
    return pending.length - pendingHead;
  }

  function shouldFlush() {
    return pendingLogs >= options.maxLogs || oldestPendingAgeMs() >= options.maxDelayMs;
  }

  function shouldUseSparseFlush() {
    return pendingRequestCount() === 1 && activeFlushes.size === 0;
  }

  function sparseFlushDelayMs() {
    return hasProvenSequentialProducer
      ? PROVEN_SERIAL_REQUEST_FLUSH_MAX_DELAY_MS
      : SPARSE_REQUEST_FLUSH_MAX_DELAY_MS;
  }

  function armFlushTimer(kind: FlushTimerKind, delayMs: number) {
    clearFlushTimer();
    const generation = ++flushTimerGeneration;
    flushTimerKind = kind;
    flushTimer = setTimeout(() => {
      if (generation !== flushTimerGeneration) return;
      flushTimer = undefined;
      flushTimerKind = undefined;

      // A second request may have arrived while a sparse timer was pending.
      // In that case, restore its normal coalescing window rather than forcing
      // an undersized batch.
      if (kind === "sparse" && !shouldUseSparseFlush()) {
        scheduleFlush();
        return;
      }
      startFlushes(true);
    }, delayMs);
  }

  async function flushOneBatch(): Promise<FlushOutcome | undefined> {
    const batch = takeFlushBatch();
    if (batch.length === 0) return;
    const wasIsolated = batch.length === 1 && activeFlushes.size === 0 && !hasPending();
    const writes: LogWrite[] = [];
    for (const request of batch) {
      for (const entry of request.entries) writes.push({ entry, tenantId: request.tenantId });
    }

    try {
      await writer(writes);
      return { ok: true, batch, wasIsolated };
    } catch (error) {
      return { ok: false, batch, error };
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
      current = flushOneBatch().then((outcome) => {
        activeFlushes.delete(current);

        if (!outcome) {
          hasProvenSequentialProducer = false;
        } else if (outcome.ok) {
          // A serial producer waits for the committed response before sending
          // its next request. After proving that pattern, use a near-immediate
          // timer for its next singleton without reducing burst coalescing.
          hasProvenSequentialProducer = outcome.wasIsolated
            && !hasPending()
            && activeFlushes.size === 0;
          for (const request of outcome.batch) request.resolve();
        } else {
          hasProvenSequentialProducer = false;
          onFlushError(outcome.error);
          for (const request of outcome.batch) request.reject(outcome.error);
        }

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

    if (hasPending() && activeFlushes.size < maxConcurrentFlushes) {
      const kind: FlushTimerKind = shouldUseSparseFlush() ? "sparse" : "normal";
      if (flushTimer && flushTimerKind === kind) return;

      const remainingDelayMs = Math.max(0, options.maxDelayMs - oldestPendingAgeMs());
      const delayMs = kind === "sparse"
        ? Math.min(sparseFlushDelayMs(), remainingDelayMs)
        : remainingDelayMs;
      armFlushTimer(kind, delayMs);
    }
  }

/**
 * Resolves only when PostgreSQL has committed both the raw rows and their
 * rollup rows. Concurrent COPY work is allowed; the database serializes only
 * the short rollup-and-commit critical section.
 */
  function enqueue(tenantId: string, entries: NewLog[]) {
    return new Promise<void>((resolve, reject) => {
      // A queued request or active write proves this is a burst, not the
      // committed-response-at-a-time pattern eligible for the serial fast path.
      if (hasPending() || activeFlushes.size > 0) hasProvenSequentialProducer = false;
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
