import { compactRollupDeltaChunk, hasRollupDeltaBacklog } from "../db/queries.js";
import { areDatabaseQueriesIdleFor } from "./database-activity.js";
import { isIngestionIdleFor } from "./ingestion-batcher.js";

type CompactorOptions = {
  intervalMs?: number;
  minimumIdleMs?: number;
  maxChunksPerTurn?: number;
  onError?: (error: unknown) => void;
};

/**
 * Run bounded rollup maintenance during a real quiet window, or one chunk at
 * a time when the append-only backlog crosses its conservative threshold.
 *
 * Normal current-time micro-batches group into very few delta rows and stay
 * below that threshold. A large sequential backfill cannot grow without bound:
 * A 50k-row chunk amortizes the grouped UPSERT work; after one chunk drops the
 * estimate below 100k, the next loop check stops.
 */
export function createRollupCompactor(
  compactChunk: () => Promise<boolean>,
  isIdle: (minimumIdleMs: number) => boolean,
  options: CompactorOptions = {},
) {
  const intervalMs = options.intervalMs ?? 100;
  const minimumIdleMs = options.minimumIdleMs ?? 250;
  const maxChunksPerTurn = options.maxChunksPerTurn ?? 4;
  const onError = options.onError ?? ((error: unknown) => console.error("Rollup compaction failed:", error));
  let stopped = false;
  let activeRun: Promise<void> | undefined;

  const run = async () => {
    try {
      for (let chunk = 0; chunk < maxChunksPerTurn && !stopped && isIdle(minimumIdleMs); chunk += 1) {
        const mayHaveMore = await compactChunk();
        if (!mayHaveMore) break;
      }
    } catch (error) {
      onError(error);
    }
  };

  const timer = setInterval(() => {
    if (stopped || activeRun || !isIdle(minimumIdleMs)) return;
    activeRun = run().finally(() => { activeRun = undefined; });
  }, intervalMs);
  timer.unref();

  return async () => {
    stopped = true;
    clearInterval(timer);
    await activeRun;
  };
}

export function startRollupCompactor() {
  return createRollupCompactor(
    compactRollupDeltaChunk,
    (minimumIdleMs) => areDatabaseQueriesIdleFor(0) && (
      hasRollupDeltaBacklog()
      || (
        hasRollupDeltaBacklog(1)
        && isIngestionIdleFor(minimumIdleMs)
        && areDatabaseQueriesIdleFor(minimumIdleMs)
      )
    ),
  );
}
