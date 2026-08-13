import { config } from "../config.js";
import { insertLogWrites } from "../db/queries.js";
import { createIngestionQueue } from "./ingestion-queue.js";

const queue = createIngestionQueue(insertLogWrites, {
  maxLogs: config.ingestionFlushMaxLogs,
  maxDelayMs: config.ingestionFlushMaxDelayMs,
  maxConcurrentFlushes: config.ingestionFlushConcurrency,
});

export const enqueueIngestion = queue.enqueue;
export const flushPendingIngestions = queue.flushPending;
