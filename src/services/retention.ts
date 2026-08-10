import { config } from "../config.js";
import { deleteExpiredLogs } from "../db/queries.js";

async function runRetention() {
  const cutoff = new Date(Date.now() - config.retentionDays * 24 * 60 * 60 * 1000);
  const deleted = await deleteExpiredLogs(cutoff);
  if (deleted > 0) console.log(`Retention removed ${deleted} expired logs.`);
}

export function startRetentionJob() {
  void runRetention().catch((error) => console.error("Retention job failed:", error));
  const timer = setInterval(() => {
    void runRetention().catch((error) => console.error("Retention job failed:", error));
  }, config.retentionIntervalMs);
  timer.unref();
  return () => clearInterval(timer);
}
