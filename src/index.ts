import { app } from "./app.js";
import { config } from "./config.js";
import { closeDatabase, runMigrations } from "./db/index.js";
import { flushRollupDeltas } from "./db/queries.js";
import { startRetentionJob } from "./services/retention.js";
import { seedLoadGeneratorKey } from "./services/auth.js";
import { flushPendingIngestions } from "./services/ingestion-batcher.js";
import { startRollupCompactor } from "./services/rollup-compactor.js";

async function bootstrap() {
  await runMigrations();
  await seedLoadGeneratorKey();
  const stopRetentionJob = startRetentionJob();
  const stopRollupCompactor = startRollupCompactor();

  const server = app.listen(config.port, () => {
    console.log(`Logging service is listening on port ${config.port}`);
  });

  const shutdown = async (signal: string) => {
    console.log(`${signal} received; shutting down.`);
    stopRetentionJob();
    server.close(async () => {
      await stopRollupCompactor();
      await flushPendingIngestions();
      await flushRollupDeltas();
      await closeDatabase();
      process.exit(0);
    });
  };

  process.once("SIGINT", () => void shutdown("SIGINT"));
  process.once("SIGTERM", () => void shutdown("SIGTERM"));
}

bootstrap().catch((error) => {
  console.error("Service startup failed:", error);
  process.exit(1);
});
