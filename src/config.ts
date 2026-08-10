import "dotenv/config";

function positiveInteger(value: string | undefined, fallback: number) {
  if (value === undefined) return fallback;
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`Expected a positive integer, received: ${value}`);
  }
  return parsed;
}

function boolean(value: string | undefined, fallback = false) {
  if (value === undefined) return fallback;
  if (value === "true") return true;
  if (value === "false") return false;
  throw new Error(`Expected true or false, received: ${value}`);
}

export const config = {
  port: positiveInteger(process.env.PORT, 8080),
  databaseUrl: process.env.DATABASE_URL ?? "postgres://postgres:postgres@localhost:5432/logging",
  databasePoolSize: positiveInteger(process.env.DATABASE_POOL_SIZE, 10),
  retentionDays: positiveInteger(process.env.RETENTION_DAYS, 30),
  retentionIntervalMs: positiveInteger(process.env.RETENTION_INTERVAL_MS, 3_600_000),
  maxBodySize: process.env.MAX_BODY_SIZE ?? "10mb",
  authEnabled: boolean(process.env.AUTH_ENABLED),
  loadgenApiKey: process.env.LOADGEN_API_KEY,
  rateLimitEnabled: boolean(process.env.RATE_LIMIT_ENABLED),
  rateLimitRequests: positiveInteger(process.env.RATE_LIMIT_REQUESTS, 1_000),
  backpressureEnabled: boolean(process.env.BACKPRESSURE_ENABLED),
  maxConcurrentIngestions: positiveInteger(process.env.MAX_CONCURRENT_INGESTIONS, 16),
  metricsEnabled: boolean(process.env.METRICS_ENABLED),
  liveTailEnabled: boolean(process.env.LIVE_TAIL_ENABLED),
  alertsEnabled: boolean(process.env.ALERTS_ENABLED),
  alertWebhookUrl: process.env.ALERT_WEBHOOK_URL,
  alertErrorThreshold: positiveInteger(process.env.ALERT_ERROR_THRESHOLD, 1),
  deadLetterEnabled: boolean(process.env.DEAD_LETTER_ENABLED),
  compressionEnabled: boolean(process.env.COMPRESSION_ENABLED),
};
