import { performance } from "node:perf_hooks";

const baseUrl = (process.env.BASE_URL ?? "http://127.0.0.1:8080").replace(/\/$/, "");
const totalLogs = positiveInteger(process.env.TOTAL_LOGS, 1000000);
const batchSize = positiveInteger(process.env.BATCH_SIZE, 1_000);
const concurrency = positiveInteger(process.env.CONCURRENCY, 8);

function positiveInteger(value: string | undefined, fallback: number) {
  if (value === undefined) return fallback;
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) throw new Error(`Expected a positive integer, received ${value}`);
  return parsed;
}

function percentile(values: number[], position: number) {
  if (values.length === 0) return null;
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.min(sorted.length - 1, Math.ceil(sorted.length * position) - 1)];
}

function makeBatch(start: number, size: number) {
  const timestamp = new Date().toISOString();
  return Array.from({ length: size }, (_, offset) => ({
    timestamp,
    level: offset % 10 === 0 ? "error" : "info",
    service: offset % 2 === 0 ? "checkout" : "auth",
    message: `load-test log ${start + offset}`,
    attributes: { region: "eu-west", sequence: start + offset, synthetic: true },
  }));
}

async function requireSuccess(response: Response) {
  if (!response.ok) throw new Error(`HTTP ${response.status}: ${await response.text()}`);
}

async function waitForHealthy() {
  let lastError: unknown;
  for (let attempt = 0; attempt < 30; attempt += 1) {
    try {
      await requireSuccess(await fetch(`${baseUrl}/health`));
      return;
    } catch (error) {
      lastError = error;
      await new Promise((resolve) => setTimeout(resolve, 1_000));
    }
  }
  throw new Error(`Service did not become healthy within 30 seconds: ${String(lastError)}`);
}

async function main() {
  await waitForHealthy();
  const aggregateSince = new Date(Date.now() - 60_000).toISOString();
  const ingestionLatencies: number[] = [];
  const aggregationLatencies: number[] = [];
  let sent = 0;
  let accepted = 0;
  let keepQuerying = true;

  const aggregationLoop = (async () => {
    while (keepQuerying) {
      const started = performance.now();
      const aggregateUntil = new Date(Date.now() + 60_000).toISOString();
      const query = new URLSearchParams({ since: aggregateSince, until: aggregateUntil, bucket: "1m", group_by: "service" });
      await requireSuccess(await fetch(`${baseUrl}/logs/aggregate?${query}`));
      aggregationLatencies.push(performance.now() - started);
      await new Promise((resolve) => setTimeout(resolve, 1_000));
    }
  })();

  const started = performance.now();
  const workers = Array.from({ length: concurrency }, async () => {
    while (true) {
      const start = sent;
      if (start >= totalLogs) return;
      const size = Math.min(batchSize, totalLogs - start);
      sent += size;
      const requestStarted = performance.now();
      const response = await fetch(`${baseUrl}/logs`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ logs: makeBatch(start, size) }),
      });
      await requireSuccess(response);
      const body = await response.json() as { accepted: number; rejected: unknown[] };
      if (body.accepted !== size || body.rejected.length !== 0) throw new Error("Server rejected generated load-test logs");
      accepted += body.accepted;
      ingestionLatencies.push(performance.now() - requestStarted);
    }
  });

  await Promise.all(workers);
  keepQuerying = false;
  await aggregationLoop;
  const elapsedSeconds = (performance.now() - started) / 1_000;

  console.log(JSON.stringify({
    totalLogs,
    accepted,
    batchSize,
    concurrency,
    ingestionRateLogsPerSecond: Number((accepted / elapsedSeconds).toFixed(2)),
    ingestionLatencyMs: { p50: percentile(ingestionLatencies, 0.5), p95: percentile(ingestionLatencies, 0.95) },
    aggregationLatencyMs: { p50: percentile(aggregationLatencies, 0.5), p95: percentile(aggregationLatencies, 0.95) },
  }, null, 2));
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
