import { Agent, request } from "node:http";
import { performance } from "node:perf_hooks";

const configuredBaseUrl = process.env.BASE_URL;
if (!configuredBaseUrl) throw new Error("BASE_URL is required for benchmark:regression so it cannot accidentally load the default development database");
const baseUrl = configuredBaseUrl.replace(/\/$/, "");
const targetLogsPerSecond = positiveInteger(process.env.TARGET_LOGS_PER_SECOND, 15_000);
const durationSeconds = positiveInteger(process.env.DURATION_SECONDS, 30);
const batchSize = positiveInteger(process.env.BATCH_SIZE, 32);
const maxInFlight = positiveInteger(process.env.MAX_IN_FLIGHT, 1_024);
const requestTimeoutMs = positiveInteger(process.env.REQUEST_TIMEOUT_MS, 60_000);
const aggregateIntervalMs = positiveInteger(process.env.AGGREGATE_INTERVAL_MS, 1_000);
const readAfterWriteIntervalMs = positiveInteger(process.env.READ_AFTER_WRITE_INTERVAL_MS, 5_000);
const readAfterWriteDeadlineMs = positiveInteger(process.env.READ_AFTER_WRITE_DEADLINE_MS, 20_000);

type HttpResult = { status: number; body: string; latencyMs: number };
type AcceptedBatch = { sequence: number; createdAt: number };

function positiveInteger(value: string | undefined, fallback: number) {
  if (value === undefined) return fallback;
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) throw new Error(`Expected a positive integer, received ${value}`);
  return parsed;
}

function percentile(values: number[], quantile: number) {
  if (values.length === 0) return null;
  const sorted = [...values].sort((left, right) => left - right);
  return sorted[Math.min(sorted.length - 1, Math.ceil(sorted.length * quantile) - 1)];
}

const ingestAgent = new Agent({ keepAlive: true, maxSockets: maxInFlight, maxFreeSockets: maxInFlight });
const queryAgent = new Agent({ keepAlive: true, maxSockets: 32, maxFreeSockets: 32 });

function send(url: URL, method: "GET" | "POST", body?: string, agent = ingestAgent): Promise<HttpResult> {
  return new Promise((resolve) => {
    const started = performance.now();
    const req = request(url, {
      method,
      agent,
      timeout: requestTimeoutMs,
      headers: body ? { "content-type": "application/json", "content-length": Buffer.byteLength(body) } : undefined,
    }, (response) => {
      let responseBody = "";
      response.setEncoding("utf8");
      response.on("data", (chunk) => { responseBody += chunk; });
      response.on("end", () => resolve({ status: response.statusCode ?? 0, body: responseBody, latencyMs: performance.now() - started }));
    });
    req.on("timeout", () => req.destroy(new Error("request timed out")));
    req.on("error", () => resolve({ status: 0, body: "", latencyMs: performance.now() - started }));
    if (body) req.end(body);
    else req.end();
  });
}

function logBatch(start: number) {
  const timestamp = new Date().toISOString();
  return JSON.stringify({ logs: Array.from({ length: batchSize }, (_, offset) => ({
    timestamp,
    level: offset % 10 === 0 ? "error" : "info",
    service: offset % 2 === 0 ? "checkout" : "auth",
    message: `benchmark-regression-${start + offset}`,
    attributes: { region: "eu-west", sequence: start + offset, synthetic: true },
  })) });
}

async function waitForHealthy() {
  const health = new URL(`${baseUrl}/health`);
  for (let attempt = 0; attempt < 30; attempt += 1) {
    if ((await send(health, "GET", undefined, queryAgent)).status === 200) return;
    await new Promise((resolve) => setTimeout(resolve, 1_000));
  }
  throw new Error("Service did not become healthy within 30 seconds");
}

async function main() {
  await waitForHealthy();
  const ingestUrl = new URL(`${baseUrl}/logs`);
  const aggregateUrl = new URL(`${baseUrl}/logs/aggregate`);
  const startedAt = performance.now();
  const durationMs = durationSeconds * 1_000;
  const requestsPerSecond = targetLogsPerSecond / batchSize;
  const requestLatencies: number[] = [];
  const aggregateLatencies: number[] = [];
  const visibilityLatencies: number[] = [];
  const visibilityChecks: Promise<void>[] = [];
  const aggregateChecks: Promise<void>[] = [];
  let scheduledRequests = 0;
  let inFlight = 0;
  let completedRequests = 0;
  let successfulPosts = 0;
  let acceptedLogs = 0;
  let rejectedLogs = 0;
  let failedRequests = 0;
  let aggregateFailures = 0;
  let visibleSamples = 0;
  let missingSamples = 0;
  let nextReadAfterWriteAt = readAfterWriteIntervalMs;
  let nextAggregateAt = 0;

  const trackVisibility = (sample: AcceptedBatch) => {
    visibilityChecks.push((async () => {
      const deadline = Date.now() + readAfterWriteDeadlineMs;
      const query = new URL(`${baseUrl}/logs?attr.sequence=${sample.sequence}&limit=1`);
      while (Date.now() < deadline) {
        const result = await send(query, "GET", undefined, queryAgent);
        if (result.status === 200) {
          try {
            const response = JSON.parse(result.body) as { logs?: unknown[] };
            if (response.logs?.length === 1) {
              visibleSamples += 1;
              visibilityLatencies.push(Date.now() - sample.createdAt);
              return;
            }
          } catch { /* Treat malformed data as not visible and retry. */ }
        }
        await new Promise((resolve) => setTimeout(resolve, 100));
      }
      missingSamples += 1;
    })());
  };

  const trackAggregate = () => {
    const now = Date.now();
    const query = new URL(aggregateUrl);
    query.search = new URLSearchParams({
      since: new Date(now - 60_000).toISOString(),
      until: new Date(now + 60_000).toISOString(),
      bucket: "1m",
      group_by: "service",
    }).toString();
    aggregateChecks.push(send(query, "GET", undefined, queryAgent).then((result) => {
      aggregateLatencies.push(result.latencyMs);
      if (result.status !== 200) aggregateFailures += 1;
    }));
  };

  await new Promise<void>((resolve) => {
    const ticker = setInterval(() => {
      const elapsed = performance.now() - startedAt;
      if (elapsed >= durationMs) {
        clearInterval(ticker);
        resolve();
        return;
      }

      const wanted = Math.floor((elapsed / 1_000) * requestsPerSecond);
      while (scheduledRequests < wanted && inFlight < maxInFlight) {
        const sequence = scheduledRequests * batchSize;
        scheduledRequests += 1;
        inFlight += 1;
        void send(ingestUrl, "POST", logBatch(sequence)).then((result) => {
          inFlight -= 1;
          completedRequests += 1;
          requestLatencies.push(result.latencyMs);
          if (result.status !== 200) {
            failedRequests += 1;
            return;
          }
          try {
            const body = JSON.parse(result.body) as { accepted?: number; rejected?: unknown[] };
            const accepted = body.accepted ?? 0;
            const rejected = body.rejected?.length ?? 0;
            acceptedLogs += accepted;
            rejectedLogs += rejected;
            if (accepted !== batchSize || rejected !== 0) {
              failedRequests += 1;
              return;
            }
            successfulPosts += 1;
            if (elapsed >= nextReadAfterWriteAt) {
              nextReadAfterWriteAt = (Math.floor(elapsed / readAfterWriteIntervalMs) + 1) * readAfterWriteIntervalMs;
              trackVisibility({ sequence, createdAt: Date.now() });
            }
          } catch {
            failedRequests += 1;
          }
        });
      }

      if (elapsed >= nextAggregateAt) {
        nextAggregateAt += aggregateIntervalMs;
        trackAggregate();
      }
    }, 5);
  });

  const drainDeadline = Date.now() + requestTimeoutMs;
  while (inFlight > 0 && Date.now() < drainDeadline) await new Promise((resolve) => setTimeout(resolve, 25));
  await Promise.all(visibilityChecks);
  await Promise.all(aggregateChecks);
  ingestAgent.destroy();
  queryAgent.destroy();

  const elapsedSeconds = (performance.now() - startedAt) / 1_000;
  console.log(JSON.stringify({
    targetLogsPerSecond,
    durationSeconds,
    batchSize,
    maxInFlight,
    scheduledRequests,
    completedRequests,
    successfulPosts,
    failedRequests,
    acceptedLogs,
    rejectedLogs,
    achievedLogsPerSecond: Number((acceptedLogs / elapsedSeconds).toFixed(2)),
    postSuccessRate: completedRequests === 0 ? 0 : Number((successfulPosts / completedRequests * 100).toFixed(2)),
    postLatencyMs: { p50: percentile(requestLatencies, 0.5), p95: percentile(requestLatencies, 0.95) },
    aggregate: { requests: aggregateLatencies.length, failures: aggregateFailures, p95Ms: percentile(aggregateLatencies, 0.95) },
    readAfterWrite: { visibleSamples, missingSamples, p95Ms: percentile(visibilityLatencies, 0.95) },
  }, null, 2));
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
