import { Agent, request } from "node:http";
import { performance } from "node:perf_hooks";

const configuredBaseUrl = process.env.BASE_URL;
if (!configuredBaseUrl) throw new Error("BASE_URL is required for benchmark:scenarios");

const baseUrl = configuredBaseUrl.replace(/\/$/, "");
const scenarioName = process.env.SCENARIO ?? "load";
const batchSize = positiveInteger(process.env.BATCH_SIZE, 32);
const maxInFlight = positiveInteger(process.env.MAX_IN_FLIGHT, 1_024);
const requestTimeoutMs = positiveInteger(process.env.REQUEST_TIMEOUT_MS, 60_000);
const aggregateIntervalMs = positiveInteger(process.env.AGGREGATE_INTERVAL_MS, 1_000);
const readAfterWriteIntervalMs = positiveInteger(process.env.READ_AFTER_WRITE_INTERVAL_MS, 5_000);
const readAfterWriteDeadlineMs = positiveInteger(process.env.READ_AFTER_WRITE_DEADLINE_MS, 20_000);

type Stage = { name: string; targetLogsPerSecond: number; durationSeconds: number };
type HttpResult = { status: number; body: string; latencyMs: number };
type AcceptedBatch = { sequence: number; createdAt: number; stage: StageStats };
type StageStats = Stage & {
  offeredRequests: number;
  clientShedRequests: number;
  scheduledRequests: number;
  completedRequests: number;
  successfulPosts: number;
  failedRequests: number;
  acceptedLogs: number;
  rejectedLogs: number;
  postLatencies: number[];
  aggregateLatencies: number[];
  aggregateFailures: number;
  visibilityLatencies: number[];
  visibleSamples: number;
  missingSamples: number;
};

const scenarios: Record<string, Stage[]> = {
  load: [{ name: "Load", targetLogsPerSecond: 15_000, durationSeconds: 120 }],
  stress: [
    { name: "Stage 1", targetLogsPerSecond: 15_000, durationSeconds: 30 },
    { name: "Stage 2", targetLogsPerSecond: 22_500, durationSeconds: 60 },
    { name: "Stage 3", targetLogsPerSecond: 30_000, durationSeconds: 60 },
  ],
  spike: [
    { name: "Stage 1", targetLogsPerSecond: 7_500, durationSeconds: 30 },
    { name: "Stage 2", targetLogsPerSecond: 30_000, durationSeconds: 10 },
    { name: "Stage 3", targetLogsPerSecond: 7_500, durationSeconds: 60 },
  ],
  breakpoint: [
    { name: "Stage 1", targetLogsPerSecond: 15_000, durationSeconds: 30 },
    { name: "Stage 2", targetLogsPerSecond: 22_500, durationSeconds: 30 },
    { name: "Stage 3", targetLogsPerSecond: 30_000, durationSeconds: 30 },
    { name: "Stage 4", targetLogsPerSecond: 45_000, durationSeconds: 30 },
  ],
};

const stages = scenarios[scenarioName];
if (!stages) throw new Error(`SCENARIO must be one of: ${Object.keys(scenarios).join(", ")}`);

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

function newStageStats(stage: Stage): StageStats {
  return {
    ...stage,
    offeredRequests: 0,
    clientShedRequests: 0,
    scheduledRequests: 0,
    completedRequests: 0,
    successfulPosts: 0,
    failedRequests: 0,
    acceptedLogs: 0,
    rejectedLogs: 0,
    postLatencies: [],
    aggregateLatencies: [],
    aggregateFailures: 0,
    visibilityLatencies: [],
    visibleSamples: 0,
    missingSamples: 0,
  };
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
    req.end(body);
  });
}

function logBatch(start: number) {
  const timestamp = new Date().toISOString();
  return JSON.stringify({ logs: Array.from({ length: batchSize }, (_, offset) => ({
    timestamp,
    level: offset % 10 === 0 ? "error" : "info",
    service: offset % 2 === 0 ? "checkout" : "auth",
    message: `benchmark-scenario-${start + offset}`,
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
  const stats = stages.map(newStageStats);
  const stageBoundaries = stages.reduce<number[]>((boundaries, stage) => {
    boundaries.push((boundaries.at(-1) ?? 0) + stage.durationSeconds * 1_000);
    return boundaries;
  }, []);
  const totalDurationMs = stageBoundaries.at(-1)!;
  const ingestUrl = new URL(`${baseUrl}/logs`);
  const aggregateUrl = new URL(`${baseUrl}/logs/aggregate`);
  const startedAt = performance.now();
  const visibilityChecks: Promise<void>[] = [];
  const aggregateChecks: Promise<void>[] = [];
  let nextSequence = 0;
  let inFlight = 0;
  let nextReadAfterWriteAt = readAfterWriteIntervalMs;
  let nextAggregateAt = 0;

  const stageIndexAt = (elapsedMs: number) => stageBoundaries.findIndex((boundary) => elapsedMs < boundary);

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
              sample.stage.visibleSamples += 1;
              sample.stage.visibilityLatencies.push(Date.now() - sample.createdAt);
              return;
            }
          } catch { /* Treat malformed data as not visible and retry. */ }
        }
        await new Promise((resolve) => setTimeout(resolve, 100));
      }
      sample.stage.missingSamples += 1;
    })());
  };

  const trackAggregate = (stage: StageStats) => {
    const now = Date.now();
    const query = new URL(aggregateUrl);
    query.search = new URLSearchParams({
      since: new Date(now - 60_000).toISOString(),
      until: new Date(now + 60_000).toISOString(),
      bucket: "1m",
      group_by: "service",
    }).toString();
    aggregateChecks.push(send(query, "GET", undefined, queryAgent).then((result) => {
      stage.aggregateLatencies.push(result.latencyMs);
      if (result.status !== 200) stage.aggregateFailures += 1;
    }));
  };

  const scheduleRequest = (stage: StageStats) => {
    const sequence = nextSequence;
    nextSequence += batchSize;
    stage.scheduledRequests += 1;
    inFlight += 1;
    void send(ingestUrl, "POST", logBatch(sequence)).then((result) => {
      inFlight -= 1;
      stage.completedRequests += 1;
      stage.postLatencies.push(result.latencyMs);
      if (result.status !== 200) {
        stage.failedRequests += 1;
        return;
      }
      try {
        const body = JSON.parse(result.body) as { accepted?: number; rejected?: unknown[] };
        const accepted = body.accepted ?? 0;
        const rejected = body.rejected?.length ?? 0;
        stage.acceptedLogs += accepted;
        stage.rejectedLogs += rejected;
        if (accepted !== batchSize || rejected !== 0) {
          stage.failedRequests += 1;
          return;
        }
        stage.successfulPosts += 1;
        const elapsed = performance.now() - startedAt;
        if (elapsed >= nextReadAfterWriteAt) {
          nextReadAfterWriteAt = (Math.floor(elapsed / readAfterWriteIntervalMs) + 1) * readAfterWriteIntervalMs;
          trackVisibility({ sequence, createdAt: Date.now(), stage });
        }
      } catch {
        stage.failedRequests += 1;
      }
    });
  };

  const offerDueRequests = (elapsedMs: number) => {
    for (let index = 0; index < stats.length; index += 1) {
      const stageStart = index === 0 ? 0 : stageBoundaries[index - 1];
      const stageEnd = stageBoundaries[index];
      const observedMs = Math.max(0, Math.min(elapsedMs, stageEnd) - stageStart);
      const expectedOffers = Math.floor(observedMs / 1_000 * stats[index].targetLogsPerSecond / batchSize);
      const due = expectedOffers - stats[index].offeredRequests;
      if (due <= 0) continue;
      stats[index].offeredRequests += due;
      for (let requestNumber = 0; requestNumber < due; requestNumber += 1) {
        if (inFlight >= maxInFlight) stats[index].clientShedRequests += 1;
        else scheduleRequest(stats[index]);
      }
    }
  };

  await new Promise<void>((resolve) => {
    const ticker = setInterval(() => {
      const elapsed = performance.now() - startedAt;
      offerDueRequests(Math.min(elapsed, totalDurationMs));
      const stageIndex = stageIndexAt(Math.min(elapsed, totalDurationMs - 1));
      if (stageIndex >= 0 && elapsed >= nextAggregateAt) {
        nextAggregateAt += aggregateIntervalMs;
        trackAggregate(stats[stageIndex]);
      }
      if (elapsed >= totalDurationMs) {
        clearInterval(ticker);
        resolve();
      }
    }, 5);
  });

  const drainDeadline = Date.now() + requestTimeoutMs;
  while (inFlight > 0 && Date.now() < drainDeadline) await new Promise((resolve) => setTimeout(resolve, 25));
  await Promise.all(visibilityChecks);
  await Promise.all(aggregateChecks);
  ingestAgent.destroy();
  queryAgent.destroy();

  console.log(JSON.stringify({
    scenario: scenarioName,
    batchSize,
    maxInFlight,
    stages: stats.map((stage) => ({
      name: stage.name,
      targetLogsPerSecond: stage.targetLogsPerSecond,
      durationSeconds: stage.durationSeconds,
      offeredLogs: stage.offeredRequests * batchSize,
      clientShedRequests: stage.clientShedRequests,
      scheduledRequests: stage.scheduledRequests,
      completedRequests: stage.completedRequests,
      successfulPosts: stage.successfulPosts,
      failedRequests: stage.failedRequests,
      acceptedLogs: stage.acceptedLogs,
      rejectedLogs: stage.rejectedLogs,
      achievedLogsPerSecond: Number((stage.acceptedLogs / stage.durationSeconds).toFixed(2)),
      postSuccessRate: stage.completedRequests === 0 ? 0 : Number((stage.successfulPosts / stage.completedRequests * 100).toFixed(2)),
      postLatencyMs: { p50: percentile(stage.postLatencies, 0.5), p95: percentile(stage.postLatencies, 0.95) },
      aggregate: { requests: stage.aggregateLatencies.length, failures: stage.aggregateFailures, p95Ms: percentile(stage.aggregateLatencies, 0.95) },
      readAfterWrite: { visibleSamples: stage.visibleSamples, missingSamples: stage.missingSamples, p95Ms: percentile(stage.visibilityLatencies, 0.95) },
    })),
  }, null, 2));
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
