import http from "k6/http";
import { check } from "k6";
import { scenario } from "k6/execution";
import { Counter, Rate, Trend } from "k6/metrics";

const baseUrl = (__ENV.BASE_URL || "http://127.0.0.1:8080").replace(/\/$/, "");
const batchSize = Number(__ENV.BATCH_SIZE || 32);
const targetLogsPerSecond = Number(__ENV.TARGET_LOGS_PER_SECOND || 15_000);
const duration = __ENV.DURATION || "2m";
const requestRate = Math.ceil(targetLogsPerSecond / batchSize);
const requestTimeout = __ENV.REQUEST_TIMEOUT || "10s";

const acceptedLogs = new Counter("accepted_logs");
const rejectedLogs = new Counter("rejected_logs");
const postSuccess = new Rate("post_success");
const aggregateDuration = new Trend("aggregate_duration", true);

export const options = {
  scenarios: {
    ingest: {
      executor: "constant-arrival-rate",
      exec: "ingest",
      rate: requestRate,
      timeUnit: "1s",
      duration,
      preAllocatedVUs: Number(__ENV.PRE_ALLOCATED_VUS || 100),
      maxVUs: Number(__ENV.MAX_VUS || 1_000),
      gracefulStop: "30s",
    },
    aggregate: {
      executor: "constant-arrival-rate",
      exec: "aggregate",
      rate: 1,
      timeUnit: "1s",
      duration,
      preAllocatedVUs: 2,
      maxVUs: 10,
      gracefulStop: "30s",
    },
  },
  thresholds: {
    post_success: ["rate==1"],
    http_req_failed: ["rate<0.01"],
  },
};

function batch(iteration) {
  const timestamp = new Date().toISOString();
  const start = iteration * batchSize;
  return JSON.stringify({
    logs: Array.from({ length: batchSize }, (_, offset) => ({
      timestamp,
      level: offset % 10 === 0 ? "error" : "info",
      service: offset % 2 === 0 ? "checkout" : "auth",
      message: `k6-local-${start + offset}`,
      attributes: { region: "eu-west", sequence: start + offset, synthetic: true },
    })),
  });
}

export function ingest() {
  const response = http.post(`${baseUrl}/logs`, batch(scenario.iterationInTest), {
    headers: { "content-type": "application/json" },
    timeout: requestTimeout,
    tags: { endpoint: "ingest" },
  });

  const ok = check(response, { "POST /logs returns 200": (result) => result.status === 200 });
  postSuccess.add(ok);
  if (response.status !== 200) return;

  try {
    const body = response.json();
    acceptedLogs.add(Number(body.accepted || 0));
    rejectedLogs.add(Array.isArray(body.rejected) ? body.rejected.length : 0);
  } catch {
    postSuccess.add(false);
  }
}

export function aggregate() {
  const now = Date.now();
  const response = http.get(
    `${baseUrl}/logs/aggregate?since=${encodeURIComponent(new Date(now - 60_000).toISOString())}&until=${encodeURIComponent(new Date(now + 60_000).toISOString())}&bucket=1m&group_by=service`,
    { timeout: requestTimeout, tags: { endpoint: "aggregate" } },
  );
  aggregateDuration.add(response.timings.duration);
  check(response, { "GET /logs/aggregate returns 200": (result) => result.status === 200 });
}

// Runs once after ingestion. It intentionally omits limit so this validates
// the service's default cursor-page policy used by a generic client.
export function teardown() {
  const deadline = Date.now() + 30_000;
  let cursor = null;
  let visible = 0;
  let pages = 0;

  do {
    if (Date.now() >= deadline) {
      console.error(`cursor drain timed out after ${visible} visible logs across ${pages} pages`);
      return;
    }
    const url = cursor ? `${baseUrl}/logs?cursor=${encodeURIComponent(cursor)}` : `${baseUrl}/logs`;
    const response = http.get(url, { timeout: requestTimeout, tags: { endpoint: "cursor_drain" } });
    if (response.status !== 200) {
      console.error(`cursor drain returned HTTP ${response.status}`);
      return;
    }
    try {
      const body = response.json();
      if (!Array.isArray(body.logs) || !(body.next_cursor === null || typeof body.next_cursor === "string")) {
        console.error("cursor drain received an invalid response shape");
        return;
      }
      visible += body.logs.length;
      cursor = body.next_cursor;
      pages += 1;
    } catch {
      console.error("cursor drain returned invalid JSON");
      return;
    }
  } while (cursor);

  console.log(`cursor drain completed: ${visible} visible logs across ${pages} pages`);
}
