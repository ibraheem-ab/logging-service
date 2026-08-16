import http from "k6/http";
import { check, sleep } from "k6";
import { scenario } from "k6/execution";
import { Counter, Rate, Trend } from "k6/metrics";

const baseUrl = (__ENV.BASE_URL || "http://127.0.0.1:8080").replace(/\/$/, "");
const maximumBatchSize = Number(__ENV.BATCH_SIZE || 32);
const targetLogsPerSecond = Number(__ENV.TARGET_LOGS_PER_SECOND || 15_000);
const duration = __ENV.DURATION || "2m";
const requestTimeout = __ENV.REQUEST_TIMEOUT || "10s";
const benchmarkRun = __ENV.BENCHMARK_RUN || "local-run";
const cursorLimit = Number(__ENV.CURSOR_LIMIT || 1_000);
const filteredQueryIntervalSeconds = Number(__ENV.FILTERED_QUERY_INTERVAL_SECONDS || 1);
const rawProbeIntervalSeconds = Number(__ENV.RAW_PROBE_INTERVAL_SECONDS || 5);
const rawProbeVisibilityTimeoutMilliseconds = Number(__ENV.RAW_PROBE_VISIBILITY_TIMEOUT_MS || 20_000);

function durationSeconds(value) {
  const match = /^(\d+(?:\.\d+)?)(ms|s|m|h)$/.exec(value);
  if (!match) throw new Error(`Unsupported DURATION value: ${value}`);
  const multipliers = { ms: 0.001, s: 1, m: 60, h: 3_600 };
  return Number(match[1]) * multipliers[match[2]];
}

if (!Number.isInteger(maximumBatchSize) || maximumBatchSize < 1) {
  throw new Error("BATCH_SIZE must be a positive integer");
}
if (!Number.isInteger(targetLogsPerSecond) || targetLogsPerSecond < 1) {
  throw new Error("TARGET_LOGS_PER_SECOND must be a positive integer");
}
if (typeof benchmarkRun !== "string" || benchmarkRun.trim().length === 0) {
  throw new Error("BENCHMARK_RUN must be a non-empty string");
}
if (!Number.isInteger(cursorLimit) || cursorLimit < 1 || cursorLimit > 10_000) {
  throw new Error("CURSOR_LIMIT must be an integer between 1 and 10000");
}
if (!Number.isInteger(filteredQueryIntervalSeconds) || filteredQueryIntervalSeconds < 1) {
  throw new Error("FILTERED_QUERY_INTERVAL_SECONDS must be a positive integer");
}
if (!Number.isInteger(rawProbeIntervalSeconds) || rawProbeIntervalSeconds < 1) {
  throw new Error("RAW_PROBE_INTERVAL_SECONDS must be a positive integer");
}
if (!Number.isInteger(rawProbeVisibilityTimeoutMilliseconds) || rawProbeVisibilityTimeoutMilliseconds < 1) {
  throw new Error("RAW_PROBE_VISIBILITY_TIMEOUT_MS must be a positive integer");
}

const durationInSeconds = durationSeconds(duration);
const requestTimeoutMilliseconds = durationSeconds(requestTimeout) * 1_000;
const requestRate = Math.ceil(targetLogsPerSecond / maximumBatchSize);
const smallerBatchSize = Math.floor(targetLogsPerSecond / requestRate);
const largerBatchCount = targetLogsPerSecond - smallerBatchSize * requestRate;
const expectedIngestRequests = requestRate * durationInSeconds;
const expectedAcceptedLogs = targetLogsPerSecond * durationInSeconds;
const expectedAggregateRequests = durationInSeconds;
const minimumFilteredQueries = Math.floor(durationInSeconds / filteredQueryIntervalSeconds);
// K6 can include or exclude an iteration exactly on the duration boundary,
// depending on scheduler timing. This is the guaranteed minimum; zero dropped
// iterations verifies that no scheduled work was silently shed.
const minimumRawProbes = Math.floor(durationInSeconds / rawProbeIntervalSeconds);

if (
  !Number.isInteger(durationInSeconds)
  || !Number.isInteger(expectedIngestRequests)
  || !Number.isInteger(expectedAcceptedLogs)
  || !Number.isInteger(minimumFilteredQueries)
  || !Number.isInteger(minimumRawProbes)
) {
  throw new Error("DURATION must be a whole number of seconds and align with query probe intervals");
}
if (!Number.isFinite(requestTimeoutMilliseconds) || requestTimeoutMilliseconds < 1) {
  throw new Error("REQUEST_TIMEOUT must be a positive duration");
}

// Pre-allocate enough sender capacity that k6 does not silently shed offered
// load while creating more VUs. These affect only the local load generator,
// never the service under test.
const maxVUs = Number(
  __ENV.MAX_VUS || Math.max(1_500, Math.ceil(requestRate * 3.2)),
);
const preAllocatedVUs = Number(
  __ENV.PRE_ALLOCATED_VUS || maxVUs,
);

if (!Number.isInteger(preAllocatedVUs) || preAllocatedVUs < 1) {
  throw new Error("PRE_ALLOCATED_VUS must be a positive integer");
}
if (!Number.isInteger(maxVUs) || maxVUs < preAllocatedVUs) {
  throw new Error("MAX_VUS must be an integer greater than or equal to PRE_ALLOCATED_VUS");
}

const acceptedLogs = new Counter("accepted_logs");
const completedIngestRequests = new Counter("completed_ingest_requests");
const rejectedLogs = new Counter("rejected_logs");
const postSuccess = new Rate("post_success");
const aggregateSuccess = new Rate("aggregate_success");
const completedAggregateRequests = new Counter("completed_aggregate_requests");
const aggregateDuration = new Trend("aggregate_duration", true);
const rawProbeSuccess = new Rate("raw_probe_success");
const completedRawProbes = new Counter("completed_raw_probes");
const rawProbeDuration = new Trend("raw_probe_duration", true);
const filteredPageSuccess = new Rate("filtered_page_success");
const completedFilteredPageQueries = new Counter("completed_filtered_page_queries");
const filteredPageDuration = new Trend("filtered_page_duration", true);
const visibleLogs = new Counter("visible_logs");
const cursorDrainSuccess = new Rate("cursor_drain_success");
const cursorDrainDuration = new Trend("cursor_drain_duration", true);
const cursorDrainPages = new Counter("cursor_drain_pages");

export const options = {
  teardownTimeout: "35s",
  scenarios: {
    ingest: {
      executor: "constant-arrival-rate",
      exec: "ingest",
      rate: requestRate,
      timeUnit: "1s",
      duration,
      preAllocatedVUs,
      maxVUs,
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
    raw_probe: {
      executor: "constant-arrival-rate",
      exec: "readAfterWrite",
      rate: 1,
      timeUnit: `${rawProbeIntervalSeconds}s`,
      duration,
      preAllocatedVUs: 4,
      maxVUs: 5,
      gracefulStop: "30s",
    },
    filtered_page: {
      executor: "constant-arrival-rate",
      exec: "filteredPage",
      rate: 1,
      timeUnit: `${filteredQueryIntervalSeconds}s`,
      duration,
      preAllocatedVUs: 2,
      maxVUs: 10,
      gracefulStop: "30s",
    },
  },
  thresholds: {
    accepted_logs: [`count>=${expectedAcceptedLogs}`],
    completed_ingest_requests: [`count>=${expectedIngestRequests}`],
    completed_aggregate_requests: [`count>=${expectedAggregateRequests}`],
    completed_filtered_page_queries: [`count>=${minimumFilteredQueries}`],
    completed_raw_probes: [`count>=${minimumRawProbes}`],
    post_success: ["rate==1"],
    aggregate_success: ["rate==1"],
    aggregate_duration: ["p(95)<1000"],
    raw_probe_success: ["rate==1"],
    raw_probe_duration: [`max<${rawProbeVisibilityTimeoutMilliseconds}`],
    filtered_page_success: ["rate==1"],
    filtered_page_duration: ["p(95)<1000"],
    cursor_drain_duration: ["max<30000"],
    cursor_drain_success: ["rate==1"],
    visible_logs: [`count>=${expectedAcceptedLogs}`],
    "http_req_failed{endpoint:ingest}": ["rate==0"],
    "http_req_failed{endpoint:aggregate}": ["rate==0"],
    "http_req_failed{endpoint:filtered_page}": ["rate==0"],
    "http_req_failed{endpoint:cursor_drain}": ["rate==0"],
    checks: ["rate==1"],
    "dropped_iterations{scenario:ingest}": ["count==0"],
    "dropped_iterations{scenario:aggregate}": ["count==0"],
    "dropped_iterations{scenario:raw_probe}": ["count==0"],
    "dropped_iterations{scenario:filtered_page}": ["count==0"],
  },
};

// A k6 arrival-rate scheduler requires an integer number of requests per
// second. Vary only a few 32-log batches to 31 logs, so 469 POSTs/second
// equals exactly 15,000 logs/second rather than 15,008.
function batchSizeForIteration(iteration) {
  const positionWithinSecond = iteration % requestRate;
  return positionWithinSecond < largerBatchCount ? smallerBatchSize + 1 : smallerBatchSize;
}

function logOffsetForIteration(iteration) {
  const completedSeconds = Math.floor(iteration / requestRate);
  const positionWithinSecond = iteration % requestRate;
  return completedSeconds * targetLogsPerSecond
    + positionWithinSecond * smallerBatchSize
    + Math.min(positionWithinSecond, largerBatchCount);
}

function batch(iteration) {
  const timestamp = new Date().toISOString();
  const count = batchSizeForIteration(iteration);
  const start = logOffsetForIteration(iteration);
  return {
    count,
    payload: JSON.stringify({
      logs: Array.from({ length: count }, (_, offset) => ({
        timestamp,
        level: offset % 10 === 0 ? "error" : "info",
        service: offset % 2 === 0 ? "checkout" : "auth",
        message: `k6-local-${start + offset}`,
        attributes: {
          region: "eu-west",
          sequence: start + offset,
          synthetic: true,
          benchmark_run: benchmarkRun,
        },
      })),
    }),
  };
}

function parseJson(response) {
  try {
    return response.json();
  } catch {
    return null;
  }
}

function hasExactKeys(value, keys) {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const actual = Object.keys(value).sort();
  const expected = [...keys].sort();
  return actual.length === expected.length && actual.every((key, index) => key === expected[index]);
}

function isPublicLog(log) {
  return hasExactKeys(log, ["id", "timestamp", "level", "service", "message", "attributes"])
    && typeof log.id === "string"
    && typeof log.timestamp === "string"
    && Number.isFinite(Date.parse(log.timestamp))
    && typeof log.level === "string"
    && typeof log.service === "string"
    && typeof log.message === "string"
    && log.attributes !== null
    && typeof log.attributes === "object"
    && !Array.isArray(log.attributes);
}

function orderedAfterOrEqual(previous, current) {
  const previousTimestamp = Date.parse(previous.timestamp);
  const currentTimestamp = Date.parse(current.timestamp);
  if (currentTimestamp < previousTimestamp) return true;
  return currentTimestamp === previousTimestamp && current.id < previous.id;
}

function filteredLogsUrl(cursor = null, extraAttributes = {}) {
  // k6's JavaScript runtime does not provide URLSearchParams, so construct
  // the small, fixed query string explicitly while still encoding all values.
  const query = [
    `attr.benchmark_run=${encodeURIComponent(benchmarkRun)}`,
    `limit=${encodeURIComponent(String(cursorLimit))}`,
  ];
  for (const [key, value] of Object.entries(extraAttributes)) {
    query.push(`attr.${encodeURIComponent(key)}=${encodeURIComponent(String(value))}`);
  }
  if (cursor) query.push(`cursor=${encodeURIComponent(cursor)}`);
  return `${baseUrl}/logs?${query.join("&")}`;
}

function isTaggedPublicLog(log) {
  return isPublicLog(log) && log.attributes.benchmark_run === benchmarkRun;
}

function hasPageEnvelope(body) {
  return hasExactKeys(body, ["logs", "next_cursor"])
    && Array.isArray(body.logs)
    && (body.next_cursor === null || (typeof body.next_cursor === "string" && body.next_cursor.length > 0))
    && !(body.logs.length === 0 && body.next_cursor !== null);
}

export function setup() {
  const response = http.get(filteredLogsUrl(), { timeout: requestTimeout });
  const body = parseJson(response);
  if (response.status !== 200 || !hasPageEnvelope(body)) {
    throw new Error(`Cannot verify benchmark-run isolation: GET /logs returned HTTP ${response.status}`);
  }
  if (body.logs.length !== 0) {
    throw new Error(
      `BENCHMARK_RUN=${benchmarkRun} already exists. Choose a new run tag; retained earlier data is expected.`,
    );
  }
  return { expectedAcceptedLogs, benchmarkRun, cursorLimit };
}

export function ingest() {
  const request = batch(scenario.iterationInTest);
  const response = http.post(`${baseUrl}/logs`, request.payload, {
    headers: { "content-type": "application/json" },
    timeout: requestTimeout,
    tags: { endpoint: "ingest" },
  });

  const body = parseJson(response);
  completedIngestRequests.add(1);
  const ok = response.status === 200
    && Number(body?.accepted) === request.count
    && Array.isArray(body?.rejected)
    && body.rejected.length === 0;
  postSuccess.add(ok);
  if (ok) {
    acceptedLogs.add(request.count);
  } else if (Array.isArray(body?.rejected)) {
    rejectedLogs.add(body.rejected.length);
  }
  check(response, { "POST /logs accepts the complete batch": () => ok });
}

export function aggregate() {
  const now = Date.now();
  const response = http.get(
    `${baseUrl}/logs/aggregate?since=${encodeURIComponent(new Date(now - 60_000).toISOString())}&until=${encodeURIComponent(new Date(now + 60_000).toISOString())}&bucket=1m&group_by=service`,
    { timeout: requestTimeout, tags: { endpoint: "aggregate" } },
  );
  const body = parseJson(response);
  const ok = response.status === 200 && Array.isArray(body?.buckets);
  completedAggregateRequests.add(1);
  aggregateDuration.add(response.timings.duration);
  aggregateSuccess.add(ok);
  check(response, { "GET /logs/aggregate returns 200": () => ok });
}

// Probe the oldest known log from this run while new logs keep arriving. A
// timestamp-order plan would scan a growing number of rows to find it; the
// service's selective attr.* path should locate it quickly through the GIN
// index. No extra probe writes are needed, so the final drain count remains
// exactly the scheduled ingestion total.
export function readAfterWrite() {
  const startedAt = Date.now();
  const deadline = startedAt + rawProbeVisibilityTimeoutMilliseconds;
  let found = false;

  while (Date.now() < deadline) {
    const remainingMilliseconds = Math.max(1, deadline - Date.now());
    const timeout = `${Math.min(5_000, requestTimeoutMilliseconds, remainingMilliseconds)}ms`;
    const response = http.get(filteredLogsUrl(null, { sequence: 0 }), {
      timeout,
      tags: { endpoint: "read_after_write" },
    });
    const body = parseJson(response);
    const firstLog = body?.logs?.[0];
    if (
      response.status === 200
      && hasExactKeys(body, ["logs", "next_cursor"])
      && isTaggedPublicLog(firstLog)
      && Number(firstLog.attributes.sequence) === 0
      && Date.now() <= deadline
    ) {
      found = true;
      break;
    }
    sleep(0.1);
  }

  completedRawProbes.add(1);
  rawProbeDuration.add(Date.now() - startedAt);
  rawProbeSuccess.add(found);
  check({ found }, { "read-after-write finds attr.sequence=0 within 20 seconds": (result) => result.found });
}

// Keep a selective, explicitly limited query active while writes saturate the
// database. The evaluator can send any supported limit explicitly, so this
// path must not rely on the server default.
export function filteredPage() {
  const response = http.get(filteredLogsUrl(), {
    timeout: requestTimeout,
    tags: { endpoint: "filtered_page" },
  });
  const body = parseJson(response);
  const firstLog = body?.logs?.[0];
  const lastLog = body?.logs?.[body.logs.length - 1];
  const ok = response.status === 200
    && hasPageEnvelope(body)
    && (!firstLog || isTaggedPublicLog(firstLog))
    && (!lastLog || isTaggedPublicLog(lastLog));
  completedFilteredPageQueries.add(1);
  filteredPageDuration.add(response.timings.duration);
  filteredPageSuccess.add(ok);
  check(response, { "filtered GET /logs returns the tagged page while ingesting": () => ok });
}

// Runs once after ingestion. It retains the filter and explicit limit on every
// cursor page, mirroring a client that does not rely on server-side defaults.
export function teardown(data) {
  const startedAt = Date.now();
  const deadline = startedAt + 30_000;
  const seenCursors = new Set();
  let cursor = null;
  let previousLog = null;
  let visible = 0;
  let pages = 0;

  function finish(success, message) {
    visibleLogs.add(visible);
    cursorDrainPages.add(pages);
    cursorDrainDuration.add(Date.now() - startedAt);
    cursorDrainSuccess.add(success);
    (success ? console.log : console.error)(message);
  }

  do {
    if (Date.now() >= deadline) {
      finish(false, `cursor drain timed out after ${visible} visible logs across ${pages} pages`);
      return;
    }
    if (cursor && seenCursors.has(cursor)) {
      finish(false, "cursor drain received a repeated cursor");
      return;
    }
    if (cursor) seenCursors.add(cursor);

    const url = filteredLogsUrl(cursor);
    const remainingMilliseconds = Math.max(1, deadline - Date.now());
    const drainRequestTimeout = `${Math.min(requestTimeoutMilliseconds, remainingMilliseconds)}ms`;
    const response = http.get(url, { timeout: drainRequestTimeout, tags: { endpoint: "cursor_drain" } });
    if (response.status !== 200) {
      finish(false, `cursor drain returned HTTP ${response.status}`);
      return;
    }

    const body = parseJson(response);
    if (!hasPageEnvelope(body)) {
      finish(false, "cursor drain received an invalid response shape");
      return;
    }
    if (body.logs.length > 0) {
      const firstLog = body.logs[0];
      const lastLog = body.logs[body.logs.length - 1];
      if (
        !isTaggedPublicLog(firstLog)
        || !isTaggedPublicLog(lastLog)
        || (previousLog && !orderedAfterOrEqual(previousLog, firstLog))
        || (body.logs.length > 1 && !orderedAfterOrEqual(firstLog, lastLog))
      ) {
        finish(false, "cursor drain received invalid or non-descending log rows");
        return;
      }
      previousLog = lastLog;
    }

    if (Date.now() > deadline) {
      finish(false, `cursor drain exceeded its 30-second deadline after ${visible} visible logs`);
      return;
    }

    visible += body.logs.length;
    cursor = body.next_cursor;
    pages += 1;
  } while (cursor);

  if (visible < data.expectedAcceptedLogs) {
    finish(false, `cursor drain count too low: visible=${visible}, expected-at-least=${data.expectedAcceptedLogs}`);
    return;
  }
  finish(true, `cursor drain completed: ${visible} visible logs across ${pages} pages`);
}

function metricCount(data, name) {
  return Number(data.metrics?.[name]?.values?.count || 0);
}

function metricRate(data, name) {
  return Number(data.metrics?.[name]?.values?.rate || 0);
}

function allThresholdsPassed(data) {
  return Object.values(data.metrics || {}).every((metric) =>
    Object.values(metric.thresholds || {}).every((threshold) => threshold.ok),
  );
}

export function handleSummary(data) {
  const accepted = metricCount(data, "accepted_logs");
  const completed = metricCount(data, "completed_ingest_requests");
  const aggregates = metricCount(data, "completed_aggregate_requests");
  const filteredPages = metricCount(data, "completed_filtered_page_queries");
  const rawProbes = metricCount(data, "completed_raw_probes");
  const visible = metricCount(data, "visible_logs");
  const dropped = metricCount(data, "dropped_iterations");
  const drainPassed = metricRate(data, "cursor_drain_success") === 1;
  const passed = allThresholdsPassed(data)
    && accepted >= expectedAcceptedLogs
    && completed >= expectedIngestRequests
    && aggregates >= expectedAggregateRequests
    && filteredPages >= minimumFilteredQueries
    && rawProbes >= minimumRawProbes
    && visible === accepted
    && dropped === 0
    && drainPassed;
  const lines = [
    "",
    "============================================================",
    " Local query-drain verification",
    "============================================================",
    ` Result:                     ${passed ? "PASS" : "FAIL"}`,
    ` Target:                     ${targetLogsPerSecond} logs/s for ${duration}`,
    ` Minimum accepted logs:      ${expectedAcceptedLogs}`,
    ` Accepted logs:              ${accepted}`,
    ` Completed ingest POSTs:     ${completed} / at least ${expectedIngestRequests}`,
    ` Completed aggregate GETs:   ${aggregates} / at least ${expectedAggregateRequests}`,
    ` Tagged page GETs:           ${filteredPages} / at least ${minimumFilteredQueries}`,
    ` Rare-attribute probes:      ${rawProbes} / at least ${minimumRawProbes}`,
    ` Actual ingestion window:    ${(accepted / durationInSeconds).toFixed(2)} logs/s`,
    ` Dropped ingest iterations:  ${dropped}`,
    ` Visible after cursor drain: ${visible}`,
    ` Cursor drain:               ${drainPassed ? "PASS" : "FAIL"}`,
    "============================================================",
    "",
  ];
  return { stdout: `${lines.join("\n")}\n` };
}
