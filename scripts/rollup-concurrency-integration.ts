const configuredBaseUrl = process.env.BASE_URL;
if (!configuredBaseUrl) throw new Error("BASE_URL is required for rollup:integration");

const baseUrl = configuredBaseUrl.replace(/\/$/, "");
const requestCount = positiveInteger(process.env.REQUEST_COUNT, 300);
const batchSize = positiveInteger(process.env.BATCH_SIZE, 32);

function positiveInteger(value: string | undefined, fallback: number) {
  if (value === undefined) return fallback;
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) throw new Error(`Expected a positive integer, received ${value}`);
  return parsed;
}

function assert(value: unknown, message: string): asserts value {
  if (!value) throw new Error(message);
}

async function main() {
  const service = `rollup-concurrency-${Date.now()}`;
  const timestamp = new Date(Math.floor((Date.now() - 30_000) / 1_000) * 1_000);
  const body = JSON.stringify({ logs: Array.from({ length: batchSize }, (_, index) => ({
    timestamp: timestamp.toISOString(),
    level: index % 4 === 0 ? "error" : "info",
    service,
    message: `concurrent-rollup-${index}`,
    attributes: { integration: true },
  })) });

  const responses = await Promise.all(Array.from({ length: requestCount }, async () => {
    const response = await fetch(`${baseUrl}/logs`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body,
    });
    const result = await response.json() as { accepted?: number; rejected?: unknown[] };
    assert(response.status === 200, `concurrent POST returned ${response.status}`);
    assert(result.accepted === batchSize && result.rejected?.length === 0, "concurrent POST did not commit its complete batch");
    return result;
  }));
  assert(responses.length === requestCount, "not every concurrent request resolved");

  const since = new Date(timestamp.getTime() - 1_000).toISOString();
  const until = new Date(timestamp.getTime() + 1_000).toISOString();
  const aggregateUrl = new URL(`${baseUrl}/logs/aggregate`);
  aggregateUrl.search = new URLSearchParams({
    since,
    until,
    bucket: "1m",
    group_by: "service",
    service,
  }).toString();
  const aggregate = await fetch(aggregateUrl);
  const bodyResult = await aggregate.json() as { buckets?: Array<{ count: number }> };
  const actual = bodyResult.buckets?.reduce((total, bucket) => total + bucket.count, 0) ?? 0;
  const expected = requestCount * batchSize;
  assert(aggregate.status === 200, `rollup aggregate returned ${aggregate.status}`);
  assert(actual === expected, `rollup count mismatch: expected ${expected}, received ${actual}`);

  console.log(JSON.stringify({ requestCount, batchSize, expected, actual }, null, 2));
  console.log("Concurrent rollup integration test passed.");
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
