const baseUrl = (process.env.BASE_URL ?? "http://127.0.0.1:8080").replace(/\/$/, "");
const authToken = process.env.AUTH_TOKEN;

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message);
}

async function request(path: string, init?: RequestInit) {
  const headers = new Headers(init?.headers);
  if (authToken) headers.set("authorization", `Bearer ${authToken}`);
  return fetch(`${baseUrl}${path}`, { ...init, headers });
}

async function waitForHealthy() {
  let lastError: unknown;
  for (let attempt = 0; attempt < 30; attempt += 1) {
    try {
      const response = await request("/health");
      if (response.ok) return;
      lastError = new Error(`GET /health returned ${response.status}`);
    } catch (error) {
      lastError = error;
    }
    await new Promise((resolve) => setTimeout(resolve, 1_000));
  }
  throw new Error(`Service did not become healthy within 30 seconds: ${String(lastError)}`);
}

async function main() {
  await waitForHealthy();
  if (process.env.AUTH_EXPECTED === "true") {
    const unauthenticated = await fetch(`${baseUrl}/logs`);
    assert(unauthenticated.status === 401, "auth mode must reject requests without credentials");
  } else {
    const ignoredCredential = await fetch(`${baseUrl}/logs`, { headers: { authorization: "Bearer ignored-when-auth-is-disabled" } });
    assert(ignoredCredential.status === 200, "core mode must ignore an Authorization header when authentication is disabled");
  }

  const timestamp = new Date().toISOString();
  const service = `contract-smoke-${Date.now()}`;
  const aggregateSince = new Date(Date.now() - 60_000).toISOString();
  const aggregateUntil = new Date(Date.now() + 60_000).toISOString();
  const ingest = await request("/logs", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ logs: [
      { timestamp, level: "info", service, message: "first", attributes: { region: "eu-west", user_id: "42" } },
      { timestamp, level: "error", service, message: "second", attributes: { region: "eu-west", user_id: "42" } },
      { timestamp, level: "invalid", service, message: "rejected" },
      { timestamp: "2026-02-30T10:00:00Z", level: "info", service, message: "invalid date" },
    ] }),
  });
  assert(ingest.status === 200, `POST /logs returned ${ingest.status}`);
  const ingestion = await ingest.json() as { accepted: number; rejected: Array<{ index: number }> };
  assert(ingestion.accepted === 2 && ingestion.rejected.length === 2 && ingestion.rejected[0].index === 2 && ingestion.rejected[1].index === 3, "batch validation contract failed");

  const firstPage = await request(`/logs?service=${encodeURIComponent(service)}&attr.user_id=42&limit=1`);
  assert(firstPage.status === 200, `GET /logs returned ${firstPage.status}`);
  const first = await firstPage.json() as { logs: Array<{ id: string }>; next_cursor: string | null };
  assert(first.logs.length === 1 && typeof first.next_cursor === "string", "pagination first page contract failed");

  const secondPage = await request(`/logs?service=${encodeURIComponent(service)}&attr.user_id=42&limit=1&cursor=${encodeURIComponent(first.next_cursor)}`);
  assert(secondPage.status === 200, `GET /logs page 2 returned ${secondPage.status}`);
  const second = await secondPage.json() as { logs: Array<{ id: string }> };
  assert(second.logs.length === 1 && second.logs[0].id !== first.logs[0].id, "cursor did not advance to the next record");

  const aggregate = await request(`/logs/aggregate?service=${encodeURIComponent(service)}&attr.user_id=42&since=${encodeURIComponent(aggregateSince)}&until=${encodeURIComponent(aggregateUntil)}&bucket=5m&group_by=service`);
  assert(aggregate.status === 200, `GET /logs/aggregate returned ${aggregate.status}`);
  const aggregation = await aggregate.json() as { buckets: Array<{ group: string | null; count: number }> };
  assert(aggregation.buckets.some((bucket) => bucket.group === service && bucket.count >= 2), "aggregation contract failed");

  const rollupAggregate = await request(`/logs/aggregate?service=${encodeURIComponent(service)}&since=${encodeURIComponent(aggregateSince)}&until=${encodeURIComponent(aggregateUntil)}&bucket=5m&group_by=service`);
  assert(rollupAggregate.status === 200, `rollup GET /logs/aggregate returned ${rollupAggregate.status}`);
  const rollupAggregation = await rollupAggregate.json() as { buckets: Array<{ group: string | null; count: number }> };
  assert(rollupAggregation.buckets.some((bucket) => bucket.group === service && bucket.count >= 2), "rollup aggregation did not include committed logs");

  const invalidQuery = await request("/logs?level=critical");
  assert(invalidQuery.status === 400, "invalid query parameters must return 400");
  const invalidTimestamp = await request("/logs?since=2026-02-30T10%3A00%3A00Z");
  assert(invalidTimestamp.status === 400, "invalid calendar timestamps must return 400");
  const invalidAggregate = await request("/logs/aggregate?bucket=1m");
  assert(invalidAggregate.status === 400, "aggregation must require since and until");
  console.log("Required API contract smoke test passed.");
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
