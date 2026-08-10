const baseUrl = (process.env.BASE_URL ?? "http://127.0.0.1:8080").replace(/\/$/, "");
const tenantAKey = process.env.TENANT_A_KEY ?? "optional-tenant-a-key";
const tenantBKey = process.env.TENANT_B_KEY ?? "optional-tenant-b-key";
const ingestOnlyKey = process.env.INGEST_ONLY_KEY ?? "optional-ingest-only-key";
function assert(value: unknown, message: string): asserts value { if (!value) throw new Error(message); }
const headers = (key: string) => ({ authorization: `Bearer ${key}`, "content-type": "application/json" });

async function main() {
  const service = `auth-tenant-${Date.now()}`;
  const timestamp = new Date().toISOString();
  const unauthorized = await fetch(`${baseUrl}/logs`);
  assert(unauthorized.status === 401, "authentication did not reject an anonymous logs request");
  const protectedMetrics = await fetch(`${baseUrl}/metrics`);
  assert(protectedMetrics.status === 401, "authentication did not protect metrics");
  const protectedDashboard = await fetch(`${baseUrl}/dashboard`);
  assert(protectedDashboard.status === 401, "authentication did not protect dashboard");
  const ingest = await fetch(`${baseUrl}/logs`, { method: "POST", headers: headers(tenantAKey), body: JSON.stringify({ logs: [{ timestamp, level: "info", service, message: "tenant A only" }] }) });
  assert(ingest.status === 200, "tenant A could not ingest");
  const ownLogs = await fetch(`${baseUrl}/logs?service=${encodeURIComponent(service)}`, { headers: headers(tenantAKey) });
  assert(ownLogs.status === 200 && ((await ownLogs.json()) as { logs: unknown[] }).logs.length === 1, "tenant A cannot query its log");
  const foreignLogs = await fetch(`${baseUrl}/logs?service=${encodeURIComponent(service)}`, { headers: headers(tenantBKey) });
  assert(foreignLogs.status === 200 && ((await foreignLogs.json()) as { logs: unknown[] }).logs.length === 0, "tenant isolation failed");
  const since = new Date(Date.now() - 60_000).toISOString();
  const until = new Date(Date.now() + 60_000).toISOString();
  const ownAggregate = await fetch(`${baseUrl}/logs/aggregate?since=${encodeURIComponent(since)}&until=${encodeURIComponent(until)}&bucket=1m&group_by=service&service=${encodeURIComponent(service)}`, { headers: headers(tenantAKey) });
  const ownBuckets = (await ownAggregate.json()) as { buckets: Array<{ count: number }> };
  assert(ownAggregate.status === 200 && ownBuckets.buckets.reduce((sum, bucket) => sum + bucket.count, 0) === 1, "tenant A aggregate did not include its log");
  const foreignAggregate = await fetch(`${baseUrl}/logs/aggregate?since=${encodeURIComponent(since)}&until=${encodeURIComponent(until)}&bucket=1m&group_by=service&service=${encodeURIComponent(service)}`, { headers: headers(tenantBKey) });
  const foreignBuckets = (await foreignAggregate.json()) as { buckets: Array<{ count: number }> };
  assert(foreignAggregate.status === 200 && foreignBuckets.buckets.reduce((sum, bucket) => sum + bucket.count, 0) === 0, "tenant rollup isolation failed");
  const narrowSince = new Date(new Date(timestamp).getTime() - 5).toISOString();
  const narrowUntil = new Date(new Date(timestamp).getTime() + 5).toISOString();
  const narrowForeignAggregate = await fetch(`${baseUrl}/logs/aggregate?since=${encodeURIComponent(narrowSince)}&until=${encodeURIComponent(narrowUntil)}&bucket=1m&group_by=service&service=${encodeURIComponent(service)}`, { headers: headers(tenantBKey) });
  const narrowForeignBuckets = (await narrowForeignAggregate.json()) as { buckets: Array<{ count: number }> };
  assert(narrowForeignAggregate.status === 200 && narrowForeignBuckets.buckets.reduce((sum, bucket) => sum + bucket.count, 0) === 0, "tenant isolation failed for a sub-second raw aggregation");
  const noQuery = await fetch(`${baseUrl}/logs`, { headers: headers(ingestOnlyKey) });
  assert(noQuery.status === 403, "scope enforcement did not reject a query-only violation");
  const health = await fetch(`${baseUrl}/health`);
  assert(health.status === 200, "health endpoint must remain public");
  console.log("Authentication and tenant-isolation integration test passed.");
}
main().catch((error) => { console.error(error); process.exit(1); });
