import { execFile } from "node:child_process";
import { createHash } from "node:crypto";
import { promisify } from "node:util";
import postgres from "postgres";

const execFileAsync = promisify(execFile);
const baseUrl = (process.env.BASE_URL ?? "http://127.0.0.1:8080").replace(/\/$/, "");
const databaseUrl = process.env.DATABASE_URL ?? "postgres://postgres:postgres@127.0.0.1:5432/logging";
const tenantAKey = process.env.TENANT_A_KEY ?? "optional-tenant-a-key";
const tenantBKey = process.env.TENANT_B_KEY ?? "optional-tenant-b-key";
const ingestOnlyKey = process.env.INGEST_ONLY_KEY ?? "optional-ingest-only-key";

const testFixtures = [
  { key: tenantAKey, tenantId: "tenant-a", scopes: ["ingest", "query"] },
  { key: tenantBKey, tenantId: "tenant-b", scopes: ["ingest", "query"] },
  { key: ingestOnlyKey, tenantId: "tenant-ingest-only", scopes: ["ingest"] },
] as const;

type TestFixture = {
  seed(): Promise<void>;
  cleanup(): Promise<void>;
  close(): Promise<void>;
};

function assert(value: unknown, message: string): asserts value {
  if (!value) throw new Error(message);
}

function hash(value: string) {
  return createHash("sha256").update(value).digest("hex");
}

const headers = (key: string) => ({ authorization: `Bearer ${key}`, "content-type": "application/json" });

function secondBoundary(offsetMs = 0) {
  const date = new Date(Date.now() + offsetMs);
  date.setMilliseconds(0);
  return date;
}

async function ingestLog(apiKey: string, service: string, timestamp: string, message: string) {
  const response = await fetch(`${baseUrl}/logs`, {
    method: "POST",
    headers: headers(apiKey),
    body: JSON.stringify({ logs: [{ timestamp, level: "info", service, message }] }),
  });
  assert(response.status === 200, `${message}: ingestion failed with ${response.status}`);
}

async function aggregateCount(apiKey: string, service: string, since: string, until: string, label: string) {
  const response = await fetch(
    `${baseUrl}/logs/aggregate?since=${encodeURIComponent(since)}&until=${encodeURIComponent(until)}&bucket=1m&group_by=service&service=${encodeURIComponent(service)}`,
    { headers: headers(apiKey) },
  );
  const body = await response.json() as { buckets?: Array<{ count: number }> };
  assert(response.status === 200, `${label}: aggregate returned ${response.status}`);
  return body.buckets?.reduce((sum, bucket) => sum + bucket.count, 0) ?? 0;
}

function fixtureInsertSql() {
  const values = testFixtures.map(({ key, tenantId, scopes }) =>
    `('${hash(key)}', '${tenantId}', ARRAY[${scopes.map((scope) => `'${scope}'`).join(", ")}])`,
  ).join(",\n");
  return `
    INSERT INTO api_keys (key_hash, tenant_id, scopes)
    VALUES ${values}
    ON CONFLICT (key_hash) DO UPDATE
    SET tenant_id = EXCLUDED.tenant_id, scopes = EXCLUDED.scopes;
  `;
}

function fixtureDeleteSql() {
  const keyHashes = testFixtures.map(({ key }) => `'${hash(key)}'`).join(", ");
  return `DELETE FROM api_keys WHERE key_hash IN (${keyHashes});`;
}

async function runDockerPsql(sql: string) {
  await execFileAsync("docker", ["compose", "exec", "-T", "postgres", "psql", "-U", "postgres", "-d", "logging", "-v", "ON_ERROR_STOP=1", "-c", sql], {
    cwd: process.cwd(),
  });
}

async function createPostgresFixture(): Promise<TestFixture> {
  const sql = postgres(databaseUrl, { max: 1, connect_timeout: 3 });
  await sql`SELECT 1`;
  return {
    async seed() {
      for (const fixture of testFixtures) {
        await sql`
          INSERT INTO api_keys (key_hash, tenant_id, scopes)
          VALUES (${hash(fixture.key)}, ${fixture.tenantId}, ${sql.array([...fixture.scopes])})
          ON CONFLICT (key_hash) DO UPDATE
          SET tenant_id = EXCLUDED.tenant_id, scopes = EXCLUDED.scopes
        `;
      }
    },
    async cleanup() {
      const keyHashes = testFixtures.map(({ key }) => hash(key));
      await sql`DELETE FROM api_keys WHERE key_hash IN ${sql(keyHashes)}`;
    },
    async close() {
      await sql.end({ timeout: 5 });
    },
  };
}

async function createDockerFixture(): Promise<TestFixture> {
  return {
    seed: () => runDockerPsql(fixtureInsertSql()),
    cleanup: () => runDockerPsql(fixtureDeleteSql()),
    close: async () => {},
  };
}

async function createTestFixture(): Promise<TestFixture> {
  try {
    return await createPostgresFixture();
  } catch {
    return createDockerFixture();
  }
}

async function main() {
  const fixture = await createTestFixture();
  await fixture.seed();

  try {
    const service = `auth-tenant-${Date.now()}`;
    const timestamp = new Date().toISOString();
    const unauthorized = await fetch(`${baseUrl}/logs`);
    assert(unauthorized.status === 401, "authentication did not reject an anonymous logs request");
    const protectedMetrics = await fetch(`${baseUrl}/metrics`);
    assert(protectedMetrics.status === 401, "authentication did not protect metrics");
    const protectedDashboard = await fetch(`${baseUrl}/dashboard`);
    assert(protectedDashboard.status === 401, "authentication did not protect dashboard");
    const ingest = await fetch(`${baseUrl}/logs`, {
      method: "POST",
      headers: headers(tenantAKey),
      body: JSON.stringify({ logs: [{ timestamp, level: "info", service, message: "tenant A only" }] }),
    });
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

    const anchor = secondBoundary();
    const leadingService = `auth-leading-${Date.now()}`;
    const leadingTimestamp = new Date(anchor.getTime() + 500).toISOString();
    const leadingSince = new Date(anchor.getTime() + 100).toISOString();
    const leadingUntil = new Date(anchor.getTime() + 2_000).toISOString();
    await ingestLog(tenantAKey, leadingService, leadingTimestamp, "leading raw edge");
    assert(
      await aggregateCount(tenantAKey, leadingService, leadingSince, leadingUntil, "leading raw edge") === 1,
      "tenant A leading raw edge aggregate did not include its log",
    );

    const trailingService = `auth-trailing-${Date.now()}`;
    const trailingTimestamp = new Date(anchor.getTime() + 1_200).toISOString();
    const trailingSince = anchor.toISOString();
    const trailingUntil = new Date(anchor.getTime() + 1_500).toISOString();
    await ingestLog(tenantAKey, trailingService, trailingTimestamp, "trailing raw edge");
    assert(
      await aggregateCount(tenantAKey, trailingService, trailingSince, trailingUntil, "trailing raw edge") === 1,
      "tenant A trailing raw edge aggregate did not include its log",
    );

    const subSecondService = `auth-subsecond-${Date.now()}`;
    const subSecondTimestamp = new Date(anchor.getTime() + 500).toISOString();
    const subSecondSince = new Date(anchor.getTime() + 100).toISOString();
    const subSecondUntil = new Date(anchor.getTime() + 800).toISOString();
    await ingestLog(tenantAKey, subSecondService, subSecondTimestamp, "sub-second raw");
    assert(
      await aggregateCount(tenantAKey, subSecondService, subSecondSince, subSecondUntil, "sub-second raw") === 1,
      "tenant A sub-second raw aggregate did not include its log",
    );

    const noQuery = await fetch(`${baseUrl}/logs`, { headers: headers(ingestOnlyKey) });
    assert(noQuery.status === 403, "scope enforcement did not reject a query-only violation");
    const health = await fetch(`${baseUrl}/health`);
    assert(health.status === 200, "health endpoint must remain public");
    console.log("Authentication and tenant-isolation integration test passed.");
  } finally {
    await fixture.cleanup();
    await fixture.close();
  }
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
