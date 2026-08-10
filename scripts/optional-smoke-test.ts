const baseUrl = (process.env.BASE_URL ?? "http://127.0.0.1:8080").replace(/\/$/, "");
function assert(condition: unknown, message: string): asserts condition { if (!condition) throw new Error(message); }

async function main() {
  const timestamp = new Date().toISOString();
  const service = `optional-smoke-${Date.now()}`;
  const ingest = await fetch(`${baseUrl}/logs`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ logs: [{ timestamp, level: "error", service, message: "optional query sample", attributes: { region: "eu" } }] }) });
  assert(ingest.status === 200, "optional smoke ingestion failed");
  const query = await fetch(`${baseUrl}/logs?query=${encodeURIComponent(`service:${service} level:error attr.region:eu q:optional`)}`);
  const result = await query.json() as { logs?: unknown[] };
  assert(query.status === 200 && result.logs?.length === 1, "custom query language failed");
  const metrics = await fetch(`${baseUrl}/metrics`);
  assert(metrics.status === 200 && (await metrics.text()).includes("log_service_requests_total"), "metrics endpoint failed");
  const dashboard = await fetch(`${baseUrl}/dashboard`);
  assert(dashboard.status === 200 && (await dashboard.text()).includes("Log Service Dashboard"), "dashboard endpoint failed");
  console.log("Optional feature smoke test passed.");
}
main().catch((error) => { console.error(error); process.exit(1); });
