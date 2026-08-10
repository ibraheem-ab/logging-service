const baseUrl = (process.env.BASE_URL ?? "http://127.0.0.1:8080").replace(/\/$/, "");
function assert(value: unknown, message: string): asserts value { if (!value) throw new Error(message); }

async function main() {
  const service = `optional-enabled-${Date.now()}`;
  const timestamp = new Date().toISOString();
  const stream = await fetch(`${baseUrl}/logs/tail`);
  const streamBody = stream.body;
  assert(stream.status === 200 && streamBody, "live tail did not connect");
  const reader = streamBody.getReader();
  const ready = new TextDecoder().decode((await reader.read()).value);
  assert(ready.includes("event: ready"), "live tail did not send ready event");

  const ingest = await fetch(`${baseUrl}/logs`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ logs: [
    { timestamp, level: "error", service, message: "compressed ".repeat(250), attributes: { region: "eu" } },
    { timestamp, level: "invalid", service, message: "dead letter" },
  ] }) });
  const body = await ingest.json() as { accepted: number; rejected: unknown[] };
  assert(ingest.status === 200 && body.accepted === 1 && body.rejected.length === 1, "ingestion/dead-letter precondition failed");
  const event = new TextDecoder().decode((await reader.read()).value);
  assert(event.includes(service), "live tail did not publish the accepted log");
  await reader.cancel();

  const query = await fetch(`${baseUrl}/logs?query=${encodeURIComponent(`service:${service} level:error attr.region:eu q:compressed`)}`, { headers: { "accept-encoding": "gzip" } });
  const logs = await query.json() as { logs: unknown[] };
  assert(query.status === 200 && logs.logs.length === 1, "custom query failed");
  assert(query.headers.get("content-encoding") === "gzip", "compression was not applied");
  const metrics = await fetch(`${baseUrl}/metrics`);
  assert(metrics.status === 200 && (await metrics.text()).includes("log_service_accepted_logs_total"), "metrics failed");
  const dashboard = await fetch(`${baseUrl}/dashboard`);
  assert(dashboard.status === 200 && (await dashboard.text()).includes("<form"), "dashboard failed");
  console.log("Enabled optional integration test passed.");
}
main().catch((error) => { console.error(error); process.exit(1); });
