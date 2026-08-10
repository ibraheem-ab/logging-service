const baseUrl = (process.env.BASE_URL ?? "http://127.0.0.1:8080").replace(/\/$/, "");
const mode = process.env.CONTROL_MODE;
function assert(value: unknown, message: string): asserts value { if (!value) throw new Error(message); }

function validBody(service: string) {
  return JSON.stringify({ logs: [{ timestamp: new Date().toISOString(), level: "info", service, message: "control test" }] });
}

async function rateLimit() {
  const first = await fetch(`${baseUrl}/logs?limit=1`);
  const second = await fetch(`${baseUrl}/logs?limit=1`);
  assert(first.status === 200 && second.status === 429 && second.headers.get("retry-after") === "60", "rate limit did not produce the expected 429 response");
}

async function backpressure() {
  const target = new URL(`${baseUrl}/logs`);
  let resolveFirst!: () => void;
  let rejectFirst!: (error: Error) => void;
  const firstFinished = new Promise<void>((resolve, reject) => { resolveFirst = resolve; rejectFirst = reject; });
  const first = request({ hostname: target.hostname, port: Number(target.port), path: target.pathname, method: "POST", headers: { "content-type": "application/json", "transfer-encoding": "chunked" } }, (response) => {
    response.resume();
    response.on("end", resolveFirst);
  });
  first.on("error", rejectFirst);
  first.write('{"logs":[');
  await new Promise((resolve) => setTimeout(resolve, 150));
  const second = await fetch(`${baseUrl}/logs`, { method: "POST", headers: { "content-type": "application/json" }, body: validBody(`backpressure-${Date.now()}`) });
  first.end("]}");
  await firstFinished;
  assert(second.status === 503 && second.headers.get("retry-after") === "1", "backpressure did not reject a concurrent ingestion");
}

async function main() {
  if (mode === "rate") await rateLimit();
  else if (mode === "backpressure") await backpressure();
  else throw new Error("CONTROL_MODE must be rate or backpressure");
  console.log(`${mode} control integration test passed.`);
}
main().catch((error) => { console.error(error); process.exit(1); });
import { request } from "node:http";
