import { createServer } from "node:http";

const baseUrl = (process.env.BASE_URL ?? "http://127.0.0.1:8080").replace(/\/$/, "");
function assert(value: unknown, message: string): asserts value { if (!value) throw new Error(message); }

async function main() {
  let payload = "";
  const server = createServer((req, res) => {
    req.setEncoding("utf8");
    req.on("data", (chunk) => { payload += chunk; });
    req.on("end", () => res.writeHead(204).end());
  });
  await new Promise<void>((resolve) => server.listen(18089, "0.0.0.0", resolve));
  try {
    const response = await fetch(`${baseUrl}/logs`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ logs: [{ timestamp: new Date().toISOString(), level: "error", service: `alert-${Date.now()}`, message: "alert integration" }] }) });
    assert(response.status === 200, "alert integration log ingestion failed");
    for (let attempt = 0; attempt < 20 && !payload; attempt += 1) await new Promise((resolve) => setTimeout(resolve, 100));
    const alert = JSON.parse(payload || "null") as { tenant_id?: string; error_count?: number } | null;
    assert(alert?.tenant_id === "default" && alert.error_count === 1, "alert webhook payload was not delivered correctly");
    console.log("Alert webhook integration test passed.");
  } finally {
    await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
  }
}
main().catch((error) => { console.error(error); process.exit(1); });
