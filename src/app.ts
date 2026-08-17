import express, { type NextFunction, type Request, type Response } from "express";
import { gzipSync } from "node:zlib";
import { config } from "./config.js";
import { ApiError } from "./errors.js";
import { getAggregate, getLogs } from "./db/queries.js";
import { parseAggregateQuery, parseLogsQuery, validateIngestionBatch } from "./validation.js";
import { authenticate, type Principal } from "./services/auth.js";
import { addTail, metrics, publishTail, recordIngestion, recordRequest } from "./services/observability.js";
import { allowRequest, beginIngestion, endIngestion, persistDeadLetters } from "./services/ingestion-controls.js";
import { notifyErrorThreshold } from "./services/alerts.js";
import { enqueueIngestion } from "./services/ingestion-batcher.js";

export const app = express();
const defaultPrincipal: Principal = { tenantId: "default", scopes: ["ingest", "query"], seeded: false };

app.disable("x-powered-by");
// Cursor pages are dynamic and can be large; calculating an ETag hashes every
// response body without helping the load-generator contract.
app.disable("etag");
if (config.metricsEnabled) {
  app.use((_req, res, next) => { res.on("finish", () => recordRequest(res.statusCode)); next(); });
}
if (config.compressionEnabled) {
  app.use((req, res, next) => {
    if (!req.acceptsEncodings("gzip")) return next();
    const send = res.send.bind(res);
    res.send = ((body: unknown) => {
      if (res.get("content-encoding") || (typeof body !== "string" && !Buffer.isBuffer(body))) return send(body as never);
      const source = Buffer.isBuffer(body) ? body : Buffer.from(body);
      if (source.length < 1024) return send(body as never);
      res.set("content-encoding", "gzip").removeHeader("content-length");
      return send(gzipSync(source) as never);
    }) as Response["send"];
    return next();
  });
}

app.get("/health", (_req, res) => {
  res.status(200).json({ status: "ok" });
});

if (config.authEnabled) {
  app.use(async (req, res, next) => {
    if (req.path === "/health") return next();
    const authorization = req.get("authorization");
    const bearer = authorization?.match(/^Bearer\s+(.+)$/i)?.[1] ?? req.get("x-api-key");
    if (!bearer) return res.status(401).json({ error: "missing or malformed credential" });
    try {
      const principal = await authenticate(bearer);
      if (!principal) return res.status(401).json({ error: "invalid credential" });
      res.locals.principal = principal;
      return next();
    } catch (error) {
      return next(error);
    }
  });
}

app.get("/metrics", (_req, res) => {
  if (!config.metricsEnabled) return res.status(404).json({ error: "metrics disabled" });
  res.type("text/plain; version=0.0.4").send(metrics());
});

app.get("/dashboard", (_req, res) => {
  res.type("html").send(`<!doctype html><title>Log Service</title><main><h1>Log Service Dashboard</h1><form id=f><input name=service placeholder=service><select name=level><option value="">all levels</option><option>debug</option><option>info</option><option>warn</option><option>error</option></select><input name=q placeholder="message search"><button>Search</button></form><pre id=logs>Submit a search.</pre><h2>Metrics</h2><pre id=m>Loading…</pre><script>const out=document.querySelector('#logs');document.querySelector('#f').onsubmit=async e=>{e.preventDefault();const p=new URLSearchParams(new FormData(e.target));for(const[k,v]of [...p])if(!v)p.delete(k);out.textContent=JSON.stringify(await fetch('/logs?'+p).then(r=>r.json()),null,2)};fetch('/metrics').then(r=>r.text()).then(t=>m.textContent=t)</script></main>`);
});

function principalFor(res: Response): Principal {
  return res.locals.principal ?? defaultPrincipal;
}

if (config.rateLimitEnabled) {
  app.use((req, res, next) => {
    if (!req.path.startsWith("/logs")) return next();
    const principal = principalFor(res);
    if (!allowRequest(principal.tenantId, true, config.rateLimitRequests, principal.seeded)) {
      return res.set("retry-after", "60").status(429).json({ error: "rate limit exceeded" });
    }
    return next();
  });
}

if (config.backpressureEnabled) {
  app.use((req, res, next) => {
    if (req.method !== "POST" || req.path !== "/logs") return next();
    if (!beginIngestion(true, config.maxConcurrentIngestions)) {
      return res.set("retry-after", "1").status(503).json({ error: "ingestion queue is full" });
    }
    let released = false;
    const release = () => {
      if (released) return;
      released = true;
      endIngestion();
    };
    res.once("finish", release);
    res.once("close", release);
    return next();
  });
}

app.use(express.json({ limit: config.maxBodySize }));

app.post("/logs", async (req, res, next) => {
  try {
    const result = validateIngestionBatch(req.body);
    const principal = principalFor(res);
    if (!principal.scopes.includes("ingest")) return res.status(403).json({ error: "credential lacks ingest permission" });
    if (config.deadLetterEnabled && result.rejected.length > 0) {
      await persistDeadLetters(principal.tenantId, req.body, result.rejected);
    }
    if (result.validLogs.length === 0) {
      return res.status(400).json({ accepted: 0, rejected: result.rejected });
    }

    await enqueueIngestion(principal.tenantId, result.validLogs);
    if (config.alertsEnabled) notifyErrorThreshold(result.validLogs, principal.tenantId);
    if (config.metricsEnabled) recordIngestion(result.validLogs.length, result.rejected.length);
    if (config.liveTailEnabled) publishTail(principal.tenantId, result.validLogs);
    return res.status(200).json({ accepted: result.validLogs.length, rejected: result.rejected });
  } catch (error) {
    return next(error);
  }
});

app.get("/logs/tail", (req, res) => {
  if (!config.liveTailEnabled) return res.status(404).json({ error: "live tail disabled" });
  const principal = principalFor(res);
  if (!principal.scopes.includes("query")) return res.status(403).json({ error: "credential lacks query permission" });
  res.status(200).set({ "content-type": "text/event-stream", "cache-control": "no-cache", connection: "keep-alive" });
  res.write("event: ready\ndata: {}\n\n");
  addTail(principal.tenantId, res);
});

app.get("/logs", async (req, res, next) => {
  try {
    const query = parseLogsQuery(req.query);
    const principal = principalFor(res);
    if (!principal.scopes.includes("query")) return res.status(403).json({ error: "credential lacks query permission" });
    const result = await getLogs(query, principal.tenantId);
    return res.status(200).json({ logs: result.logs, next_cursor: result.nextCursor });
  } catch (error) {
    return next(error);
  }
});

app.get("/logs/aggregate", async (req, res, next) => {
  try {
    const query = parseAggregateQuery(req.query);
    const principal = principalFor(res);
    if (!principal.scopes.includes("query")) return res.status(403).json({ error: "credential lacks query permission" });
    const buckets = await getAggregate(query, principal.tenantId);
    return res.status(200).json({ buckets });
  } catch (error) {
    return next(error);
  }
});

app.use((error: unknown, _req: Request, res: Response, _next: NextFunction) => {
  if (error instanceof SyntaxError && "status" in error && error.status === 400) {
    return res.status(400).json({ error: "malformed JSON request body" });
  }
  if (error instanceof ApiError) {
    return res.status(error.statusCode).json({ error: error.message });
  }
  if (typeof error === "object" && error !== null && "status" in error && typeof error.status === "number" && error.status >= 400 && error.status < 500) {
    const message = "message" in error && typeof error.message === "string" ? error.message : "invalid request";
    return res.status(error.status).json({ error: message });
  }

  console.error("Unhandled request error:", error);
  return res.status(500).json({ error: "internal server error" });
});
