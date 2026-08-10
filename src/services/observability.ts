import type { Response } from "express";

const counters = { requests: 0, failures: 0, accepted: 0, rejected: 0 };
const tails = new Map<string, Set<Response>>();

export function recordRequest(status: number) { counters.requests += 1; if (status >= 500) counters.failures += 1; }
export function recordIngestion(accepted: number, rejected: number) { counters.accepted += accepted; counters.rejected += rejected; }
export function metrics() {
  return [
    "# TYPE log_service_requests_total counter", `log_service_requests_total ${counters.requests}`,
    "# TYPE log_service_failures_total counter", `log_service_failures_total ${counters.failures}`,
    "# TYPE log_service_accepted_logs_total counter", `log_service_accepted_logs_total ${counters.accepted}`,
    "# TYPE log_service_rejected_logs_total counter", `log_service_rejected_logs_total ${counters.rejected}`,
  ].join("\n") + "\n";
}
export function addTail(tenantId: string, res: Response) {
  const subscribers = tails.get(tenantId) ?? new Set<Response>();
  subscribers.add(res);
  tails.set(tenantId, subscribers);
  res.on("close", () => { subscribers.delete(res); if (subscribers.size === 0) tails.delete(tenantId); });
}
export function publishTail(tenantId: string, logs: unknown[]) {
  for (const res of tails.get(tenantId) ?? []) res.write(`event: logs\ndata: ${JSON.stringify(logs)}\n\n`);
}
