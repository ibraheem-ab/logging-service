import { client } from "../db/index.js";

const windows = new Map<string, { startedAt: number; requests: number }>();
let activeIngestions = 0;

export function allowRequest(key: string, enabled: boolean, limit: number, exempt: boolean) {
  if (!enabled || exempt) return true;
  const now = Date.now();
  const current = windows.get(key);
  if (!current || now - current.startedAt >= 60_000) {
    windows.set(key, { startedAt: now, requests: 1 });
    return true;
  }
  if (current.requests >= limit) return false;
  current.requests += 1;
  return true;
}

export function beginIngestion(enabled: boolean, limit: number) {
  if (enabled && activeIngestions >= limit) return false;
  activeIngestions += 1;
  return true;
}

export function endIngestion() { activeIngestions = Math.max(0, activeIngestions - 1); }

export async function persistDeadLetters(tenantId: string, body: unknown, rejected: Array<{ index: number; reason: string }>) {
  if (!body || typeof body !== "object" || !("logs" in body) || !Array.isArray(body.logs)) return;
  const logs = body.logs as unknown[];
  await Promise.all(rejected.map(({ index, reason }) => client`
    INSERT INTO dead_letters (tenant_id, entry_index, reason, payload)
    VALUES (${tenantId}, ${index}, ${reason}, ${JSON.stringify(logs[index])}::jsonb)
  `));
}
