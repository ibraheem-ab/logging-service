import assert from "node:assert/strict";
import test from "node:test";
import { summarizeRollupWrites, type LogWrite } from "./queries.js";

function write(tenantId: string, timestamp: string, service: string, level = "info"): LogWrite {
  return {
    tenantId,
    entry: {
      timestamp: new Date(timestamp),
      level: level as "info" | "error",
      service,
      message: `${tenantId}-${service}-${level}`,
      attributes: {},
    },
  };
}

test("groups manual rollup rows by tenant, second, service, and level", () => {
  const rollups = summarizeRollupWrites([
    write("tenant-a", "2026-08-13T12:00:00.100Z", "api"),
    write("tenant-a", "2026-08-13T12:00:00.900Z", "api"),
    write("tenant-b", "2026-08-13T12:00:00.900Z", "api"),
    write("tenant-a", "2026-08-13T12:00:01.000Z", "api", "error"),
  ]);

  assert.deepEqual(rollups, [
    { tenant_id: "tenant-a", bucket_start: "2026-08-13T12:00:00.000Z", service: "api", level: "info", count: 2 },
    { tenant_id: "tenant-a", bucket_start: "2026-08-13T12:00:01.000Z", service: "api", level: "error", count: 1 },
    { tenant_id: "tenant-b", bucket_start: "2026-08-13T12:00:00.000Z", service: "api", level: "info", count: 1 },
  ]);
});

test("sums repeated groups from multiple committed delta batches", () => {
  const rollups = summarizeRollupWrites([
    write("default", "2026-08-13T12:00:00.100Z", "api"),
    write("default", "2026-08-13T12:00:00.200Z", "api"),
    write("default", "2026-08-13T12:00:00.300Z", "api"),
  ]);
  assert.deepEqual(rollups, [
    { tenant_id: "default", bucket_start: "2026-08-13T12:00:00.000Z", service: "api", level: "info", count: 3 },
  ]);
});
