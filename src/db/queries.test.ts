import assert from "node:assert/strict";
import test from "node:test";
import { summarizeRollupWrites, type LogWrite, uuidV7, uuidV7FromRandomBytes } from "./queries.js";
import { decodeCursor } from "../cursor.js";

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

test("generates UUIDv7 values accepted by cursor pagination", () => {
  const timestamp = new Date("2026-08-13T12:00:00.000Z");
  for (let index = 0; index < 100; index += 1) {
    const id = uuidV7(timestamp.getTime());
    assert.match(id, /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i);
    assert.deepEqual(decodeCursor(Buffer.from(JSON.stringify({ timestamp: timestamp.toISOString(), id })).toString("base64url")), { timestamp, id });
  }
});

test("maps exactly 74 independent random bits into UUIDv7", () => {
  const milliseconds = 0;
  const baseline = uuidV7FromRandomBytes(milliseconds, Buffer.alloc(10));
  assert.equal(baseline, "00000000-0000-7000-8000-000000000000");
  assert.equal(
    uuidV7FromRandomBytes(milliseconds, Buffer.alloc(10, 0xff)),
    "00000000-0000-7fff-bfff-ffffffffffff",
  );

  const changed = new Set<string>();
  let discarded = 0;
  for (let bit = 0; bit < 80; bit += 1) {
    const bytes = Buffer.alloc(10);
    bytes[Math.floor(bit / 8)] = 1 << (bit % 8);
    const id = uuidV7FromRandomBytes(milliseconds, bytes);
    if (id === baseline) discarded += 1;
    else changed.add(id);
  }
  assert.equal(discarded, 6);
  assert.equal(changed.size, 74);
});

test("UUIDv7 encodes timestamp order and remains unique within one millisecond", () => {
  const timestamps = [0, 1, Date.parse("2026-08-18T12:00:00.000Z"), 2 ** 48 - 1];
  for (const milliseconds of timestamps) {
    const id = uuidV7(milliseconds);
    assert.equal(Number.parseInt(id.slice(0, 8) + id.slice(9, 13), 16), milliseconds);
  }
  assert.ok(uuidV7(timestamps[2]!) < uuidV7(timestamps[2]! + 1));

  const sameMillisecond = Array.from({ length: 1_000 }, () => uuidV7(timestamps[2]!));
  assert.equal(new Set(sameMillisecond).size, sameMillisecond.length);

  const historicalId = uuidV7(Date.parse("1960-01-01T00:00:00.000Z"));
  assert.match(historicalId, /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
  assert.ok(historicalId.startsWith("00000000-0000-7"));
});
