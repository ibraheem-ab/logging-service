import assert from "node:assert/strict";
import test from "node:test";
import { createIngestionQueue } from "./ingestion-queue.js";
import type { NewLog } from "../db/schema.js";

function log(message: string): NewLog {
  return { timestamp: new Date("2026-08-13T12:00:00.000Z"), level: "info", service: "test", message, attributes: {} };
}

test("micro-batches tenants and resolves only after the shared write commits", async () => {
  const writes: Array<{ tenantId: string; message: string }> = [];
  let release!: () => void;
  const committed = new Promise<void>((resolve) => { release = resolve; });
  const queue = createIngestionQueue(async (batch) => {
    await committed;
    writes.push(...batch.map(({ tenantId, entry }) => ({ tenantId, message: entry.message })));
  }, { maxLogs: 100, maxDelayMs: 1 });

  let settled = false;
  const first = queue.enqueue("tenant-a", [log("one")]).then(() => { settled = true; });
  const second = queue.enqueue("tenant-b", [log("two")]);
  await new Promise((resolve) => setTimeout(resolve, 10));
  assert.equal(settled, false);
  release();
  await Promise.all([first, second]);
  assert.deepEqual(writes, [
    { tenantId: "tenant-a", message: "one" },
    { tenantId: "tenant-b", message: "two" },
  ]);
});

test("keeps an HTTP request intact when the flush maximum is reached", async () => {
  const copies: number[] = [];
  const queue = createIngestionQueue(async (batch) => { copies.push(batch.length); }, { maxLogs: 3, maxDelayMs: 1 });
  await Promise.all([
    queue.enqueue("default", [log("one"), log("two")]),
    queue.enqueue("default", [log("three"), log("four")]),
  ]);
  assert.deepEqual(copies, [2, 2]);
});

test("rejects every affected request if its durable write fails", async () => {
  const queue = createIngestionQueue(async () => { throw new Error("database unavailable"); }, { maxLogs: 100, maxDelayMs: 1 }, () => {});
  await assert.rejects(queue.enqueue("default", [log("one")]), /database unavailable/);
});

test("drains pending writes during shutdown instead of waiting for the normal flush timer", async () => {
  const copies: number[] = [];
  const queue = createIngestionQueue(async (batch) => { copies.push(batch.length); }, { maxLogs: 100, maxDelayMs: 60_000 });
  const accepted = queue.enqueue("default", [log("one")]);
  await queue.flushPending();
  await accepted;
  await new Promise((resolve) => setTimeout(resolve, 30));
  assert.deepEqual(copies, [1]);
});

test("reports idle time only after pending and active writes finish", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout", "Date"], now: 0 });
  const queue = createIngestionQueue(async () => {}, { maxLogs: 100, maxDelayMs: 500 });
  assert.equal(queue.isIdleFor(100), false);
  t.mock.timers.tick(100);
  assert.equal(queue.isIdleFor(100), true);

  const accepted = queue.enqueue("default", [log("one")]);
  assert.equal(queue.isIdleFor(0), false);
  await queue.flushPending();
  await accepted;
  assert.equal(queue.isIdleFor(100), false);
  t.mock.timers.tick(100);
  assert.equal(queue.isIdleFor(100), true);
});

test("flushes a lone request before the high-throughput batching deadline", async () => {
  let writerStarted!: () => void;
  const writerStartedPromise = new Promise<void>((resolve) => { writerStarted = resolve; });
  const queue = createIngestionQueue(async () => { writerStarted(); }, { maxLogs: 100, maxDelayMs: 500 });

  const accepted = queue.enqueue("default", [log("one")]);
  await Promise.race([
    writerStartedPromise,
    new Promise<never>((_, reject) => setTimeout(() => reject(new Error("lone request waited for the normal deadline")), 150)),
  ]);
  await accepted;
});

test("accelerates a producer proven to send requests sequentially", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout", "Date"], now: 0 });
  let invocations = 0;
  const queue = createIngestionQueue(async () => {
    invocations += 1;
  }, { maxLogs: 100, maxDelayMs: 500 });

  const first = queue.enqueue("default", [log("first")]);
  t.mock.timers.tick(19);
  assert.equal(invocations, 0);
  t.mock.timers.tick(1);
  await first;

  const second = queue.enqueue("default", [log("second")]);
  t.mock.timers.tick(1);
  await second;
  assert.equal(invocations, 2);
});

test("returns a proven serial producer to normal batching when a request joins it", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout", "Date"], now: 0 });
  const batches: string[][] = [];
  const queue = createIngestionQueue(async (batch) => {
    batches.push(batch.map(({ entry }) => entry.message));
  }, { maxLogs: 100, maxDelayMs: 500 });

  const warmup = queue.enqueue("default", [log("warmup")]);
  t.mock.timers.tick(20);
  await warmup;

  const first = queue.enqueue("default", [log("one")]);
  const second = queue.enqueue("default", [log("two")]);
  t.mock.timers.tick(499);
  assert.deepEqual(batches, [["warmup"]]);

  t.mock.timers.tick(1);
  await Promise.all([first, second]);
  assert.deepEqual(batches, [["warmup"], ["one", "two"]]);
});

test("restores normal batching when another request joins a sparse request", async () => {
  const batches: string[][] = [];
  const queue = createIngestionQueue(async (batch) => {
    batches.push(batch.map(({ entry }) => entry.message));
  }, { maxLogs: 100, maxDelayMs: 500 });

  const first = queue.enqueue("default", [log("one")]);
  const second = queue.enqueue("default", [log("two")]);

  // The original sparse timer would have fired around 20ms. A second request
  // must cancel it and keep the two requests together.
  await new Promise((resolve) => setTimeout(resolve, 35));
  assert.deepEqual(batches, []);

  await queue.flushPending();
  await Promise.all([first, second]);
  assert.deepEqual(batches, [["one", "two"]]);
});

test("does not use sparse flushing while another writer is active", async () => {
  let invocations = 0;
  let firstStarted!: () => void;
  const firstStartedPromise = new Promise<void>((resolve) => { firstStarted = resolve; });
  let releaseFirst!: () => void;
  const firstCanFinish = new Promise<void>((resolve) => { releaseFirst = resolve; });
  const queue = createIngestionQueue(async () => {
    invocations += 1;
    if (invocations === 1) {
      firstStarted();
      await firstCanFinish;
    }
  }, { maxLogs: 100, maxDelayMs: 500, maxConcurrentFlushes: 2 });

  const first = queue.enqueue("default", [log("one")]);
  await firstStartedPromise;
  const second = queue.enqueue("default", [log("two")]);
  await new Promise((resolve) => setTimeout(resolve, 35));
  assert.equal(invocations, 1);

  releaseFirst();
  await queue.flushPending();
  await Promise.all([first, second]);
  assert.equal(invocations, 2);
});

test("honors a batching deadline shorter than the sparse request delay", async () => {
  let writerStarted!: () => void;
  const writerStartedPromise = new Promise<void>((resolve) => { writerStarted = resolve; });
  const queue = createIngestionQueue(async () => { writerStarted(); }, { maxLogs: 100, maxDelayMs: 1 });

  const accepted = queue.enqueue("default", [log("one")]);
  await Promise.race([
    writerStartedPromise,
    new Promise<never>((_, reject) => setTimeout(() => reject(new Error("short batching deadline was ignored")), 40)),
  ]);
  await accepted;
});

test("continues flushing later requests after a failed batch", async () => {
  let attempts = 0;
  const queue = createIngestionQueue(async () => {
    attempts += 1;
    if (attempts === 1) throw new Error("temporary database failure");
  }, { maxLogs: 100, maxDelayMs: 1 }, () => {});
  await assert.rejects(queue.enqueue("default", [log("first")]), /temporary database failure/);
  await queue.enqueue("default", [log("second")]);
  assert.equal(attempts, 2);
});

test("allows the configured number of durable writes to run concurrently", async () => {
  let active = 0;
  let maxActive = 0;
  let started!: () => void;
  const twoWritesStarted = new Promise<void>((resolve) => { started = resolve; });
  let release!: () => void;
  const releaseWrites = new Promise<void>((resolve) => { release = resolve; });
  const queue = createIngestionQueue(async () => {
    active += 1;
    maxActive = Math.max(maxActive, active);
    if (active === 2) started();
    await releaseWrites;
    active -= 1;
  }, { maxLogs: 1, maxDelayMs: 60_000, maxConcurrentFlushes: 2 });

  const first = queue.enqueue("default", [log("one")]);
  const second = queue.enqueue("default", [log("two")]);
  const third = queue.enqueue("default", [log("three")]);
  await twoWritesStarted;
  assert.equal(maxActive, 2);
  release();
  await Promise.all([first, second, third]);
  assert.equal(maxActive, 2);
});

test("does not add a second batching delay after a busy writer releases", async () => {
  let invocation = 0;
  let firstStarted!: () => void;
  const firstStartedPromise = new Promise<void>((resolve) => { firstStarted = resolve; });
  let releaseFirst!: () => void;
  const firstCanFinish = new Promise<void>((resolve) => { releaseFirst = resolve; });
  let secondStarted!: () => void;
  const secondStartedPromise = new Promise<void>((resolve) => { secondStarted = resolve; });

  const queue = createIngestionQueue(async () => {
    invocation += 1;
    if (invocation === 1) {
      firstStarted();
      await firstCanFinish;
      return;
    }
    secondStarted();
  }, { maxLogs: 100, maxDelayMs: 25 });

  const first = queue.enqueue("default", [log("one")]);
  await firstStartedPromise;
  const second = queue.enqueue("default", [log("two")]);
  // The second request has already waited longer than its batching interval
  // while the first COPY was in progress.
  await new Promise((resolve) => setTimeout(resolve, 35));
  releaseFirst();
  await Promise.race([
    secondStartedPromise,
    new Promise<never>((_, reject) => setTimeout(() => reject(new Error("second write was delayed again")), 15)),
  ]);
  await Promise.all([first, second]);
});

test("preserves FIFO order after compacting a large pending request backlog", async () => {
  const written: string[] = [];
  let firstStarted!: () => void;
  const firstStartedPromise = new Promise<void>((resolve) => { firstStarted = resolve; });
  let releaseFirst!: () => void;
  const firstCanFinish = new Promise<void>((resolve) => { releaseFirst = resolve; });
  let writes = 0;
  const queue = createIngestionQueue(async (batch) => {
    writes += 1;
    if (writes === 1) {
      firstStarted();
      await firstCanFinish;
    }
    written.push(...batch.map(({ entry }) => entry.message));
  }, { maxLogs: 10, maxDelayMs: 60_000 });

  const expected = Array.from({ length: 1_100 }, (_, index) => String(index));
  const pending = expected.map((message) => queue.enqueue("default", [log(message)]));
  await firstStartedPromise;
  releaseFirst();
  await Promise.all(pending);
  assert.deepEqual(written, expected);
});
