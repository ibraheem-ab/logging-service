import assert from "node:assert/strict";
import test from "node:test";
import { RecentLogCache, type RecentLogRow } from "./recent-log-cache.js";

function row(id: number, milliseconds: number, message = `message-${id}`): RecentLogRow {
  return {
    id: `00000000-0000-4000-8000-${String(id).padStart(12, "0")}`,
    timestamp: new Date(milliseconds),
    level: "info",
    service: "api",
    message,
    attributes: { id },
  };
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((accept, decline) => {
    resolve = accept;
    reject = decline;
  });
  return { promise, resolve, reject };
}

test("single-flights a database seed and merges commits racing its snapshot", async () => {
  const cache = new RecentLogCache({ capacity: 3 });
  const database = deferred<RecentLogRow[]>();
  let loads = 0;
  const load = () => {
    loads += 1;
    return database.promise;
  };
  const first = cache.getOrLoad("tenant", load);
  const second = cache.getOrLoad("tenant", load);
  cache.record("tenant", [row(3, 3)]);
  database.resolve([row(2, 2), row(1, 1)]);

  const expected = [row(3, 3).id, row(2, 2).id, row(1, 1).id];
  assert.equal(loads, 1);
  assert.deepEqual((await first).map(({ id }) => id), expected);
  assert.deepEqual((await second).map(({ id }) => id), expected);
  assert.deepEqual(cache.get("tenant")?.map(({ id }) => id), expected);
});

test("clear during a seed prevents its old snapshot from becoming ready", async () => {
  const cache = new RecentLogCache({ capacity: 3 });
  const database = deferred<RecentLogRow[]>();
  const loading = cache.getOrLoad("tenant", () => database.promise);
  cache.clear();
  database.resolve([row(1, 1)]);
  assert.deepEqual((await loading).map(({ id }) => id), [row(1, 1).id]);
  assert.equal(cache.get("tenant"), undefined);

  let retried = false;
  await cache.getOrLoad("tenant", async () => {
    retried = true;
    return [row(2, 2)];
  });
  assert.equal(retried, true);
  assert.equal(cache.get("tenant")?.[0]?.id, row(2, 2).id);
});

test("an oversized commit invalidates an in-flight seed instead of installing stale state", async () => {
  const cache = new RecentLogCache({ capacity: 3, maxBytesPerTenant: 500, maxTotalBytes: 5_000 });
  const database = deferred<RecentLogRow[]>();
  const loading = cache.getOrLoad("tenant", () => database.promise);
  cache.record("tenant", [row(2, 2, "x".repeat(1_000))]);
  database.resolve([row(1, 1)]);
  await loading;
  assert.equal(cache.get("tenant"), undefined);
});

test("does not evict loading entries when tenant capacity is exhausted", async () => {
  const cache = new RecentLogCache({ capacity: 2, maxTenants: 2, maxTotalBytes: 20_000 });
  const firstDatabase = deferred<RecentLogRow[]>();
  const first = cache.getOrLoad("loading", () => firstDatabase.promise);
  await cache.getOrLoad("ready", async () => [row(1, 1)]);
  await cache.getOrLoad("replacement", async () => [row(2, 2)]);
  assert.equal(cache.get("ready"), undefined);

  cache.record("loading", [row(3, 3)]);
  firstDatabase.resolve([row(1, 1)]);
  assert.deepEqual((await first).map(({ id }) => id), [row(3, 3).id, row(1, 1).id]);
});

test("keeps tenants isolated and evicts least-recently-used ready state", async () => {
  const cache = new RecentLogCache({ capacity: 2, maxTenants: 2, maxTotalBytes: 20_000 });
  await cache.getOrLoad("one", async () => [row(1, 1)]);
  await cache.getOrLoad("two", async () => [row(2, 2)]);
  assert.ok(cache.get("one"));
  await cache.getOrLoad("three", async () => [row(3, 3)]);
  assert.equal(cache.get("two"), undefined);
  assert.equal(cache.get("one")?.[0]?.id, row(1, 1).id);
  assert.equal(cache.get("three")?.[0]?.id, row(3, 3).id);
});

test("orders equal timestamps by PostgreSQL-compatible UUID order", async () => {
  const cache = new RecentLogCache({ capacity: 3 });
  await cache.getOrLoad("tenant", async () => [row(1, 5), row(3, 5), row(2, 5)]);
  assert.deepEqual(cache.get("tenant")?.map(({ id }) => id), [row(3, 5).id, row(2, 5).id, row(1, 5).id]);
});

test("a failed seed is removed and can be retried", async () => {
  const cache = new RecentLogCache();
  await assert.rejects(cache.getOrLoad("tenant", async () => { throw new Error("database failed"); }));
  const rows = await cache.getOrLoad("tenant", async () => [row(1, 1)]);
  assert.equal(rows[0]?.id, row(1, 1).id);
});
