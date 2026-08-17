import assert from "node:assert/strict";
import test from "node:test";
import {
  orderedLogIdSlice,
  packLogIds,
  QueryPageSessions,
} from "./query-page-sessions.js";

test("keeps ordered page snapshots tenant- and query-bound without consuming retries", () => {
  let now = 1_000;
  const sessions = new QueryPageSessions({ now: () => now, ttlMs: 100, maxSessions: 2, maxTotalIds: 10 });
  const created = sessions.create("tenant-a", "query-a", ["newest", "older"]);
  assert.ok(created);
  assert.deepEqual(sessions.get(created.id, "tenant-a", "query-a")?.ids, ["newest", "older"]);
  assert.deepEqual(sessions.get(created.id, "tenant-a", "query-a")?.ids, ["newest", "older"]);
  assert.equal(sessions.get(created.id, "tenant-b", "query-a"), undefined);
  assert.equal(sessions.get(created.id, "tenant-a", "query-b"), undefined);

  now = 1_101;
  assert.equal(sessions.get(created.id, "tenant-a", "query-a"), undefined);
});

test("uses LRU eviction so active cursor snapshots retain bounded capacity", () => {
  const sessions = new QueryPageSessions({ maxSessions: 2, maxIdsPerSession: 4, maxTotalIds: 4 });
  const first = sessions.create("default", "one", ["1", "2"]);
  const second = sessions.create("default", "two", ["3", "4"]);
  assert.ok(first && second);
  // Touch the first cursor; the untouched second cursor is now least-recent.
  assert.deepEqual(sessions.get(first.id, "default", "one")?.ids, ["1", "2"]);
  const third = sessions.create("default", "three", ["5", "6"]);
  assert.ok(third);
  assert.deepEqual(sessions.get(first.id, "default", "one")?.ids, ["1", "2"]);
  assert.equal(sessions.get(second.id, "default", "two"), undefined);
  assert.deepEqual(sessions.get(third.id, "default", "three")?.ids, ["5", "6"]);
  assert.equal(sessions.create("default", "too-large", ["1", "2", "3", "4", "5"]), undefined);

  sessions.delete(first.id);
  assert.ok(sessions.create("default", "four", ["7", "8"]));
});

test("packs UUID snapshots without changing their ordered page slices", () => {
  const ids = [
    "00000000-0000-4000-8000-000000000001",
    "00000000-0000-4000-8000-000000000002",
    "00000000-0000-4000-8000-000000000003",
    "00000000-0000-4000-8000-000000000004",
    "00000000-0000-4000-8000-000000000005",
  ];
  const packed = packLogIds(ids, 2);
  assert.equal(packed.count, ids.length);
  assert.deepEqual(orderedLogIdSlice(packed, 1, 4), ids.slice(1, 4));
  assert.throws(() => packLogIds(["not-a-uuid"]), TypeError);
});

test("can reserve a large snapshot slot without evicting an active drain", () => {
  const sessions = new QueryPageSessions({
    maxSessions: 1,
    maxIdsPerSession: 4,
    maxTotalIds: 4,
    evictOnCreate: false,
  });
  assert.equal(sessions.hasUnreservedCapacityFor(4), true);
  const active = sessions.create("default", "one", ["1", "2", "3", "4"]);
  assert.ok(active);
  assert.equal(sessions.hasUnreservedCapacityFor(1), false);
  assert.equal(sessions.create("default", "two", ["5"]), undefined);
  assert.deepEqual(sessions.get(active.id, "default", "one")?.ids, ["1", "2", "3", "4"]);
});

test("peeking at a snapshot does not renew its lease", () => {
  let now = 1_000;
  const sessions = new QueryPageSessions({ now: () => now, ttlMs: 100 });
  const session = sessions.create("default", "one", ["1"]);
  assert.ok(session);
  now = 1_099;
  assert.ok(sessions.peek(session.id, "default", "one"));
  now = 1_101;
  sessions.pruneExpired();
  assert.equal(sessions.get(session.id, "default", "one"), undefined);
});
