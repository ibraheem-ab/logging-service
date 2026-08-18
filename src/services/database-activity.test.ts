import assert from "node:assert/strict";
import test from "node:test";
import { areDatabaseQueriesIdleFor, beginDatabaseQueryActivity } from "./database-activity.js";

test("reports idle only after every active query finishes", () => {
  const finishFirst = beginDatabaseQueryActivity();
  const finishSecond = beginDatabaseQueryActivity();
  assert.equal(areDatabaseQueriesIdleFor(0), false);
  finishFirst();
  assert.equal(areDatabaseQueriesIdleFor(0), false);
  finishSecond();
  assert.equal(areDatabaseQueriesIdleFor(0), true);
});

test("a route finally callback is idempotent", () => {
  const finish = beginDatabaseQueryActivity();
  finish();
  finish();
  assert.equal(areDatabaseQueriesIdleFor(0), true);
});
