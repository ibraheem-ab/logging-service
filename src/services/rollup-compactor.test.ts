import assert from "node:assert/strict";
import test from "node:test";
import { createRollupCompactor } from "./rollup-compactor.js";

test("compacts bounded chunks only while the service remains idle", async (t) => {
  t.mock.timers.enable({ apis: ["setInterval"] });
  let idle = false;
  let calls = 0;
  let secondCalled!: () => void;
  const secondCall = new Promise<void>((resolve) => { secondCalled = resolve; });
  const stop = createRollupCompactor(
    async () => {
      calls += 1;
      if (calls === 2) {
        idle = false;
        secondCalled();
      }
      return true;
    },
    () => idle,
    { intervalMs: 10, minimumIdleMs: 1, maxChunksPerTurn: 4 },
  );

  t.mock.timers.tick(10);
  await Promise.resolve();
  assert.equal(calls, 0);

  idle = true;
  t.mock.timers.tick(10);
  await secondCall;
  await stop();
  assert.equal(calls, 2);
});

test("stops a turn when the delta table reports it is drained", async (t) => {
  t.mock.timers.enable({ apis: ["setInterval"] });
  let calls = 0;
  const stop = createRollupCompactor(
    async () => {
      calls += 1;
      return false;
    },
    () => true,
    { intervalMs: 10, minimumIdleMs: 1, maxChunksPerTurn: 4 },
  );
  t.mock.timers.tick(10);
  await stop();
  assert.equal(calls, 1);
});
