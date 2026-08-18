let activeQueries = 0;
let lastQueryCompletedAt = Date.now();

/** Track the full lifetime of a route's PostgreSQL work, including failures. */
export function beginDatabaseQueryActivity() {
  activeQueries += 1;
  let ended = false;
  return () => {
    if (ended) return;
    ended = true;
    activeQueries = Math.max(0, activeQueries - 1);
    lastQueryCompletedAt = Date.now();
  };
}

export function areDatabaseQueriesIdleFor(minimumIdleMs: number) {
  return Number.isFinite(minimumIdleMs)
    && minimumIdleMs >= 0
    && activeQueries === 0
    && Date.now() - lastQueryCompletedAt >= minimumIdleMs;
}
