-- Keep the ingestion transaction free of contended UPSERTs on the current
-- second. These small, append-only deltas are included in aggregation reads
-- immediately and may be compacted into log_second_rollups later.
CREATE TABLE IF NOT EXISTS log_second_rollup_deltas (
  id bigserial PRIMARY KEY,
  tenant_id text NOT NULL,
  bucket_start timestamptz NOT NULL,
  service text NOT NULL,
  level text NOT NULL CHECK (level IN ('debug', 'info', 'warn', 'error')),
  count bigint NOT NULL CHECK (count > 0)
);
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS log_second_rollup_deltas_query_idx
ON log_second_rollup_deltas (tenant_id, bucket_start, service, level);
--> statement-breakpoint
-- deleteExpiredLogs holds the exclusive maintenance advisory lock before a
-- DELETE. At that point every committed delta has been compacted, so this
-- statement trigger only needs to subtract from the compact summary table.
CREATE OR REPLACE FUNCTION roll_up_deleted_logs() RETURNS trigger AS $$
BEGIN
  UPDATE log_second_rollups AS rollup SET count = rollup.count - deleted.count
  FROM (SELECT tenant_id, date_trunc('second', timestamp) AS bucket_start, service, level, count(*) FROM old_logs GROUP BY 1, 2, 3, 4) AS deleted
  WHERE rollup.tenant_id = deleted.tenant_id AND rollup.bucket_start = deleted.bucket_start AND rollup.service = deleted.service AND rollup.level = deleted.level;
  DELETE FROM log_second_rollups WHERE count = 0;
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;
