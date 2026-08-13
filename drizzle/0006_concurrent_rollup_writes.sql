-- COPY transactions may run concurrently. The short rollup update remains
-- serialized with pg_advisory_xact_lock so raw rows and summary rows commit
-- together without a hot-row lock queue on every incoming HTTP request.
DROP TRIGGER IF EXISTS logs_rollup_after_insert ON logs;
--> statement-breakpoint
DROP FUNCTION IF EXISTS roll_up_inserted_logs();
--> statement-breakpoint
CREATE OR REPLACE FUNCTION roll_up_deleted_logs() RETURNS trigger AS $$
BEGIN
  PERFORM pg_advisory_xact_lock(78123456);
  UPDATE log_second_rollups AS rollup SET count = rollup.count - deleted.count
  FROM (SELECT tenant_id, date_trunc('second', timestamp) AS bucket_start, service, level, count(*) FROM old_logs GROUP BY 1, 2, 3, 4) AS deleted
  WHERE rollup.tenant_id = deleted.tenant_id AND rollup.bucket_start = deleted.bucket_start AND rollup.service = deleted.service AND rollup.level = deleted.level;
  DELETE FROM log_second_rollups WHERE count = 0;
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;
