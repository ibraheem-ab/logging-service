ALTER TABLE log_second_rollups ADD COLUMN IF NOT EXISTS tenant_id text NOT NULL DEFAULT 'default';
--> statement-breakpoint
ALTER TABLE log_second_rollups DROP CONSTRAINT IF EXISTS log_second_rollups_pkey;
--> statement-breakpoint
ALTER TABLE log_second_rollups ADD PRIMARY KEY (tenant_id, bucket_start, service, level);
--> statement-breakpoint
CREATE OR REPLACE FUNCTION roll_up_inserted_logs() RETURNS trigger AS $$
BEGIN
  INSERT INTO log_second_rollups (tenant_id, bucket_start, service, level, count)
  SELECT tenant_id, date_trunc('second', timestamp), service, level, count(*)
  FROM new_logs GROUP BY 1, 2, 3, 4
  ON CONFLICT (tenant_id, bucket_start, service, level)
  DO UPDATE SET count = log_second_rollups.count + EXCLUDED.count;
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;
--> statement-breakpoint
TRUNCATE log_second_rollups;
--> statement-breakpoint
INSERT INTO log_second_rollups (tenant_id, bucket_start, service, level, count)
SELECT tenant_id, date_trunc('second', timestamp), service, level, count(*)
FROM logs GROUP BY 1, 2, 3, 4;
--> statement-breakpoint
CREATE OR REPLACE FUNCTION roll_up_deleted_logs() RETURNS trigger AS $$
BEGIN
  UPDATE log_second_rollups AS rollup SET count = rollup.count - deleted.count
  FROM (SELECT tenant_id, date_trunc('second', timestamp) AS bucket_start, service, level, count(*) FROM old_logs GROUP BY 1, 2, 3, 4) AS deleted
  WHERE rollup.tenant_id = deleted.tenant_id AND rollup.bucket_start = deleted.bucket_start AND rollup.service = deleted.service AND rollup.level = deleted.level;
  DELETE FROM log_second_rollups WHERE count = 0;
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;
