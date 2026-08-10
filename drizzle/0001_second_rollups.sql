CREATE TABLE IF NOT EXISTS log_second_rollups (
  bucket_start timestamptz NOT NULL,
  service text NOT NULL,
  level text NOT NULL CHECK (level IN ('debug', 'info', 'warn', 'error')),
  count bigint NOT NULL CHECK (count >= 0),
  PRIMARY KEY (bucket_start, service, level)
);
--> statement-breakpoint

CREATE OR REPLACE FUNCTION roll_up_inserted_logs() RETURNS trigger AS $$
BEGIN
  INSERT INTO log_second_rollups (bucket_start, service, level, count)
  SELECT date_trunc('second', timestamp), service, level, count(*)
  FROM new_logs
  GROUP BY 1, 2, 3
  ON CONFLICT (bucket_start, service, level)
  DO UPDATE SET count = log_second_rollups.count + EXCLUDED.count;
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;
--> statement-breakpoint

CREATE OR REPLACE FUNCTION roll_up_deleted_logs() RETURNS trigger AS $$
BEGIN
  UPDATE log_second_rollups AS rollup
  SET count = rollup.count - deleted.count
  FROM (
    SELECT date_trunc('second', timestamp) AS bucket_start, service, level, count(*)
    FROM old_logs
    GROUP BY 1, 2, 3
  ) AS deleted
  WHERE rollup.bucket_start = deleted.bucket_start
    AND rollup.service = deleted.service
    AND rollup.level = deleted.level;

  DELETE FROM log_second_rollups WHERE count = 0;
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;
--> statement-breakpoint

DROP TRIGGER IF EXISTS logs_rollup_after_insert ON logs;
--> statement-breakpoint
CREATE TRIGGER logs_rollup_after_insert
AFTER INSERT ON logs
REFERENCING NEW TABLE AS new_logs
FOR EACH STATEMENT EXECUTE FUNCTION roll_up_inserted_logs();
--> statement-breakpoint

DROP TRIGGER IF EXISTS logs_rollup_after_delete ON logs;
--> statement-breakpoint
CREATE TRIGGER logs_rollup_after_delete
AFTER DELETE ON logs
REFERENCING OLD TABLE AS old_logs
FOR EACH STATEMENT EXECUTE FUNCTION roll_up_deleted_logs();
--> statement-breakpoint

INSERT INTO log_second_rollups (bucket_start, service, level, count)
SELECT date_trunc('second', timestamp), service, level, count(*)
FROM logs
GROUP BY 1, 2, 3
ON CONFLICT (bucket_start, service, level)
DO UPDATE SET count = log_second_rollups.count + EXCLUDED.count;
--> statement-breakpoint

CREATE INDEX IF NOT EXISTS log_second_rollups_bucket_idx
ON log_second_rollups (bucket_start, service, level);
