CREATE EXTENSION IF NOT EXISTS pgcrypto;
--> statement-breakpoint
CREATE EXTENSION IF NOT EXISTS pg_trgm;
--> statement-breakpoint

CREATE TABLE IF NOT EXISTS logs (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  timestamp timestamptz NOT NULL DEFAULT now(),
  level text NOT NULL CHECK (level IN ('debug', 'info', 'warn', 'error')),
  service text NOT NULL CHECK (length(btrim(service)) > 0),
  message text NOT NULL CHECK (length(btrim(message)) > 0),
  attributes jsonb NOT NULL DEFAULT '{}'::jsonb
);
--> statement-breakpoint

UPDATE logs SET attributes = '{}'::jsonb WHERE attributes IS NULL;
--> statement-breakpoint
ALTER TABLE logs ALTER COLUMN attributes SET DEFAULT '{}'::jsonb;
--> statement-breakpoint
ALTER TABLE logs ALTER COLUMN attributes SET NOT NULL;
--> statement-breakpoint

CREATE INDEX IF NOT EXISTS logs_timestamp_id_idx ON logs (timestamp DESC, id DESC);
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS logs_service_timestamp_id_idx ON logs (service, timestamp DESC, id DESC);
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS logs_level_timestamp_id_idx ON logs (level, timestamp DESC, id DESC);
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS logs_attributes_gin_idx ON logs USING GIN (attributes jsonb_path_ops);
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS logs_message_trgm_idx ON logs USING GIN (message gin_trgm_ops);
