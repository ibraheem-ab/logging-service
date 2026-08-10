ALTER TABLE logs ADD COLUMN IF NOT EXISTS tenant_id text NOT NULL DEFAULT 'default';
--> statement-breakpoint
CREATE TABLE IF NOT EXISTS api_keys (
  key_hash text PRIMARY KEY,
  tenant_id text NOT NULL,
  scopes text[] NOT NULL DEFAULT ARRAY['ingest', 'query'],
  created_at timestamptz NOT NULL DEFAULT now()
);
--> statement-breakpoint

CREATE TABLE IF NOT EXISTS dead_letters (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id text NOT NULL DEFAULT 'default',
  received_at timestamptz NOT NULL DEFAULT now(),
  entry_index integer NOT NULL,
  reason text NOT NULL,
  payload jsonb NOT NULL
);
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS dead_letters_received_at_idx ON dead_letters (received_at DESC);
