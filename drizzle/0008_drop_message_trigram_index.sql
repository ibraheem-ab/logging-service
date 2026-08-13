-- The generic trigram index generates many index entries for every unique log
-- message and dominates the write path under sustained ingestion. q remains a
-- fully correct ILIKE filter; service/level/time and attribute indexes remain.
DROP INDEX IF EXISTS logs_message_trgm_idx;
