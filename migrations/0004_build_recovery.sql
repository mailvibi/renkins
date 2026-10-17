ALTER TABLE builds
ADD COLUMN finished_at INTEGER;

ALTER TABLE builds
ADD COLUMN failure_kind TEXT;
