ALTER TABLE builds
ADD COLUMN started_at INTEGER;

ALTER TABLE builds
ADD COLUMN resolved_commit TEXT;

ALTER TABLE build_steps
ADD COLUMN finished_at INTEGER;

ALTER TABLE build_steps
ADD COLUMN exit_code INTEGER;
