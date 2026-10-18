ALTER TABLE builds
ADD COLUMN cancel_requested_at INTEGER;

ALTER TABLE builds
ADD COLUMN cancel_reason TEXT;
