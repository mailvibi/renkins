ALTER TABLE builds
ADD COLUMN log_integrity TEXT NOT NULL DEFAULT 'valid'
CHECK (log_integrity IN ('valid', 'recovered_tail', 'corrupt', 'expired'));

ALTER TABLE builds
ADD COLUMN recovery_marker_written INTEGER NOT NULL DEFAULT 0
CHECK (recovery_marker_written IN (0, 1));
