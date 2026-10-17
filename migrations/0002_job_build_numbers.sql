ALTER TABLE jobs
ADD COLUMN next_build_number INTEGER NOT NULL DEFAULT 1 CHECK (next_build_number > 0);
