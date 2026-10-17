# Development Progress

## Current handoff

The latest session implemented durable queue claiming and build plans:

- Added migration `0003_pipeline_snapshots.sql` to persist the normalized pipeline representation.
- Added an immediate SQLite transaction for claiming the oldest queued build; concurrent claimers cannot claim the same build.
- Added pipeline validation, snapshot persistence, and insertion of all `pending` step rows in one transaction before transitioning a claimed build from `preparing` to `running`.
- Prevented direct conditional transitions into `running` unless a snapshot and at least one persisted step exist.
- Validation passed: `cargo test --all-targets` (38 tests), `cargo clippy --all-targets --all-features -- -D warnings`, and `cargo fmt --check`.

The preceding storage step added migration `0002_job_build_numbers.sql`, transactional build-number allocation, job/build lookup and bounded listing, job disabling, and conditional status transitions. It is committed separately.

The earlier server fix in `src/server.rs` changed the captured route segment to `{job_id}` and unwraps the extracted path before job lookup. It fixed compilation and router construction.

## Project status

The project plan and README are the source of truth for intended behavior. No full roadmap phase is declared complete here.

The HTTP server and in-memory scheduler still are not wired to `DatabaseStore`; startup recovery is also not persisted in SQLite yet. Next roadmap slice: recover queued and active builds durably at startup, then connect enqueue/claim/execute operations to the database and cover restart recovery end to end. Continue comparing implementation with [PLAN.md](./PLAN.md) and verify the roadmap acceptance criteria before declaring a phase complete.

## Continuation and commits

- Keep the main branch compiling and tested after each logical implementation step.
- Commit each independently completed implementation step with its tests and directly related documentation.
- SQLite schema version is now 3. Build numbers are allocated transactionally; claim transitions and the snapshot/steps-to-running transition are also atomic.
