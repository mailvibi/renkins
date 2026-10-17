# Development Progress

## Current handoff

The latest session implemented durable job/build storage:

- Added migration `0002_job_build_numbers.sql`; per-job build numbers begin at one and are assigned transactionally as builds are enqueued.
- Added job lookup/listing with bounded pagination and enable/disable storage operations.
- Added build lookup/history listing with bounded pagination and conditional status transitions that validate domain transition rules.
- Added tests for persistence after reopening, previous-schema upgrade, paginated reads, concurrent number allocation, failed-insert rollback, disabled jobs, and conditional status transitions.
- SQLite setup from the previous step remains: embedded transactional migrations tracked by `user_version`, a five-second busy timeout, WAL, and foreign-key enforcement.

The earlier server fix in `src/server.rs` changed the captured route segment to `{job_id}` and unwraps the extracted path before job lookup. It fixed compilation and router construction.

Validation passed: `cargo test --all-targets` (36 tests), `cargo clippy --all-targets --all-features -- -D warnings`, and `cargo fmt --check`.

## Project status

The project plan and README are the source of truth for intended behavior. No full roadmap phase is declared complete here.

The HTTP server still uses in-memory job storage and is not wired to `DatabaseStore`. Next roadmap slice: add transactional durable queue claiming and persist validated pipeline snapshots plus pending step rows before execution. Then connect server/scheduler operations to the persistent store and cover competing claimers and restart recovery. Continue comparing implementation with [PLAN.md](./PLAN.md) and verify the roadmap acceptance criteria before declaring a phase complete.

## Continuation and commits

- Keep the main branch compiling and tested after each logical implementation step.
- Commit each independently completed implementation step with its tests and directly related documentation.
- SQLite schema version is now 2. Build numbers are allocated with an atomic row update in the same transaction that inserts a queued build; a failed insert rolls back the allocation.
