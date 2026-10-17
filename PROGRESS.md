# Development Progress

## Current handoff

The latest session added durable startup recovery:

- Added migration `0004_build_recovery.sql` with build finish time and failure kind fields.
- Added recovery that marks `preparing`, `running`, and `cancelling` builds as `interrupted` with `server_crash`, while preserving queued builds.
- Terminal build transitions now persist `finished_at`.
- Tested recovery after reopening the database, including preparing and cancelling builds plus subsequent claiming of queued work.
- Validation passed: `cargo test --all-targets` (39 tests), `cargo clippy --all-targets --all-features -- -D warnings`, and `cargo fmt --check`.

The preceding persistence steps added transactional build-number allocation, job/build lookup and bounded listing, conditional queue claiming, and validated pipeline snapshot plus pending-step persistence before entering `running`. They are committed separately.

The earlier server fix in `src/server.rs` changed the captured route segment to `{job_id}` and unwraps the extracted path before job lookup. It fixed compilation and router construction.

## Project status

The project plan and README are the source of truth for intended behavior. No full roadmap phase is declared complete here.

The HTTP server and in-memory scheduler still are not wired to `DatabaseStore`, so the new recovery operation is not yet called during actual server startup. Next roadmap slice: integrate durable storage into server/scheduler startup, enqueue, claim, execution, and readiness flows, then cover recovery end to end. Continue comparing implementation with [PLAN.md](./PLAN.md) and verify the roadmap acceptance criteria before declaring a phase complete.

## Continuation and commits

- Keep the main branch compiling and tested after each logical implementation step.
- Commit each independently completed implementation step with its tests and directly related documentation.
- SQLite schema version is now 4. Build numbers are allocated transactionally; claim transitions and the snapshot/steps-to-running transition are atomic, and active builds can be marked interrupted during startup recovery.
