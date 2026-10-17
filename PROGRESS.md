# Development Progress

## Current handoff

The latest session implemented the Phase 2 SQLite setup/migration slice:

- Moved the initial schema into embedded `migrations/0001_initial.sql`.
- Applied schema changes transactionally and tracked with SQLite `user_version`; startup rejects a database created by a newer schema version.
- Configured a five-second SQLite busy timeout alongside WAL and foreign-key enforcement.
- Added tests for schema migration idempotency, newer-version rejection, and connection pragmas.

The immediately preceding session fixed Axum routing in `src/server.rs`: it changed the captured route segment to `{job_id}` and unwraps the extracted path before looking up the job. That fixed compilation and router construction.

Validation passed: `cargo test --all-targets` (30 tests) and `cargo clippy --all-targets --all-features -- -D warnings`. Targeted storage tests passed (5 tests), and `cargo fmt --check` passed after formatting.

## Project status

The project plan and README are the source of truth for scope and intended behavior. The README lists existing work in pipeline validation, domain state transitions, SQLite persistence, Git checkout, step execution, scheduling, API validation, and CLI bootstrap. No full roadmap phase is being declared complete here.

The HTTP server currently has in-memory job storage. Next roadmap slice: complete SQLite job/build CRUD and transactional build-number allocation, then add restart/claimer tests as required by Phase 2. Continue comparing implementation with [PLAN.md](./PLAN.md); do not declare a whole phase complete until its acceptance criteria are met.

## Continuation and commits

- Keep the main branch compiling and tested after each logical implementation step.
- Commit each independently completed implementation step with its tests and directly related documentation.
- The SQLite migration/setup change and this handoff update are intended to be committed together as one roadmap step.
- SQLite now applies the embedded initial schema migration transactionally, tracks schema version with `user_version`, and sets a five-second busy timeout with WAL and foreign keys enabled.
