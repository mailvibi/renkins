# Development Progress

## Current handoff

The latest session completed persistent job/build inspection routes:

- Added authenticated job detail, enable/disable, paginated build history, and build detail routes backed by SQLite.
- Added bounded `limit`/`offset` query handling for job and build listings; invalid page limits receive stable `invalid_pagination` JSON errors.
- Build detail exposes persisted pipeline snapshot when one exists; build history returns stable status strings.
- Added route assertions for history/detail, job disable/enable, and invalid pagination.
- Validation passed: `cargo test --all-targets` (40 tests), `cargo clippy --all-targets --all-features -- -D warnings`, and `cargo fmt --check`.

Earlier committed work connected create/list/enqueue routes to SQLite and performs schema migrations plus abandoned-build recovery before server startup.

The earlier server fix in `src/server.rs` changed the captured route segment to `{job_id}` and unwraps the extracted path before job lookup. It fixed compilation and router construction.

## Project status

The project plan and README are the source of truth for intended behavior. No full roadmap phase is declared complete here.

The HTTP API does not yet provide cancellation or log retrieval, and no durable scheduler worker dispatches queued builds. Build details do not yet contain full step outcomes/timing. Readiness still does not reflect scheduler state. Next roadmap slice: connect a worker loop to durable claim, checkout, pipeline snapshot, step execution, and terminal persistence with safe process-group cleanup. Continue comparing implementation with [PLAN.md](./PLAN.md) and verify acceptance criteria before declaring a phase complete.

## Continuation and commits

- Keep the main branch compiling and tested after each logical implementation step.
- Commit each independently completed implementation step with its tests and directly related documentation.
- SQLite schema version is 4. Server startup invokes persisted recovery; job and build inspection/control routes implemented so far use SQLite. This remains partial coverage of Phase 6, not completion of the scheduler.
