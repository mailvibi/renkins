# Development Progress

## Current handoff

The latest session connected core HTTP operations to SQLite:

- Server startup now opens the data directory's `renkins.sqlite3`, applies migrations, and recovers abandoned active builds before serving.
- Authenticated job creation/listing and build enqueue now use the persistent store; build IDs are unique, job defaults apply when request refs are omitted, and disabled jobs reject enqueue.
- API build refs now require a fully qualified branch/tag ref or full commit ID.
- Added an HTTP persistence test that creates a job and build, reopens SQLite, and verifies the records and queued status survive.
- Validation passed: `cargo test --all-targets` (40 tests), `cargo clippy --all-targets --all-features -- -D warnings`, and `cargo fmt --check`.

The preceding persistence steps implemented schema migrations, durable job/build storage, conditional claims, plan persistence, and abandoned-build recovery; those are committed separately.

The earlier server fix in `src/server.rs` changed the captured route segment to `{job_id}` and unwraps the extracted path before job lookup. It fixed compilation and router construction.

## Project status

The project plan and README are the source of truth for intended behavior. No full roadmap phase is declared complete here.

The HTTP API now persists job creation and build enqueue, but there is not yet a durable scheduler worker dispatching builds, nor build detail/history/cancel/log routes. Readiness still does not reflect scheduler state. Next roadmap slice: connect a worker loop to durable claim, checkout, pipeline snapshot, step execution, and terminal persistence; then add build inspection/cancellation and readiness signals. Continue comparing implementation with [PLAN.md](./PLAN.md) and verify acceptance criteria before declaring a phase complete.

## Continuation and commits

- Keep the main branch compiling and tested after each logical implementation step.
- Commit each independently completed implementation step with its tests and directly related documentation.
- SQLite schema version is 4. Server startup invokes persisted recovery; API job creation/listing and enqueue use SQLite. This remains a partial integration, not completion of scheduler or Phase 2/6.
