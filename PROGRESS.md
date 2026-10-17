# Development Progress

## Current handoff

The latest session hardened step process cleanup:

- Step shells now run in their own process group. Timeout terminates the process group with SIGTERM then SIGKILL, waits for the direct child, and joins both output drainers.
- When the shell exits but same-group descendants still hold stdout/stderr open, those descendants are terminated rather than leaving the build blocked on pipe EOF.
- Reader failures/panics are surfaced instead of ignored.
- Added tests for descendants surviving the shell and timeout cleanup.
- Validation passed: `cargo test --all-targets` (42 tests), `cargo clippy --all-targets --all-features -- -D warnings`, and `cargo fmt --check`.

Git operations are now prepared for worker integration:

- Clone, ref resolution, and checkout use argument-based commands and avoid UTF-8 path conversions for workspace paths.
- Git cannot wait indefinitely for interactive credentials. Clone has a five-minute timeout; other Git operations have a thirty-second timeout.
- Timed-out Git commands terminate their process group, reap the child, and join bounded stdout/stderr readers. Failures include captured stderr (capped at 64 KiB).
- The Git process environment disables system Git configuration and clears environment overrides that could redirect Git into a different repository or object store, while preserving normal user/SSH credential environment.
- Validation passed: `cargo test --all-targets` (44 tests), `cargo clippy --all-targets --all-features -- -D warnings`, and `cargo fmt --check`.

Earlier committed work completed persistent job/build inspection routes: detail, enable/disable, paginated history, and build detail.

The earlier server fix in `src/server.rs` changed the captured route segment to `{job_id}` and unwraps the extracted path before job lookup. It fixed compilation and router construction.

## Project status

The project plan and README are the source of truth for intended behavior. No full roadmap phase is declared complete here.

The HTTP API does not yet provide cancellation or log retrieval, and no durable scheduler worker dispatches queued builds. Build details do not yet contain full step outcomes/timing. Readiness still does not reflect scheduler state. Next roadmap slice: connect a worker loop to durable claim, checkout, pipeline snapshot, step execution, and terminal/step outcome persistence, reusing this process cleanup. Ensure a snapshot transaction commits before any repository command executes. Continue comparing implementation with [PLAN.md](./PLAN.md) and verify acceptance criteria before declaring a phase complete.

## Continuation and commits

- Keep the main branch compiling and tested after each logical implementation step.
- Commit each independently completed implementation step with its tests and directly related documentation.
- Same-process-group cleanup does not contain deliberately detached daemons. SQLite schema version is 4. Server startup invokes persisted recovery; job and build inspection/control routes implemented so far use SQLite. This remains partial coverage of Phase 6, not completion of the scheduler.
