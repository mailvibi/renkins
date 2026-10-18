# Development Progress

## Current handoff

The durable worker execution slice is now implemented:

- Server startup launches one polling worker sharing the SQLite store with the API. Claims and metadata operations hold the store lock only for storage calls; checkout and shell execution run without holding it.
- The worker claims queued builds, creates a per-build checkout and log, checks out the requested ref, and records the resolved commit.
- It validates `.renkins.yml` and uses `persist_pipeline_and_start`; step execution begins only after that transaction commits successfully.
- Step execution applies each step's working directory and environment, the build-wide timeout, and any shorter per-step timeout. Step outcomes and terminal state/failure kind are persisted.
- Build detail returns the resolved commit and persisted step outcomes.
- Added schema migration 5 for build start/resolved-commit and per-step completion/exit-code fields.
- Added worker tests for success, invalid pipeline handling, and an injected SQLite trigger failure proving no step command runs before pipeline persistence.

Validation passed: `cargo test --all-targets` (49 tests), `cargo clippy --all-targets --all-features -- -D warnings`, `cargo fmt --check`, and `git diff --check`.

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

The worker is currently a single polling loop with no configured concurrency or graceful shutdown. The HTTP API does not yet provide cancellation or log retrieval; although logs are written to per-build files, there is no bounded read/tail API or structured record format. Build-level API timing is also incomplete. Readiness does not reflect worker state. Next roadmap slice: persist and expose richer build/step timing and log metadata, then implement cancellation and bounded log retrieval. Follow with configurable worker concurrency and graceful shutdown, scheduler-aware readiness, data-directory ownership locking/root rejection, and Git credential/operator documentation. Continue comparing implementation with [PLAN.md](./PLAN.md) and verify acceptance criteria before declaring a phase complete.

## Continuation and commits

- Keep the main branch compiling and tested after each logical implementation step.
- Commit each independently completed implementation step with its tests and directly related documentation.
- Same-process-group cleanup does not contain deliberately detached daemons. SQLite schema version is 5. Server startup invokes persisted recovery; job/build APIs and the single worker use SQLite. This remains partial coverage of Phase 6, not completion of the scheduler.
