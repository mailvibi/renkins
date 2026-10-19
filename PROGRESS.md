# Development Progress

## Latest handoff: per-build output limits

Added validated `max_build_output_bytes` server configuration (10 MiB default, 1 byte to 1 GiB). The executor counts combined stdout/stderr across all steps, preserves no more than the configured raw-output bytes, and terminates the process group as soon as additional output is observed. The worker persists the explicit `log_limit` failure and appends a truncation marker; existing worker entry points keep their default behavior. README documents the limit and what it measures.

- Validation passed: `cargo fmt --check`, `cargo test --all-targets` (71 tests), `cargo clippy --all-targets --all-features -- -D warnings`, and `git diff --check`.

## Latest handoff: startup log recovery and integrity

Startup now scans retained `.rkl` logs after interrupted builds are marked and before readiness or worker startup. Incomplete final records are truncated and synced; interior corruption is preserved and persisted as `corrupt`; recoverable interrupted builds receive a recovery marker after the valid prefix. Marker work is tracked in SQLite so normal restarts do not append duplicates, and the logs API persists integrity changes it detects, including expired logs. Schema version is now 8.

- Validation passed: `cargo fmt --check`, `cargo test --all-targets` (69 tests), `cargo clippy --all-targets --all-features -- -D warnings`, and `git diff --check`.

## Latest handoff: Git credential boundaries

Repository URLs now accept public HTTPS or `git@host:path` syntax only, rejecting embedded user-info, query strings, fragments, malformed hosts, and empty SSH paths. Git disables system/global configuration and interactive prompts; SSH is forced to use batch mode and strict host-key checking. README guidance covers operator-provisioned read-only SSH identities, pinned `known_hosts`, and the fact that pipeline code shares the service account's key/agent authority and is not sandboxed. `HOME` remains inherited for SSH config and known-hosts lookup.

- Validation passed: `cargo fmt --check`, `cargo test --all-targets` (68 tests), `cargo clippy --all-targets --all-features -- -D warnings`, and `git diff --check`.

## Latest handoff: data-directory ownership and locking

Server startup now refuses root execution, canonicalizes and permission-hardens the data directory, verifies service-account ownership, and holds a nonblocking exclusive `flock` for the server lifetime. The configured token file must resolve to a regular non-symlink file under the canonical data directory; database, logs, and workspaces are rooted there. Added coverage for lock contention/release, restrictive permissions, root rejection, and path escapes.

- Validation passed: `cargo fmt --check`, `cargo test --all-targets` (68 tests), `cargo clippy --all-targets --all-features -- -D warnings`, and `git diff --check`.

## Latest handoff: scheduler-aware readiness

`/readyz` is now separate from `/healthz` and reports readiness only when the data-directory lock is held, the current SQLite schema/query path is available, and the bounded worker pool has started. It returns `503` before scheduler startup or after shutdown begins. Database readiness failures are logged without exposing internal details in the HTTP response.

- Validation passed: `cargo fmt --check`, `cargo test --all-targets` (65 tests), `cargo clippy --all-targets --all-features -- -D warnings`, and `git diff --check`.

## Latest handoff: worker concurrency and graceful shutdown

The server now uses a bounded worker pool configured with `max_concurrent_builds` (default 1, maximum 32) and `shutdown_grace_seconds` (default 30, maximum 3600):

- Worker claims are serialized with shutdown startup so no queued build can be claimed after shutdown begins; each claim still uses the durable SQLite transaction.
- Ctrl-C and SIGTERM stop accepting requests and stop new worker claims. Active builds get the configured grace period to finish; remaining active builds are cancelled with reason `server_shutdown`, their process groups are terminated, and queued builds remain queued.
- Worker threads are joined on shutdown and also cleaned up if pool startup fails partway through.
- Added worker setting validation and tests for one-slot serialization, multi-slot parallel execution, graceful completion, forced cancellation, and preserving queued work.
- Validation passed: `cargo fmt --check`, `cargo test --all-targets` (63 tests), `cargo clippy --all-targets --all-features -- -D warnings`, and `git diff --check`.

## Latest handoff: build and step timing

Build and step timing is persisted and included in build API responses:

- Build history and detail responses expose creation, claim/start, and finish timestamps as UTC Unix milliseconds, plus the elapsed duration for completed builds.
- Each persisted step outcome includes a start timestamp (null if execution never started), finish timestamp, and duration.
- Schema migrations 6 and 7 add step start timestamps and cancellation reason metadata; prior databases migrate forward.
- Build-detail API and worker tests verify the persisted timing fields and duration calculations.
- Validation passed: `cargo fmt --check`, `cargo test --all-targets` (58 tests), `cargo clippy --all-targets --all-features -- -D warnings`, and `git diff --check`.

## Latest handoff: build cancellation

Build cancellation is now implemented across the API, worker, Git operations, and shell executor:

- Added authenticated `POST /api/v1/builds/{build_id}/cancel`. Queued builds are cancelled immediately; repeated requests are idempotent; unknown build IDs return 404.
- Cancellation requests transition active builds to `cancelling` transactionally. The worker then finalizes them as `cancelled` and persists cancelled step outcomes.
- Active Git commands and shell steps poll a shared cancellation flag. Cancellation terminates their process groups, reaps the direct child, and joins output readers.
- Added API, worker, executor, and Git process-group cancellation coverage.
- Validation passed: `cargo fmt --check`, `cargo test --all-targets` (57 tests), `cargo clippy --all-targets --all-features -- -D warnings`, and `git diff --check`.

## Current handoff

The log-format and retrieval slice is now implemented:

- Build output is written as versioned length-prefixed records containing timestamp, optional step index, stream, payload length, CRC-32 checksum, and up to 8 KiB of raw payload.
- Stdout and stderr are drained concurrently into records; a shared per-build writer lock prevents frames from interleaving. Records flush on write and sync at least once per second and at step/build transitions.
- Added authenticated `GET /api/v1/builds/{build_id}/logs` with an opaque frame-boundary cursor, record limit (1-100), base64 payloads, and a bounded 32 KiB raw-payload page.
- Reader validates all frames, returns only complete-record cursors, reports interior corruption as `log_integrity=corrupt` with `log_corrupt`, and truncates incomplete tails only for terminal builds with `log_integrity=recovered_tail`. Missing logs for terminal builds return `410 log_expired`.
- Removed the previous placeholder API helper that returned fabricated log entries.
- API integration tests cover bearer authentication, binary output, invalid cursors, corrupt logs, and expired logs. Worker tests confirm executor output uses the framed format.
- Validation passed: `cargo test --all-targets` (53 tests), `cargo clippy --all-targets --all-features -- -D warnings`, `cargo fmt --check`, and `git diff --check`.

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

Next roadmap slice: continue the remaining API/CLI/UI and release-readiness work in `PLAN.md`, including backup/restore, retention, end-to-end restart testing, and operator setup.

## Continuation and commits

- Keep the main branch compiling and tested after each logical implementation step.
- Commit each independently completed implementation step with its tests and directly related documentation.
- Same-process-group cleanup does not contain deliberately detached daemons. SQLite schema version is 8. Server startup invokes persisted build and log recovery; job/build APIs and the worker pool use SQLite, while framed logs are stored on the filesystem. This remains partial coverage of Phase 6, not completion of the scheduler.
