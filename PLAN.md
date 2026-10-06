# Renkins: Single-Node CI Engine Plan

## 1. Product Goal

Build a Rust CI server that accepts jobs for trusted Git repositories, checks out a requested revision, runs a configured sequence of commands on the same machine, records results and logs durably, and exposes enough API and UI to inspect and control builds.

The first release is a usable single-node CI engine, not a compatible reimplementation of Jenkins. Keep the core small and make job execution predictable. The controller and executor run in one OS process; the executor may run several builds concurrently, but all workspaces, processes, database files, and logs are local to one host.

Working name: **Renkins**.

## 2. Version-One Scope

### Must ship

- A foreground server process with configuration from a file and command-line overrides.
- A local SQLite database that survives server restarts for job definitions and build records.
- Jobs defined by a checked-in `.renkins.yml` file at a configured Git revision.
- Git clone/fetch and checkout of a branch, tag, or commit using the installed `git` executable.
- Ordered shell steps with working-directory selection, environment variables, per-step timeout, and build-wide timeout.
- A bounded, in-process worker pool. A setting controls maximum simultaneous builds; the default is one.
- Build state, timestamps, exit information, commit metadata, and append-only stdout/stderr logs.
- API to create/list/get jobs, queue/list/get builds, cancel a build, and read its logs.
- Single-administrator authentication for every API route that can inspect or control jobs/builds; only liveness/readiness endpoints are public.
- A small browser UI for jobs, build history, build status, and live-enough log viewing.
- Startup recovery that marks builds interrupted by a process/server crash and makes queued builds runnable again.
- Structured logs, health endpoint, graceful shutdown, input validation, and automated tests.

### Explicitly out of scope for version one

- Jenkinsfile or Jenkins plugin compatibility; distributed agents; remote workers; Kubernetes; container isolation; multi-tenant hosting; untrusted public pull requests; credentials vault; arbitrary plugin loading; pipeline DAGs, parallel stages, matrices, approvals, and complex artifact retention; enterprise SSO/RBAC; Windows execution; and high availability.

Do not advertise arbitrary command execution as secure isolation. Version one executes repository-controlled commands as the OS account running Renkins. Only loopback HTTP is supported in v1: startup must reject non-loopback listen addresses. Remote access requires an SSH tunnel; direct network binding, TLS termination, and reverse-proxy trust are deferred until separately designed. Run Renkins as a dedicated non-root service account and fail startup when the effective UID is root. The service account must have only the filesystem and Git access the configured jobs require.

## 3. Operating Assumptions and Decisions

- Initial supported host: Linux on a supported stable Rust toolchain. Keep OS-specific process-control code behind a small module so macOS support can be added deliberately.
- Initial security model: one administrator, no roles. Generate a high-entropy bearer token on first run, store it in a mode-0600 file under the data directory, and provide a CLI command to rotate it. Require the token in the `Authorization` header for all API access except health/readiness. The UI asks for the token and keeps it in memory only; never put it in URLs, HTML, logs, or browser persistent storage. Bind to loopback by default, reject unexpected `Host`/`Origin` values, and do not enable permissive CORS. Treat anyone with the token as able to execute arbitrary code as the service account.
- Job source: a Git repository URL and a requested ref supplied when a build is queued. The repository's `.renkins.yml` is authoritative for that build. An optional job-level pinned config revision can be considered later; do not let the web API contain a second, divergent pipeline definition.
- Git: invoke `git` with argument arrays, never interpolate URLs, refs, or paths into a shell command. Require a configured executable path or resolve `git` at startup and report a clear error if missing.
- Configuration: YAML for the repository pipeline file; TOML for the server configuration. Validate both at load time and include file/field context in errors.
- Suggested libraries, subject to a dependency/license/security check when implementation begins: Tokio, Axum, SQLx with SQLite, Serde, a maintained Serde-compatible YAML parser, Clap, tracing, UUIDs, and `tokio-util` cancellation tokens. Pin compatible versions in `Cargo.lock` and keep third-party dependencies out of the domain layer where practical.
- Data durability: SQLite is the source of truth for jobs and build metadata. Build logs and checked-out workspaces are files under a configured data directory; database rows store their relative paths. SQLite uses foreign keys, WAL mode, and a busy timeout. Document that one server process owns a data directory.
- Enforce single-process ownership with an OS-level lock held for the lifetime of the server; fail startup if another Renkins process already owns the data directory. A documentation-only single-writer rule is insufficient because log files and cleanup are also shared state.
- Keep build metadata indefinitely in version one. Apply retention to workspaces and logs only; if a log has expired, keep build history and return an explicit `log_expired` response.
- Time: persist UTC timestamps in a single documented format (integer Unix milliseconds or RFC 3339 text; choose one before schema migration 1 and use it consistently).

## 4. User Workflow

1. An operator creates a job with a name and repository URL through the API or CLI.
2. A user queues a build by supplying a fully qualified branch/tag ref or a full commit object ID; short ambiguous names are rejected.
3. The server creates a durable queued build record and returns its ID immediately.
4. A worker claims that build, creates an isolated per-build directory, fetches/checks out the ref, and reads `.renkins.yml` from that checkout.
5. The server validates the pipeline, persists its normalized snapshot and complete ordered step list, and only then executes any configured command.
6. Steps run in order. Each step's output is appended to the build log with timestamps and step identity. A nonzero exit, timeout, cancellation, checkout error, invalid config, or server interruption produces an explicit terminal result.
7. The build detail API and UI show the status, revision, timing, step outcomes, and log. A user can request cancellation while a build is running.

## 5. Architecture

Keep one binary initially, split into modules by responsibility:

```text
src/
  main.rs                 CLI parsing and process startup
  config.rs               server config parsing and validation
  api/                    HTTP routes, DTOs, request validation
  domain/                 Job, Build, Step, status and transition rules
  storage/                SQLite queries, transactions, migrations
  queue/                  durable enqueue/claim/recovery operations
  scheduler/              bounded worker pool and shutdown coordination
  executor/               checkout, workspace, step process, cancellation
  logs/                   append, read, tail and retention primitives
  pipeline/               YAML schema and validation
  web/                    embedded/static UI assets
  error.rs                internal and HTTP error mapping
```

Dependency direction: API and scheduler call domain/storage interfaces; executor consumes a validated immutable build plan; storage does not depend on HTTP or process execution. Avoid a generic plugin framework or elaborate trait graph in the first implementation. Use traits only where they make deterministic testing or a real boundary materially easier.

### Request and execution path

```text
HTTP/CLI -> validate request -> SQLite transaction creates queued build
         -> scheduler wakes -> atomic claim -> create workspace
         -> git checkout -> parse/validate pipeline -> run steps
         -> append logs and update step/build rows -> terminal result
```

The scheduler is a worker loop over the database, not an in-memory-only queue. The database is authoritative after restart. Use a single dispatcher to claim at most the number of available worker slots, in a short write transaction with a conditional state update; use an immediate SQLite transaction or an equivalent serialization mechanism so concurrent claim attempts cannot race. Never hold a database transaction while running Git or user commands. Since this release uses one locked process, do not design distributed leases now.

## 6. Data Model

Use migrations from the first commit. Minimum tables:

### `jobs`

- `id`: UUID text primary key.
- `name`: required, unique after documented normalization.
- `repository_url`: required.
- `default_ref`: optional default ref; it must use the same fully qualified branch/tag or full object-ID syntax accepted for build requests.
- `next_build_number`: required positive integer, incremented in the same transaction that creates a build.
- `created_at`, `updated_at`: required UTC timestamps.
- `disabled_at`: nullable; disabled jobs reject new builds but retain history.

### `builds`

- `id`: UUID text primary key.
- `job_id`: required foreign key to jobs.
- `number`: positive per-job build number, unique with `job_id` and allocated transactionally.
- `requested_ref`: required user-supplied ref.
- `resolved_commit`: nullable until checkout succeeds.
- `status`: `queued | preparing | running | cancelling | succeeded | failed | cancelled | interrupted`.
- `queued_at`, `started_at`, `finished_at`: timestamps; nullable where not yet applicable.
- `cancel_requested_at`: nullable timestamp, set once when an active build enters `cancelling`.
- `cancel_reason`: nullable stable value such as `user` or `server_shutdown`.
- `exit_code`: nullable integer for the overall command result.
- `failure_kind`: nullable stable enum/string such as `checkout`, `pipeline_config`, `step`, `git_timeout`, `step_timeout`, `build_timeout`, `log_limit`, `server_crash`, `internal`. Cancellation outcome and cause are represented by `status` and `cancel_reason`, not `failure_kind`.
- `workspace_relpath`, `log_relpath`: relative paths under the configured data directory.
- `log_integrity`: `valid | recovered_tail | corrupt | expired`; independent of the build's execution status.
- `pipeline_snapshot`: the exact validated pipeline document or normalized representation used for this build.
- `created_at`: timestamp.

### `build_steps`

- `build_id`, `step_index`: composite primary key; build foreign key with cascade delete only if retention later deletes the build.
- `name`, `command`: captured step identity and command as configured.
- `status`: `pending | running | succeeded | failed | skipped | cancelled`.
- `started_at`, `finished_at`, `exit_code`, `failure_kind`: execution outcome fields.

Add indexes for `(job_id, queued_at DESC)`, `(status, queued_at)`, and `(build_id, step_index)`. Store no secrets in job/build rows or logs by default. Schema migrations are forward-only; back up the database before any future destructive migration.

### State transition rules

Allowed build transitions:

```text
queued -> preparing -> running -> succeeded | failed
queued -> cancelled
preparing -> failed | cancelling
running -> succeeded | failed | cancelling
cancelling -> cancelled
preparing | running | cancelling -> interrupted (detected after unclean server exit)
```

On graceful shutdown, stop accepting mutating requests and stop claiming work; let active builds finish for a configurable grace period. Then cancel remaining child process groups and persist `cancelled` with reason `server_shutdown`. On startup, any `preparing`, `running`, or `cancelling` build left by a prior process is marked `interrupted` with a failure kind and finish timestamp. Queued builds remain queued. Serialize cancellation and completion through conditional state updates: if cancellation changes an active build to `cancelling` first, completion must not overwrite it as `succeeded`; if completion wins first, cancellation returns the terminal state. A queued build can transition directly to `cancelled`. Never infer success from the presence of a workspace or a zero exit code without a completed state transition.

## 7. Pipeline File Contract

Start with a small, versioned schema. Example:

```yaml
version: 1
timeout_minutes: 30
steps:
  - name: test
    run: cargo test --locked
  - name: lint
    run: cargo clippy --all-targets -- -D warnings
```

Initial schema rules:

- `version` is required and must equal `1`.
- `timeout_minutes` is optional; when present it must be a positive integer.
- `steps` is required, contains 1 to 100 entries, and preserves file order.
- Each step has a non-empty `name` and one non-empty `run` command.
- Optional `working_directory` must be relative to the checkout and must resolve inside it after canonicalization; reject traversal and symlink escapes.
- Optional per-step `timeout_minutes` sets an upper bound for that step, capped by a server-configured maximum and the remaining build deadline.
- Optional `env` is a map of validated environment names to strings. Disallow reserved Renkins variables and avoid logging values.
- Unknown keys fail validation in version one so typos do not silently change behavior.
- No implicit retries, shell pipelines, background daemons, or cross-step parallelism beyond what the command itself starts.

The effective build timeout is `min(pipeline.timeout_minutes, server.max_build_timeout_minutes)` when the pipeline value is present, otherwise the server maximum. It is an absolute wall-clock deadline starting when a worker claims the build and covers Git operations, pipeline loading, and all steps. A step timeout is an additional upper bound, never an extension of the remaining build deadline. Stop at the first failed step and persist all later steps as `skipped`. Run each command in a documented shell (`/bin/sh -eu -c` for initial Linux support) from the build checkout or validated working directory. Commands are intentionally shell programs, so the security boundary is the authenticated administrator and trusted job/repository, not YAML parsing. Never add a second shell interpolation layer in Rust.

## 8. HTTP and CLI Surface

Version-one HTTP API, JSON unless noted:

- `GET /healthz`: liveness only; no sensitive details and no authentication required.
- `GET /readyz`: readiness only; no authentication required and no sensitive details. Return not-ready until database migrations, data-directory lock, and scheduler startup complete.
- `POST /api/v1/jobs`: create a job from `{name, repository_url, default_ref?}`; validate `default_ref` with the same strict Git-ref grammar as build requests; return `201` and the job representation.
- `GET /api/v1/jobs`: paginated job list.
- `GET /api/v1/jobs/{job_id}`: job detail.
- `POST /api/v1/jobs/{job_id}/disable`: disable a job and reject future build requests while preserving history.
- `POST /api/v1/jobs/{job_id}/enable`: re-enable a disabled job without changing its build history.
- `POST /api/v1/jobs/{job_id}/builds`: enqueue `{ref?}` using only fully qualified branch/tag refs or full commit IDs; use the job default ref if omitted; return `202` with build ID and `queued` state.
- `GET /api/v1/jobs/{job_id}/builds`: paginated build history, newest first.
- `GET /api/v1/builds/{build_id}`: build, step, and timing detail.
- `POST /api/v1/builds/{build_id}/cancel`: idempotently request cancellation; return the current state.
- `GET /api/v1/builds/{build_id}/logs`: retrieve structured log records after an opaque record cursor, with a record/byte limit; cap response size and include `log_integrity`. For corruption, return the valid prefix in a normal response with `log_integrity=corrupt` and a `log_corrupt` warning; after retention, return `410` with code `log_expired`.
- `GET /api/v1/builds/{build_id}/logs/stream`: optional server-sent events of structured log records; implement only after bounded retrieval is correct.

Use stable error JSON with a machine-readable code and human-readable message. Require bearer authentication for every API route except `/healthz` and `/readyz`; compare fixed-length token digests in constant time. Reject cross-origin requests and do not emit permissive CORS headers. Validate pagination, IDs, names, URL schemes, ref length, and body size. Do not accept arbitrary filesystem paths from clients. Add an `renkins` CLI with `serve`, `auth token-path`, `auth token rotate`, `job create/list/enable/disable`, `build run/list/show/cancel`, `logs`, and `backup create/restore` commands; CLI and HTTP should call the same application/domain operations rather than duplicate rules. CLI reads the token file with restrictive permissions and never prints it unless the explicit rotate operation requires displaying the newly generated token once. Token rotation runs offline while holding the data-directory lock and takes effect on the next server start.

## 9. Workspace, Process, and Log Rules

- Data directory layout: `data/renkins.sqlite3`, `data/workspaces/{build_id}/checkout`, and `data/logs/{build_id}.log`. Keep all stored paths relative in SQLite and resolve them against the canonical data directory.
- A build gets a unique workspace; never share a mutable checkout between builds. Create files with restrictive permissions appropriate to the service account and avoid following attacker-controlled symlinks outside the workspace.
- Validate repository URL schemes against an explicit allowlist; reject embedded user-info and redact repository URLs from logs/errors. Resolve and record the exact commit selected when the worker starts checkout; a branch may advance between enqueue and execution, so the resolved commit—not the requested ref—is the reproducibility identifier. Fetch the requested ref without putting credentials in command arguments or logs. Version one supports public HTTPS repositories and operator-preprovisioned SSH identity/agent access only; no credentials are accepted from the job API or pipeline file. SSH-backed jobs must use a dedicated read-only deploy identity scoped to the required repository, with pinned `known_hosts`, host-key checking enabled, and noninteractive SSH. Build commands run as the same OS account and must be assumed capable of exercising that account's SSH authority; omitting `SSH_AUTH_SOCK` from a step environment is not credential isolation. Credential management remains deferred.
- Accept only fully qualified branch refs (`refs/heads/...`), fully qualified tag refs (`refs/tags/...`), or full-length hexadecimal commit IDs matching the repository's object format; reject short names and abbreviated object IDs rather than guessing branch/tag precedence. Validate ref names with Git's ref rules, resolve annotated tags to their target commit, and reject objects that do not resolve to a commit. Pass refs only as process arguments, use Git's option terminator where supported, resolve to an object ID, then check out that object ID detached. Run Git with prompts disabled, a controlled `HOME`, pinned host-key verification for SSH, and system/global Git configuration disabled so service-account credential helpers or filters are not implicitly invoked. Do not recursively initialize submodules in version one.
- Spawn children without a shell around `git`; for configured step commands, use only the one explicitly documented shell invocation. Give Git only the service-account variables needed for checkout, including `SSH_AUTH_SOCK` when configured; give steps a separately constructed environment that omits Git-only variables, while documenting that the shared OS account prevents this from being a security boundary. Set a controlled working directory and construct step environment from a minimal inherited environment plus documented CI variables and step env.
- Define built-in variables such as `RENKINS_BUILD_ID`, `RENKINS_JOB_ID`, `RENKINS_BUILD_NUMBER`, `RENKINS_GIT_COMMIT`, and `RENKINS_WORKSPACE`; document exact values and stability before release.
- Stream stdout and stderr concurrently to avoid pipe deadlocks. Store append-only versioned length-prefixed records containing timestamp, step index, stream, payload length, checksum, and raw payload bytes; cap the maximum record length. Flush each complete record and call `sync_data` at least once per second and on step/build transitions. This bounds possible power-loss log loss to the documented sync interval; normal process-crash recovery must retain every complete record. API cursors address complete records and responses encode payloads as base64. The UI decodes invalid UTF-8 with replacement and renders all content as text.
- Before readiness and queue dispatch after startup, scan retained logs to the last valid record. Truncate an incomplete final record, set `log_integrity=recovered_tail`, and append a recovery marker for interrupted builds. If a checksum or frame is invalid before the final tail, preserve the valid prefix, set `log_integrity=corrupt`, and return that prefix with a `log_corrupt` warning rather than silently skipping bytes. When retention deletes a log, set `log_integrity=expired`; log retrieval returns `410` with code `log_expired`.
- Enforce output limits by configurable maximum bytes per build. Once exceeded, terminate the build with an explicit `log_limit` failure and append a final truncation marker if possible. Do not silently truncate while reporting success.
- Apply timeouts to Git and shell steps. On timeout, cancellation, or leader exit, manage the process group independently of stdout/stderr EOF: send termination, wait a short grace interval, then force-kill remaining group members before waiting for pipe readers to finish. Drain output with a bounded deadline, close readers on expiry, and reap the direct child before marking the build terminal. This does not guarantee termination of commands that deliberately detach into another session; detached daemons are unsupported and must be documented as a limitation.
- Clean old workspaces only after the build is terminal and according to explicit retention settings. Retain build metadata indefinitely; logs and workspaces have separate retention policies. Never delete a running build's workspace.

## 10. Server Configuration

Example `renkins.toml`:

```toml
listen = "127.0.0.1:8080"
data_dir = "./data"
database_file = "renkins.sqlite3"
max_concurrent_builds = 1
shutdown_grace_seconds = 20
max_build_timeout_minutes = 240
max_log_bytes_per_build = 104857600
auth_token_file = "admin-token"
workspace_retention_days = 7
log_retention_days = 30
```

Validate at startup: concurrency is positive and bounded, directories can be created and written with restrictive permissions, the process lock is acquired, database migrations complete, Git and the step shell exist, listen address parses and is loopback, the effective UID is non-root, the authentication token is present/generated, and limits are sensible. Resolve database, token, log, and workspace paths under the canonical data directory and reject path escapes. Create the token file exclusively with restrictive permissions; reject symlinks, unexpected ownership, or overly broad permissions. Support CLI overrides for config path, listen address, and data directory. Do not implement hot reload in version one. `renkins backup create/restore` requires the server to be stopped and holds the data-directory lock; backup uses SQLite's backup API and includes retained logs plus a checksummed manifest. Workspaces and the admin token are excluded; restore validates the complete backup in a staging directory, refuses an incomplete/corrupt backup without modifying the target, then installs atomically, creates a new token, and prints it once. Do not treat a raw copy of the database file while WAL is active as a supported backup.

## 11. Implementation Roadmap

Each phase is a small vertical slice. Keep the main branch compiling and tested after every phase.

### Phase 0: Pin product contract and bootstrap

1. Create `Cargo.toml`, `Cargo.lock`, `src/main.rs`, and a minimal crate/module skeleton.
2. Choose the minimum supported Rust version and edition; record them in `Cargo.toml` and CI.
3. Add formatting, lint, and test commands to CI: `cargo fmt --check`, `cargo clippy --all-targets --all-features -- -D warnings`, and `cargo test --all-targets`.
4. Add README sections for the trust model, supported host, configuration path, and local development commands.

**Acceptance:** a clean checkout builds and passes the three CI commands; `renkins --help` works; dependencies and license metadata have been reviewed.

### Phase 1: Domain types and pipeline validation

1. Implement IDs, job/build/step records, statuses, failure kinds, and transition validation without HTTP or database code.
2. Implement the version-1 YAML structures and strict validation rules from section 7.
3. Add tests for valid pipeline, missing fields, unknown keys, empty/too-many steps, invalid timeout, reserved env keys, and working-directory traversal.
4. Add property/table-driven tests proving illegal state transitions are rejected.

**Acceptance:** parsing never executes commands; invalid configuration produces field-specific errors; all transition tests pass.

### Phase 2: Persistence and migration

1. Add SQLite connection setup, WAL/foreign-key/busy-timeout settings, and an embedded migration mechanism.
2. Implement job CRUD and build creation/read/list operations with bounded pagination.
3. Implement transaction-based queue claim and conditional state transition; persist step snapshots.
4. Add startup recovery for abandoned non-terminal builds and tests using temporary SQLite databases.

**Acceptance:** integration tests prove job/build state survives closing and reopening the database; two simulated claimers cannot claim one build; migration failure prevents the server from reporting ready; a build's validated pipeline snapshot and complete step list are durable before execution can begin.

### Phase 3: Git checkout and isolated workspace

1. Implement safe data-directory/path handling and per-build workspace creation.
2. Implement Git clone/fetch/checkout through argument-based process invocation.
3. Resolve and persist the exact commit; reject invalid refs and missing `.renkins.yml` with clear failures.
4. Add tests using a temporary local Git repository, including a branch update and a missing config case.
5. Test branch/tag name collisions, annotated-tag peeling, abbreviated commit rejection, and rejection of tree/blob objects.

**Acceptance:** a test build checks out the expected commit in a unique workspace; branch/tag collisions are resolved only by fully qualified refs, and malicious-looking refs and paths cannot escape argument/path boundaries.

### Phase 4: Step executor and logs

1. Implement a process runner with explicit cwd/env, concurrent stdout/stderr draining, and append-only log writes.
2. Persist the normalized pipeline snapshot and all `pending` step rows in one transaction; only after it commits may the build enter `running` and the executor spawn the first command.
3. Implement ordered step execution and persist step/build transitions and timestamps.
4. Implement step/build timeouts, cancellation token propagation, process-group termination, exit-code handling, and log-size enforcement.
5. Add executor tests for success, nonzero exit, timeout, cancellation, output larger than a pipe buffer, output limit, and a same-group descendant that inherits stdout/stderr after its shell exits.

**Acceptance:** no deadlock on large combined output; timeout/cancel terminates the tracked process group and reaps the direct child; executor tests prove no command is spawned before the snapshot/step transaction commits; each outcome produces a stable state, failure kind, and log record.

### Phase 5: Durable scheduler and server lifecycle

1. Add one scheduler task and a configurable bounded worker pool.
2. Wake workers on enqueue, but always claim from SQLite so notifications are only hints.
3. Implement startup recovery, graceful shutdown, queue draining, and shutdown grace expiry.
4. Add concurrency tests that prove the configured limit is respected and queued builds remain runnable after restart.

**Acceptance:** with concurrency one, a second build does not execute until the first finishes; restart recovers queued work and marks previously active work interrupted; shutdown terminates tracked process groups and reaps direct children.

### Phase 6: Authentication, HTTP API, and CLI

1. Implement single-admin token bootstrap/rotation, restrictive file permissions, bearer authentication, and constant-time digest validation before exposing control routes. Rotation is an offline operation and invalidates the previous token at the next server start.
2. Add Axum routes, request/response DTOs, validation, pagination, and stable error mapping.
3. Add separate liveness/readiness endpoints; readiness checks database, process lock, and scheduler status. Reject unexpected Host/Origin values, reject non-loopback listeners at startup, and do not enable CORS.
4. Add CLI subcommands using shared application operations, including offline token rotation and backup/restore; keep tokens out of command-line arguments and process listings.
5. Add API/config integration tests for unauthenticated access denial, token rotation, origin rejection, non-loopback bind rejection, root execution rejection, happy paths, unknown IDs, invalid inputs, cancellation idempotence, and response-size limits.

**Acceptance:** a scripted CLI/API workflow can create a job, enqueue a local-repo build, poll it to completion, and retrieve its logs; control routes reject requests without a valid token; the UI uses the token without persisting it.

### Phase 7: Minimal browser UI

1. Add a job list and job detail/history view.
2. Add build detail with status, revision, timing, step summaries, cancellation control, and log view.
3. Poll build status at a modest interval; use opaque log-record cursors or SSE without repeatedly downloading the entire log.
4. Add empty, loading, error, running, and terminal states; keep all UI routes read-only except explicit create/build/cancel actions.

**Acceptance:** the full local workflow works in a browser on desktop and mobile-sized viewports; cancellation and long logs remain usable; UI never renders log text as HTML.

### Phase 8: Hardening and first release

1. Review dependency advisories, URL/ref validation, path canonicalization, child process cleanup, database locking, log bounds, and token file permissions.
2. Add end-to-end tests using a temporary local Git repository and real child commands.
3. Test process kill/restart recovery, forced termination during a log-frame append, corrupt interior log records, and disk-full/permission error reporting.
4. Require a backup/restore round-trip test that verifies build history, retained-log retrieval, `log_expired` behavior, checksummed-manifest validation, atomic failure on corrupt/incomplete backups, and regenerated token behavior.
5. Document install/build/start/backup/restore/retention and the no-untrusted-code warning.
6. Produce a release binary and record exact supported OS/toolchain and known limitations.

**Acceptance:** documented clean-machine setup and tested offline backup/restore work; crash-during-log-write recovery is covered; all automated suites pass; unauthenticated control routes are inaccessible; first release notes explicitly state the trust model and deferred features.

## 12. Test Strategy

- Unit tests: transitions, validation, timeout calculations, config parsing, path containment, error mapping.
- Database integration tests: migrations, foreign keys, transactions, queue claims, recovery, pagination.
- Process tests: exit codes, environment, cwd, concurrent pipe drainage, cancellation, timeout, same-process-group descendants, and log limits; document and test that detached sessions are outside the cleanup guarantee.
- Git integration tests: create temporary repos and commits locally; avoid network dependence in the normal test suite.
- HTTP integration tests: run the router against an ephemeral listener or in-process service and use temporary storage.
- End-to-end test: create job -> queue build -> checkout -> parse config -> persist snapshot/steps -> execute two steps -> inspect commit, statuses, and logs -> restart -> verify durable history. A failpoint after snapshot commit but before spawn must leave an interrupted build with the exact pending step plan and no command side effect. Include auth bootstrap and verify a control request without a token is rejected.
- Reliability checks: repeat cancellation and shutdown races; run tests under a temporary directory with spaces; ensure failure paths clean up handles and do not leave active rows.

## 13. Risks and Guardrails

- **Untrusted code execution:** the largest risk. Loopback binding alone does not protect against local browser attacks or other local users. Require the single-admin token even on loopback, validate Host/Origin, reject cross-origin requests, execute under a dedicated low-privilege OS account, and do not expose beyond localhost without a separate network authentication/transport review.
- **Child process leaks:** killing only the shell is insufficient. Use process groups and test same-group descendants; detached sessions can escape this mechanism, so do not claim full process-tree isolation or support detached daemons in version one.
- **Crash consistency:** write state transitions transactionally and treat filesystem artifacts as recoverable/retained data, not authority over build state. Recovery must happen before readiness or queue dispatch, and the process lock prevents two servers from reconciling/cleaning the same data directory.
- **SQLite write contention:** keep transactions short and never hold one while running Git or user commands. Surface busy/locked errors and use bounded retry only for known transient operations.
- **Unbounded disk use:** bound logs, define workspace/log retention, check free space where feasible, and report disk errors as build failures.
- **Secrets leakage:** no build-secret injection feature in v1. The administrator token is a control-plane credential; protect its file and never log or pass it in process arguments. Redaction is not a substitute for a vault; reject repository URLs with embedded credentials and avoid printing environment maps or config contents in server logs.
- **YAML/parser risk:** use a maintained parser, cap config file size and nesting/step count, reject unknown fields, and parse into typed structures before execution.
- **Compatibility expectations:** call it a Jenkins-inspired CI server, not a Jenkins replacement. Do not promise Jenkinsfile, plugin, agent, or API compatibility.

## 14. Version-One Release Checklist

- [ ] Single-node install and first-run guide is reproducible.
- [ ] Listener is loopback-only; startup rejects non-loopback binds and root execution; deployment runs as a dedicated non-root service account; a single-admin token is required even on loopback.
- [ ] Jobs, builds, statuses, and logs persist across clean restarts.
- [ ] Unclean restart recovery is tested and visible to users.
- [ ] Worker concurrency is bounded and configurable.
- [ ] Cancellation and timeout terminate the tracked child process group and reap the direct child; detached descendants are documented as unsupported.
- [ ] Git checkout records the exact commit.
- [ ] Invalid pipeline config fails before configured commands run.
- [ ] Logs and workspaces have documented bounds/retention; build metadata remains available after log expiry.
- [ ] Log integrity and crash-tail recovery are visible and tested independently of build success/failure.
- [ ] API and UI cover create, enqueue, inspect, cancel, and logs.
- [ ] Unit, integration, process, and end-to-end tests pass in CI.
- [ ] Documentation states supported platform, trust model, limitations, backup, and restore.

## 15. Decisions to Revisit After the First Usable Release

1. Multi-user identity, roles, and external authentication before any non-loopback deployment.
2. Secret storage/injection and robust log redaction.
3. Container or OS-level sandboxing for less-trusted repositories.
4. Webhooks, commit status reporting, and polling strategy.
5. Artifacts, test reports, retention controls, and cache semantics.
6. Remote agents or multiple workers only after the executor protocol and durable queue have proven stable.
7. Pipeline DAGs, parallel stages, retries, and compatibility requirements based on actual users.