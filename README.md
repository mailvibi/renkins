# Renkins

Renkins is a Rust-based single-node CI engine prototype inspired by the first-release constraints in the project plan. The current implementation focuses on the contract and durability primitives that matter most for the first release: strict pipeline validation, job/build state logic, and SQLite-backed persistence.

## What is implemented

- Validated `.renkins.yml` pipeline parsing for version 1
- Build state transition rules for queued, preparing, running, cancelling, and terminal states
- SQLite schema creation and job/build persistence in a local database
- Local Git ref validation and checkout into an isolated workspace for fully-qualified refs and full commit IDs
- Git clone/checkout commands run without interactive prompts, have bounded execution time, and terminate their process groups on timeout
- Ordered shell step execution with timeout handling and append-only log output
- Bounded durable worker pool with configurable concurrency and graceful shutdown
- Restart recovery for interrupted builds and queued work
- Separate public liveness and readiness endpoints; readiness checks the database schema/query path and worker-pool startup state
- API request validation for job creation, build enqueue, and log retrieval
- Authenticated HTTP job create/list/detail, enable/disable, durable enqueue, build history, and build detail backed by SQLite
- Durable workers that claim queued builds, check out the requested ref, record the resolved commit and validated pipeline snapshot, run steps, and persist outcomes
- Versioned, checksummed append-only build logs with an authenticated, cursor-paginated log API and explicit corruption/expired-log responses
- CLI bootstrap with `renkins --help`

## Local development

```bash
cargo fmt
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets
cargo run -- --help
```

## Trust model and current limitations

This is not a Jenkins-compatible replacement. It is intentionally limited to a single-node, localhost-first CI flow with a dedicated service account and minimal execution surface. Commands run as that service account, not in a security sandbox. Git clone is limited to five minutes and other Git operations to thirty seconds; timed-out Git children are terminated with their process groups. The step executor likewise terminates children in its process group, but deliberately detached processes are outside that cleanup guarantee and are unsupported.

The server runs a bounded polling worker pool. `max_concurrent_builds` defaults to 1 and is limited to 32; workers claim builds durably and do not execute repository commands unless the validated pipeline snapshot and ordered step rows have committed. On Ctrl-C or SIGTERM, the server stops new claims, waits up to `shutdown_grace_seconds` (default 30, maximum 3600), then cancels remaining active builds and terminates their process groups; queued builds stay queued. `/readyz` returns ready only after the database schema/query check succeeds and the worker pool starts; it becomes not-ready during shutdown. Build details include the resolved commit, persisted step outcomes, build/step start and finish timestamps, and durations where both endpoints exist. API timestamps are UTC Unix milliseconds; build timing starts when the worker claims the build, and step timing measures shell execution through process cleanup and output draining. Build cancellation records whether it was requested by the user or server shutdown. Data-directory ownership/root checks are not implemented yet. Do not expose this prototype to untrusted repositories or treat it as production-ready.

Build logs are stored under `data/logs/{build_id}.rkl` as versioned records with timestamps, step identity, stream, bounded raw payloads, and CRC-32 checksums. `GET /api/v1/builds/{build_id}/logs` accepts the opaque `cursor` returned by the previous response and an optional `limit` from 1 to 100. Responses include complete records only, base64-encode payload bytes, cap raw payload data per response, and report `log_integrity`; corrupt logs include a `log_corrupt` warning, incomplete terminal tails are truncated and reported as `recovered_tail`, and missing terminal logs return `410 log_expired`. The endpoint requires the administrator bearer token.
