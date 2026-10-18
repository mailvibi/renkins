# Renkins

Renkins is a Rust-based single-node CI engine prototype inspired by the first-release constraints in the project plan. The current implementation focuses on the contract and durability primitives that matter most for the first release: strict pipeline validation, job/build state logic, and SQLite-backed persistence.

## What is implemented

- Validated `.renkins.yml` pipeline parsing for version 1
- Build state transition rules for queued, preparing, running, cancelling, and terminal states
- SQLite schema creation and job/build persistence in a local database
- Local Git ref validation and checkout into an isolated workspace for fully-qualified refs and full commit IDs
- Git clone/checkout commands run without interactive prompts, have bounded execution time, and terminate their process groups on timeout
- Ordered shell step execution with timeout handling and append-only log output
- Bounded in-memory queue scheduling with concurrency limits and cancellation handling
- Restart recovery and graceful shutdown bookkeeping for interrupted builds and queued work
- API request validation for job creation, build enqueue, and log retrieval
- Authenticated HTTP job create/list/detail, enable/disable, durable enqueue, build history, and build detail backed by SQLite
- A single durable worker that claims queued builds, checks out the requested ref, records the resolved commit and validated pipeline snapshot, runs steps, and persists outcomes
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

The server currently runs one polling worker. It claims builds durably and does not execute repository commands unless the validated pipeline snapshot and ordered step rows have committed. Build details include the resolved commit and persisted step outcomes. Cancellation, configurable worker concurrency, graceful shutdown, scheduler-aware readiness, and data-directory ownership/root checks are not implemented yet. Do not expose this prototype to untrusted repositories or treat it as production-ready.

Build logs are stored under `data/logs/{build_id}.rkl` as versioned records with timestamps, step identity, stream, bounded raw payloads, and CRC-32 checksums. `GET /api/v1/builds/{build_id}/logs` accepts the opaque `cursor` returned by the previous response and an optional `limit` from 1 to 100. Responses include complete records only, base64-encode payload bytes, cap raw payload data per response, and report `log_integrity`; corrupt logs include a `log_corrupt` warning, incomplete terminal tails are truncated and reported as `recovered_tail`, and missing terminal logs return `410 log_expired`. The endpoint requires the administrator bearer token.
