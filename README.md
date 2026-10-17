# Renkins

Renkins is a Rust-based single-node CI engine prototype inspired by the first-release constraints in the project plan. The current implementation focuses on the contract and durability primitives that matter most for the first release: strict pipeline validation, job/build state logic, and SQLite-backed persistence.

## What is implemented

- Validated `.renkins.yml` pipeline parsing for version 1
- Build state transition rules for queued, preparing, running, cancelling, and terminal states
- SQLite schema creation and job/build persistence in a local database
- Local Git ref validation and checkout into an isolated workspace for fully-qualified refs and full commit IDs
- Ordered shell step execution with timeout handling and append-only log output
- Bounded in-memory queue scheduling with concurrency limits and cancellation handling
- Restart recovery and graceful shutdown bookkeeping for interrupted builds and queued work
- API request validation for job creation, build enqueue, and log retrieval
- Authenticated HTTP job create/list/detail, enable/disable, durable enqueue, build history, and build detail backed by SQLite
- CLI bootstrap with `renkins --help`

## Local development

```bash
cargo fmt
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets
cargo run -- --help
```

## Trust model and current limitations

This is not a Jenkins-compatible replacement. It is intentionally limited to a single-node, localhost-first CI flow with a dedicated service account and minimal execution surface. The design and first implementation still require the remaining executor, scheduler, auth, and network hardening work from the plan before it is safe to run untrusted repositories.
