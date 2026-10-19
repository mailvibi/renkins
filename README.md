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
- Configurable combined build-output limit that terminates noisy builds and records an explicit failure
- Restart recovery for interrupted builds and queued work
- Separate public liveness and readiness endpoints; readiness checks the data-directory lock, database schema/query path, and worker-pool startup state
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

## Git credentials for operators

Use public HTTPS repository URLs without embedded credentials, or SSH-style URLs such as `git@github.com:owner/repository.git`. Renkins rejects URL user-info, query strings, and fragments; do not put passwords, access tokens, or other credentials in a repository URL, job request, or `.renkins.yml`. Private HTTPS repositories that require an HTTP credential are not supported.

For private repositories, provision a dedicated read-only SSH deploy identity for the Renkins service account, scoped to only the required repository or repositories. Pre-populate that account's `known_hosts` from a trusted source. For example, its `~/.ssh/config` can select the key and known-hosts file:

```sshconfig
Host github.com
  User git
  IdentityFile /etc/renkins/ssh/repository-readonly
  IdentitiesOnly yes
  UserKnownHostsFile /etc/renkins/ssh/known_hosts
```

Protect the private key, SSH configuration, and `known_hosts` with service-account-only filesystem permissions. Git prompts are disabled; SSH is forced into batch mode and strict host-key checking. The daemon does not provide a credential-injection mechanism. It disables system and global Git configuration, but uses the service account's `HOME` for SSH configuration and known hosts; operators must provision these files correctly.

Repository checkout and pipeline commands run as the same service account. Pipeline code can therefore access that account's SSH key and any available SSH agent, so neither a dedicated deploy key nor omission of `SSH_AUTH_SOCK` from the step environment isolates credentials from repository code. Only run trusted repositories; this is not a sandbox.

## Build output limit

Set `max_build_output_bytes` in the server TOML configuration to limit the combined raw stdout and stderr recorded from all steps in one build. It defaults to 10 MiB and accepts values from 1 byte through 1 GiB. When a process produces output beyond the limit, Renkins terminates its process group, retains at most the configured number of output bytes, marks the build failed with `failure_kind=log_limit`, and appends a truncation marker when log writes remain available. Framing and build-status records are not counted toward this raw process-output limit.

## Trust model and current limitations

This is not a Jenkins-compatible replacement. It is intentionally limited to a single-node, localhost-first CI flow with a dedicated service account and minimal execution surface. Commands run as that service account, not in a security sandbox. Git clone is limited to five minutes and other Git operations to thirty seconds; timed-out Git children are terminated with their process groups. The step executor likewise terminates children in its process group, but deliberately detached processes are outside that cleanup guarantee and are unsupported.

The server refuses to run as root and acquires an exclusive OS lock on its canonical data directory; it enforces service-account ownership and restrictive directory/lock-file permissions. Configured token paths must remain inside that directory and cannot be symlinks. The server runs a bounded polling worker pool. `max_concurrent_builds` defaults to 1 and is limited to 32; workers claim builds durably and do not execute repository commands unless the validated pipeline snapshot and ordered step rows have committed. On Ctrl-C or SIGTERM, the server stops new claims, waits up to `shutdown_grace_seconds` (default 30, maximum 3600), then cancels remaining active builds and terminates their process groups; queued builds stay queued. `/readyz` returns ready only while the data-directory lock is held, the database schema/query check succeeds, and the worker pool is running; it becomes not-ready during shutdown. Build details include the resolved commit, persisted step outcomes, build/step start and finish timestamps, and durations where both endpoints exist. API timestamps are UTC Unix milliseconds; build timing starts when the worker claims the build, and step timing measures shell execution through process cleanup and output draining. Build cancellation records whether it was requested by the user or server shutdown. Do not expose this prototype to untrusted repositories or treat it as production-ready.

Build logs are stored under `data/logs/{build_id}.rkl` as versioned records with timestamps, step identity, stream, bounded raw payloads, and CRC-32 checksums. Before workers start, the server scans retained logs, truncates only incomplete final records, and persists each build's log-integrity state. Recoverable builds interrupted by a server restart receive a recovery marker after the valid log prefix; interior corruption is preserved and reported rather than skipped or rewritten. `GET /api/v1/builds/{build_id}/logs` accepts the opaque `cursor` returned by the previous response and an optional `limit` from 1 to 100. Responses include complete records only, base64-encode payload bytes, cap raw payload data per response, and report `log_integrity`; corrupt logs include a `log_corrupt` warning, incomplete terminal tails are truncated and reported as `recovered_tail`, and missing terminal logs return `410 log_expired`. The endpoint requires the administrator bearer token.
