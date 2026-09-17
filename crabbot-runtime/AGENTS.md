# AGENTS.md

## Scope

- Path: crabbot-runtime
- Parent: .
- Inheritance: additive

## Mission And Repository Map

Own the host runtime shared by the CLI and daemon: configuration, authenticated
IPC, plugin discovery and lifecycle, sessions, channel bridging, services, and
safe updates.

## Non-Negotiables

- Keep optional provider and channel behavior in external plugins.
- Route plugin communication through the core JSON-RPC contract.
- Preserve bounded async work, cancellation, locking, staging, and rollback.
- Keep filesystem and credential handling confined and redacted.

## Quick Start

```bash
cargo test --locked -p crabbot-runtime
cargo clippy --locked -p crabbot-runtime --all-targets -- -D warnings
```

## Implementation Conventions

Use explicit lifecycle phases, typed errors, authenticated local IPC, and
small helpers. Do not duplicate CLI or daemon entrypoint logic here.

## Testing And Validation

Cover plugin admission, malformed frames, leases, sessions, delivery, update
rollback, service definitions, and absent-daemon fallbacks with local fixtures.

## Free Region

Keep runtime changes focused on host orchestration and update architecture docs
when lifecycle or IPC contracts change.

## Further Context

See the root guidance and [README.md](README.md).

---

> Generated and maintained by [Agentskill](https://github.com/airscripts/agentskill).
> Do not touch this file. It is automatically managed by Agentskill.
