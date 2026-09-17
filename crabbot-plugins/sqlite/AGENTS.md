# AGENTS.md

## Scope

- Path: crabbot-plugins/sqlite
- Parent: crabbot-plugins
- Inheritance: additive

## Mission And Repository Map

Provide the durable SQLite `store` capability for values, leases, queues,
outbox delivery, retries, acknowledgements, and event deduplication.

## Non-Negotiables

- Keep database paths confined to configured private state.
- Preserve WAL setup, idempotent migrations, bounds, and retry accounting.
- Do not log values, credentials, or database contents.

## Quick Start

```bash
cargo test --locked -p crabbot-plugin-sqlite
cargo build --locked -p crabbot-plugin-sqlite
```

## Implementation Conventions

Use transactions for related state changes, explicit migration steps, and
bounded queries. Return normalized core protocol responses.

## Testing And Validation

Test migrations, page-size bounds, leases, queue recovery, retries, expiry,
deduplication, malformed inputs, and private-file handling.

## Free Region

Keep persistence policy inside this plugin and update the storage guide when
schema or command contracts change.

## Further Context

See the parent plugin guidance and [README.md](README.md).

---

> Generated and maintained by [Agentskill](https://github.com/airscripts/agentskill).
> Do not touch this file. It is automatically managed by Agentskill.
