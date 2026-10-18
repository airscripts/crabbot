# AGENTS.md

## Scope

- Path: crabbot-plugins/scheduler
- Parent: crabbot-plugins
- Inheritance: additive

## Mission And Repository Map

Provide durable scheduler scheduling with delays, repeats, five-field cron
expressions, IANA time zones, and atomic due claims.

## Non-Negotiables

- Keep scheduler files private and use shared crash-safe file primitives.
- Preserve atomic due claims and daylight-saving behavior.
- Bound schedule text, repeats, and persisted records.

## Quick Start

```bash
cargo test --locked -p crabbot-plugin-scheduler
cargo build --locked -p crabbot-plugin-scheduler
```

## Implementation Conventions

Use `jiff` for timezone-aware calculations, deterministic JSON persistence, and
explicit add, list, wait, due, and remove operations.

## Testing And Validation

Test one-shot and repeating schedules, cron parsing, timezone transitions, due
claims, malformed files, bounds, and process-local mode.

## Free Region

Keep scheduling policy in this plugin and update the guide when scheduler commands
or persistence semantics change.

## Further Context

See the parent plugin guidance and [README.md](README.md).

---

> Generated and maintained by [Agentskill](https://github.com/airscripts/agentskill).
> Do not touch this file. It is automatically managed by Agentskill.
