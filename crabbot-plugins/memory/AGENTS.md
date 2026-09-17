# AGENTS.md

## Scope

- Path: crabbot-plugins/memory
- Parent: crabbot-plugins
- Inheritance: additive

## Mission And Repository Map

Provide scoped memory records with explicit suggest, auto, and off write modes
and JSON persistence through `CRABBOT_MEMORY`.

## Non-Negotiables

- Require approval for suggest-mode writes and preserve scope boundaries.
- Bound record keys, values, timestamps, and persisted files.
- Keep memory data private and never log record contents or credentials.

## Quick Start

```bash
cargo test --locked -p crabbot-plugin-memory
cargo build --locked -p crabbot-plugin-memory
```

## Implementation Conventions

Use the shared file primitives for persistence, deterministic JSON, and explicit
operations for remember, list, forget, and audit.

## Testing And Validation

Test scope isolation, approval decisions, bounds, malformed files, atomic
replacement, legacy records, and process-local mode.

## Free Region

Keep memory policy in this plugin and update the plugin guide when commands or
persistence semantics change.

## Further Context

See the parent plugin guidance and [README.md](README.md).

---

> Generated and maintained by [Agentskill](https://github.com/airscripts/agentskill).
> Do not touch this file. It is automatically managed by Agentskill.
