# AGENTS.md

## Scope

- Path: crabbot-plugins/tui
- Parent: crabbot-plugins
- Inheritance: additive

## Mission And Repository Map

Provide the interactive `crabbot tui` client command over authenticated daemon
IPC for sessions, models, approvals, deliveries, memory, timers, and workspaces.

## Non-Negotiables

- Keep terminal interaction in this plugin rather than the core or runtime.
- Preserve authenticated IPC and session/workspace policy.
- Keep interactive tests deterministic and avoid requiring a real terminal.

## Quick Start

```bash
cargo test --locked -p crabbot-plugin-tui
cargo build --locked -p crabbot-plugin-tui
```

## Implementation Conventions

Keep commands explicit, bound input and output, preserve the help command, and
report daemon or capability failures without leaking state.

## Testing And Validation

Test command parsing, session controls, approvals, delivery controls, model
selection, workspace validation, and daemon-unavailable fallbacks.

## Free Region

Keep terminal UI behavior here and update the command guide for new commands or
interactive semantics.

## Further Context

See the parent plugin guidance and [README.md](README.md).

---

> Generated and maintained by [Agentskill](https://github.com/airscripts/agentskill).
> Do not touch this file. It is automatically managed by Agentskill.
