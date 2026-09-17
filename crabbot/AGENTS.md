# AGENTS.md

## Scope

- Path: crabbot
- Parent: .
- Inheritance: additive

## Mission And Repository Map

Own the `crabbot` CLI entrypoint: command parsing, setup, configuration
selection, and delegation to `crabbot-runtime`. Host lifecycle, plugin
discovery, authenticated IPC, native services, and update activation belong to
the runtime library.

## Non-Negotiables

- Keep command handlers thin and delegate host behavior to `crabbot-runtime`
  and capability behavior to plugins.
- Keep human output default and support stable JSON output where exposed.
- Never print secrets or run shell commands implicitly.
- Preserve staged update verification and rollback.

## Quick Start

```bash
cargo run -p crabbot -- version
cargo run -p crabbot -- doctor
cargo test -p crabbot
```

## Implementation Conventions

Use simple command and function names, four spaces, typed errors, explicit
confirmation for destructive changes, and comments only for reasons.

## Testing And Validation

Test CLI parsing, config safety, manifest discovery, JSON output, missing
capabilities, and update failures without external services.

## Free Region

Keep host changes focused and update the command guide for public command
changes.

See [AGENTS.reference.md](AGENTS.reference.md).

---

> Generated and maintained by [Agentskill](https://github.com/airscripts/agentskill).
> Do not touch this file. It is automatically managed by Agentskill.
