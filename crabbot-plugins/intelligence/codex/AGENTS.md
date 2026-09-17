# AGENTS.md

## Scope

- Path: crabbot-plugins/intelligence/codex
- Parent: crabbot-plugins
- Inheritance: additive

## Mission And Repository Map

Provide `model` and `vision` capabilities through OpenAI-compatible requests
and the Codex app-server integration for ChatGPT sign-in.

## Non-Negotiables

- Keep API-key and Codex-managed authentication paths separate.
- Do not expose, persist, or log OAuth tokens in Crabbot.
- Preserve bounded app-server operations and fail closed on protocol changes.

## Quick Start

```bash
cargo test --locked -p crabbot-plugin-codex
cargo build --locked -p crabbot-plugin-codex
```

## Implementation Conventions

Keep Codex wire behavior inside this plugin, use `CRABBOT_CODEX_HOME` and
`CRABBOT_CODEX_BINARY`, and emit normalized core messages.

## Testing And Validation

Use deterministic HTTP and app-server fixtures for auth, images, streaming,
malformed frames, timeouts, and unsupported operations.

## Free Region

Keep Codex compatibility capability-based rather than pinning a CLI release;
update the provider guide when the app-server contract changes.

## Further Context

See the parent plugin guidance and [README.md](README.md).

---

> Generated and maintained by [Agentskill](https://github.com/airscripts/agentskill).
> Do not touch this file. It is automatically managed by Agentskill.
