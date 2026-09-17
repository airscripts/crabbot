# AGENTS.md

## Scope

- Path: crabbot-plugins/intelligence/claude
- Parent: crabbot-plugins
- Inheritance: additive

## Mission And Repository Map

Provide the `model` and `vision` capabilities through Anthropic Messages and
streaming responses.

## Non-Negotiables

- Read credentials only from declared `CRABBOT_CLAUDE_KEY` sources.
- Keep HTTPS requests bounded and normalize provider payloads at the boundary.
- Preserve image confinement and never call paid APIs in tests.

## Quick Start

```bash
cargo test --locked -p crabbot-plugin-claude
cargo build --locked -p crabbot-plugin-claude
```

## Implementation Conventions

Keep vendor wire types inside this plugin, validate response shapes, and emit
only core protocol messages on stdout.

## Testing And Validation

Use local HTTP fixtures for text, image, streaming, malformed response,
credential, timeout, and provider-error paths.

## Free Region

Keep Anthropic-specific behavior here and update the plugin README when model,
credential, or image behavior changes.

## Further Context

See the parent plugin guidance and [README.md](README.md).

---

> Generated and maintained by [Agentskill](https://github.com/airscripts/agentskill).
> Do not touch this file. It is automatically managed by Agentskill.
