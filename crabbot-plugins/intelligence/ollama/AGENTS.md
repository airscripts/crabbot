# AGENTS.md

## Scope

- Path: crabbot-plugins/intelligence/ollama
- Parent: crabbot-plugins
- Inheritance: additive

## Mission And Repository Map

Provide local or cloud Ollama `model` and `vision` capabilities with bounded
stream handling.

## Non-Negotiables

- Keep host and API-key configuration in the declared `CRABBOT_OLLAMA_*`
  variables.
- Preserve image bounds, request deadlines, and response validation.
- Never require a live Ollama service in automated tests.

## Quick Start

```bash
cargo test --locked -p crabbot-plugin-ollama
cargo build --locked -p crabbot-plugin-ollama
```

## Implementation Conventions

Keep Ollama JSON-RPC/provider details inside this plugin and normalize streamed
messages before returning them to the core protocol.

## Testing And Validation

Use local HTTP fixtures for local/cloud auth, text, images, streaming,
malformed frames, and timeout behavior.

## Free Region

Keep local and cloud Ollama compatibility in this plugin and document new
configuration variables in its README.

## Further Context

See the parent plugin guidance and [README.md](README.md).

---

> Generated and maintained by [Agentskill](https://github.com/airscripts/agentskill).
> Do not touch this file. It is automatically managed by Agentskill.
