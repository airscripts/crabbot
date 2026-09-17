# AGENTS.md

## Scope

- Path: crabbot-plugins/intelligence/openrouter
- Parent: crabbot-plugins
- Inheritance: additive

## Mission And Repository Map

Provide routed `model` and `vision` capabilities through OpenRouter's
OpenAI-compatible chat-completions API.

## Non-Negotiables

- Read only declared `CRABBOT_OPENROUTER_*` credentials and routing settings.
- Keep HTTPS endpoints and loopback test overrides bounded and validated.
- Preserve streaming, image, and tool-call normalization without paid CI calls.

## Quick Start

```bash
cargo test --locked -p crabbot-plugin-openrouter
cargo build --locked -p crabbot-plugin-openrouter
```

## Implementation Conventions

Keep OpenRouter headers and wire payloads local, bound response bodies, and
emit only normalized core protocol messages.

## Testing And Validation

Use `CRABBOT_OPENROUTER_BASE_URL` with local fixtures for routing, streaming,
images, tools, malformed responses, and authentication failures.

## Free Region

Keep provider routing policy inside this plugin and document new attribution or
model variables in its README.

## Further Context

See the parent plugin guidance and [README.md](README.md).

---

> Generated and maintained by [Agentskill](https://github.com/airscripts/agentskill).
> Do not touch this file. It is automatically managed by Agentskill.
