# AGENTS.md

## Scope

- Path: crabbot-plugins/intelligence/gemini
- Parent: crabbot-plugins
- Inheritance: additive

## Mission And Repository Map

Provide Gemini Developer API `model` and `vision` capabilities through a
bounded HTTPS adapter.

## Non-Negotiables

- Read only the declared `CRABBOT_GEMINI_KEY` credential.
- Keep the default endpoint HTTPS and allow loopback endpoints only for tests.
- Normalize responses and never call paid services in CI.

## Quick Start

```bash
cargo test --locked -p crabbot-plugin-gemini
cargo build --locked -p crabbot-plugin-gemini
```

## Implementation Conventions

Keep Gemini request and response shapes local to the plugin, bound bodies and
timeouts, and emit only core protocol frames on stdout.

## Testing And Validation

Use a local fixture via `CRABBOT_GEMINI_BASE_URL` for text, images, errors,
oversized responses, and credential failures.

## Free Region

Keep Gemini-specific behavior here and update its README when endpoint or model
selection behavior changes.

## Further Context

See the parent plugin guidance and [README.md](README.md).

---

> Generated and maintained by [Agentskill](https://github.com/airscripts/agentskill).
> Do not touch this file. It is automatically managed by Agentskill.
