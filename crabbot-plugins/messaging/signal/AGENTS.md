# AGENTS.md

## Scope

- Path: crabbot-plugins/messaging/signal
- Parent: crabbot-plugins
- Inheritance: additive

## Mission And Repository Map

Provide Signal channel handling through a managed `signal-cli` process with
normalized text, images, voice, and text files.

## Non-Negotiables

- Keep process arguments bounded and diagnostics off protocol stdout.
- Read account configuration only from declared `CRABBOT_SIGNAL_*` values.
- Confine attachment paths to the private media area.

## Quick Start

```bash
cargo test --locked -p crabbot-plugin-signal
cargo build --locked -p crabbot-plugin-signal
```

## Implementation Conventions

Treat `signal-cli` as an external boundary, normalize inbound and outbound
content, and report process failures as typed channel errors.

## Testing And Validation

Use deterministic local process fixtures for framing, registration failures,
attachments, replies, retries, and malformed events.

## Free Region

Keep Signal-specific process integration here and document executable overrides
when configuration changes.

## Further Context

See the parent plugin guidance and [README.md](README.md).

---

> Generated and maintained by [Agentskill](https://github.com/airscripts/agentskill).
> Do not touch this file. It is automatically managed by Agentskill.
