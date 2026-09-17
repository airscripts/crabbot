# AGENTS.md

## Scope

- Path: crabbot-plugins/messaging/whatsapp
- Parent: crabbot-plugins
- Inheritance: additive

## Mission And Repository Map

Provide the official WhatsApp Cloud API channel through a local webhook and
Graph API replies for text, images, voice, and text files.

## Non-Negotiables

- Verify webhook signatures and configured verification values.
- Read only declared `CRABBOT_WHATSAPP_*` secrets and keep Graph URLs bounded.
- Confine downloaded media and never call Meta services in automated tests.

## Quick Start

```bash
cargo test --locked -p crabbot-plugin-whatsapp
cargo build --locked -p crabbot-plugin-whatsapp
```

## Implementation Conventions

Keep Meta wire payloads local, normalize webhook content before the host
boundary, and preserve bounded multipart downloads and replies.

## Testing And Validation

Use local webhook and HTTP fixtures for verification, signatures, media,
retries, malformed events, and Graph API failures.

## Free Region

Keep WhatsApp-specific webhook and Graph behavior here and update its README
when public media support changes.

## Further Context

See the parent plugin guidance and [README.md](README.md).

---

> Generated and maintained by [Agentskill](https://github.com/airscripts/agentskill).
> Do not touch this file. It is automatically managed by Agentskill.
