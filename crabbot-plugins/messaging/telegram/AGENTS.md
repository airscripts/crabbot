# AGENTS.md

## Scope

- Path: crabbot-plugins/messaging/telegram
- Parent: crabbot-plugins
- Inheritance: additive

## Mission And Repository Map

Provide Telegram long-polling channel handling for direct and group messages,
normalized text, images, voice, and bounded text files.

## Non-Negotiables

- Read only `CRABBOT_TELEGRAM_TOKEN` and preserve keyring behavior.
- Keep media in the confined cache and enforce aggregate file bounds.
- Acknowledge offsets only after host acceptance; never call Telegram in CI.

## Quick Start

```bash
cargo test --locked -p crabbot-plugin-telegram
cargo build --locked -p crabbot-plugin-telegram
```

## Implementation Conventions

Keep Telegram API payloads local, normalize media references at the boundary,
and preserve bounded chunking and edit delivery behavior.

## Testing And Validation

Use local HTTP fixtures for polling, offsets, media, captions, voice fallback,
documents, edits, retries, malformed responses, and limits.

## Free Region

Keep Telegram-specific policy here and update its README for public media or
message behavior changes.

## Further Context

See the parent plugin guidance and [README.md](README.md).

---

> Generated and maintained by [Agentskill](https://github.com/airscripts/agentskill).
> Do not touch this file. It is automatically managed by Agentskill.
