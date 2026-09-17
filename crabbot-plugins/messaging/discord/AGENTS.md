# AGENTS.md

## Scope

- Path: crabbot-plugins/messaging/discord
- Parent: crabbot-plugins
- Inheritance: additive

## Mission And Repository Map

Provide Discord channel handling through Gateway events and REST replies,
including normalized text, images, voice, and bounded text files.

## Non-Negotiables

- Keep Gateway heartbeats, cursors, reconnects, and acknowledgements durable.
- Read only `CRABBOT_DISCORD_TOKEN` and preserve channel allowlisting.
- Bound attachments and message chunks; never call Discord in automated tests.

## Quick Start

```bash
cargo test --locked -p crabbot-plugin-discord
cargo build --locked -p crabbot-plugin-discord
```

## Implementation Conventions

Keep Discord wire payloads local, normalize inbound content at the boundary,
and route generated replies through the standard delivery contract.

## Testing And Validation

Use local Gateway and REST fixtures for heartbeats, resume, attachments,
chunking, edits, malformed events, and retry behavior.

## Free Region

Keep Discord-specific behavior here and update channel documentation for public
attachment or routing changes.

## Further Context

See the parent plugin guidance and [README.md](README.md).

---

> Generated and maintained by [Agentskill](https://github.com/airscripts/agentskill).
> Do not touch this file. It is automatically managed by Agentskill.
