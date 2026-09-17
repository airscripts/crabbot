# AGENTS.md

## Scope

- Path: crabbot-plugins/messaging/slack
- Parent: crabbot-plugins
- Inheritance: additive

## Mission And Repository Map

Provide Slack channel handling through Socket Mode and bounded Web API history,
with normalized text, images, voice, and text files.

## Non-Negotiables

- Keep bot and app credentials in declared `CRABBOT_SLACK_*` secrets.
- Preserve channel allowlisting and attachment confinement.
- Bound downloads and retries; never call Slack in automated tests.

## Quick Start

```bash
cargo test --locked -p crabbot-plugin-slack
cargo build --locked -p crabbot-plugin-slack
```

## Implementation Conventions

Keep Slack wire payloads local, normalize events before the host boundary, and
preserve Socket Mode acknowledgement and delivery semantics.

## Testing And Validation

Use local WebSocket and HTTP fixtures for events, history polling, attachments,
chunking, retries, malformed payloads, and credential failures.

## Free Region

Keep Slack-specific routing here and update the channel guide for API or
attachment changes.

## Further Context

See the parent plugin guidance and [README.md](README.md).

---

> Generated and maintained by [Agentskill](https://github.com/airscripts/agentskill).
> Do not touch this file. It is automatically managed by Agentskill.
