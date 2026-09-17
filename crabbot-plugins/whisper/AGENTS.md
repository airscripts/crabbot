# AGENTS.md

## Scope

- Path: crabbot-plugins/whisper
- Parent: crabbot-plugins
- Inheritance: additive

## Mission And Repository Map

Provide the optional `speech` capability by invoking a local
Whisper-compatible executable for bounded media transcription.

## Non-Negotiables

- Accept media only from the confined `CRABBOT_MEDIA` area.
- Bound audio size, process arguments, output, and deadlines.
- Remove raw voice files only after successful transcription and never call a
  hosted speech API.

## Quick Start

```bash
cargo test --locked -p crabbot-plugin-whisper
cargo build --locked -p crabbot-plugin-whisper
```

## Implementation Conventions

Keep executable selection in `CRABBOT_WHISPER_COMMAND`, isolate process
diagnostics from protocol stdout, and normalize returned text.

## Testing And Validation

Use a local fake executable for success, malformed output, timeout, failure,
path confinement, size limits, and cleanup behavior.

## Free Region

Keep speech runtime integration here and update media documentation when the
transcription contract changes.

## Further Context

See the parent plugin guidance and [README.md](README.md).

---

> Generated and maintained by [Agentskill](https://github.com/airscripts/agentskill).
> Do not touch this file. It is automatically managed by Agentskill.
