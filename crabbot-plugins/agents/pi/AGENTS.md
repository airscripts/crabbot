# AGENTS.md

## Scope

- Path: crabbot-plugins/agents/pi
- Parent: crabbot-plugins
- Inheritance: additive

## Mission And Repository Map

Provide the `agent` capability and `crabbot code` command through Pi's
headless coding-agent harness. Crabbot owns session and workspace policy; Pi
performs the coding turn.

## Non-Negotiables

- Keep the manifest ID and binary name `pi` / `crabbot-plugin-pi` aligned.
- Validate session arguments and tool-bridge authorization.
- Keep protocol frames on stdout and diagnostics on stderr.
- Do not call a live Pi installation from automated tests.

## Quick Start

```bash
cargo test --locked -p crabbot-plugin-pi
cargo build --locked -p crabbot-plugin-pi
```

## Implementation Conventions

Use bounded RPC reads, explicit process failure handling, and the configured
`CRABBOT_PI_COMMAND`. Preserve the plugin manifest command contract.

## Testing And Validation

Test argument rejection, authentication, streamed output, completed messages,
and tool-bridge denial with deterministic local fixtures.

## Free Region

Keep Pi integration behind the plugin boundary and update its README and
release matrix when the command or binary contract changes.

## Further Context

See the parent plugin guidance and [README.md](README.md).

---

> Generated and maintained by [Agentskill](https://github.com/airscripts/agentskill).
> Do not touch this file. It is automatically managed by Agentskill.
