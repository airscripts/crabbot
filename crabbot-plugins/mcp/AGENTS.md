# AGENTS.md

## Scope

- Path: crabbot-plugins/mcp
- Parent: crabbot-plugins
- Inheritance: additive

## Mission And Repository Map

Bridge MCP JSON-RPC requests to short-lived stdio processes or validated
Streamable HTTP endpoints.

## Non-Negotiables

- Require explicit process approval and preserve loopback/HTTPS URL checks.
- Bound requests, responses, arguments, and transport deadlines.
- Keep child-process diagnostics off the protocol stdout stream.

## Quick Start

```bash
cargo test --locked -p crabbot-plugin-mcp
cargo build --locked -p crabbot-plugin-mcp
```

## Implementation Conventions

Keep MCP wire types inside this plugin, validate transport selection, and
normalize errors before returning them to the core protocol.

## Testing And Validation

Use local stdio fixtures and loopback HTTP servers for malformed requests,
timeouts, process failures, URL policy, and response bounds.

## Free Region

Keep MCP transport behavior isolated and update the security guide for policy
changes.

## Further Context

See the parent plugin guidance and [README.md](README.md).

---

> Generated and maintained by [Agentskill](https://github.com/airscripts/agentskill).
> Do not touch this file. It is automatically managed by Agentskill.
