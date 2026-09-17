# AGENTS.md

## Scope

- Path: crabbot-core
- Parent: .
- Inheritance: additive

## Mission And Repository Map

Own the capability-free kernel, normalized domain types, policy, JSON-RPC
protocol framing, and bounded agent loop. Plugin supervision and host lifecycle
belong to `crabbot-runtime`.

## Non-Negotiables

- Do not add providers, network clients, storage engines, tools, UI, or vendor
  payloads here.
- Keep protocol data serializable, versioned, provider-neutral, and framed as
  bounded JSON-RPC messages.
- Preserve bounded frames, deadlines, cancellation, and typed errors.

## Quick Start

```bash
cargo test -p crabbot-core
cargo clippy -p crabbot-core --all-targets -- -D warnings
```

## Implementation Conventions

Use simple names, four spaces, explicit phases, and comments only for reasons
or invariants that code cannot express.

## Testing And Validation

Test round trips, malformed JSON, frame limits, protocol compatibility, policy
denials, and interrupted turns without live services.

## Free Region

Keep changes small and update the root implementation ledger for protocol decisions.

See [AGENTS.reference.md](AGENTS.reference.md).

---

> Generated and maintained by [Agentskill](https://github.com/airscripts/agentskill).
> Do not touch this file. It is automatically managed by Agentskill.
