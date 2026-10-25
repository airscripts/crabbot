# AGENTS.md

## Scope

- Path: krabb-plugins
- Parent: .
- Inheritance: additive

## Mission And Repository Map

Each plugin crate is one process with a manifest, README, and binary. The
`intelligence/` and `messaging/` directories group provider and channel
plugins; manifests, not folder names, declare each plugin capability. The
runtime loads plugins through the public core JSON-RPC contract.

## Non-Negotiables

- Use the public core JSON-RPC protocol with JSONL framing; do not create
  private host paths.
- Keep stdout protocol-only and put logs on stderr.
- Declare capabilities, permissions, and protocol ranges in the manifest.
- Do not put credentials in source, tests, manifests, or README examples.

## Quick Start

```bash
cargo test --workspace
cargo build -p krabb-plugin-codex --locked
```

## Implementation Conventions

Use one-word plugin IDs, simple names, typed errors, bounded requests, and
comments only when explaining a non-obvious reason.

## Testing And Validation

Test handshake, unknown methods, malformed requests, provider errors, timeout,
and deterministic fake responses. Never call paid APIs in CI.

## Free Region

Keep each plugin independently understandable and update its README install
line when release packaging changes.

See [AGENTS.reference.md](AGENTS.reference.md).

---

> Generated and maintained by [Agentskill](https://github.com/airscripts/agentskill).
> Do not touch this file. It is automatically managed by Agentskill.
