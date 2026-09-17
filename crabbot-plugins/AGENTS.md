# AGENTS.md

## Scope

- Path: crabbot-plugins
- Parent: .
- Inheritance: additive

## Mission And Repository Map

Each child is one capability process with a manifest, README, and binary. Keep
provider or channel behavior inside its own child.

## Non-Negotiables

- Use the public core protocol; do not create private host paths.
- Keep stdout protocol-only and put logs on stderr.
- Declare capabilities, permissions, and protocol ranges in the manifest.
- Do not put credentials in source, tests, manifests, or README examples.

## Quick Start

```bash
cargo test --workspace
cargo build -p crabbot-plugin-openai
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
