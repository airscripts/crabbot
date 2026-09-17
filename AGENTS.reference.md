# AGENTS.reference.md

## Provenance And Decisions

- Agentskill Version: `2.1.0`.
- Evidence Schema Version: `4`.
- Repository Revision: `03c79ff484255bb0b14ac00111d65f9239e88300`.
- Configuration: default signature enabled.
- Maintainer-Confirmed Decisions: capability-free core; process plugins over JSONL; Apache-2.0; one-word naming where clear; Lefthook pre-commit gates; Gitfleet-style releases; the implementation ledger is authoritative.
- Unresolved Uncertainty: vendor OAuth terms and the final native service implementation remain platform-specific implementation work.

## Boundaries And Ownership

`crabbot-core` owns normalized types, JSON-RPC framing, policy, and the bounded
turn loop. `crabbot-runtime` owns host orchestration, daemon state, authenticated
IPC, and plugin lifecycle, while `crabbot` and `crabbot-daemon` are thin
entrypoints. `crabbot-libs/file` owns shared filesystem primitives. Every
directory under `crabbot-plugins` is an executable capability boundary.
`crabbot-docs` is the user-facing source of truth; `crabbot-scripts` contains
release, metrics, verification, and review helpers.

## Development Workflow

Lefthook runs format, locked workspace check, and library/binary tests before a
commit. `make verify` runs formatting, Clippy, locked check, tests, coverage,
build, and metrics. Coverage uses cargo-llvm-cov without source exclusions.

## Testing Topology

Unit tests live beside Rust code. Protocol tests use in-memory JSONL fixtures.
Provider tests must use deterministic fake responses and never call live paid
APIs. Release scripts must validate `VERSION`, changelog headings, archives,
and checksums.

## Nested Scopes

Child scopes inherit this document additively:

- `crabbot-core/` — kernel, policy, types, and protocol.
- `crabbot-runtime/` — host orchestration, daemon state, IPC, and plugin lifecycle.
- `crabbot-libs/file/` — shared private and crash-safe filesystem primitives.
- `crabbot-plugins/` — independently released capability processes.
- `crabbot/` — thin host CLI entrypoint.
- `crabbot-daemon/` — thin daemon entrypoint.

## Naming

Prefer `load`, `save`, `send`, `route`, `approve`, and `retry` over redundant
compound names. Keep public artifact prefixes only where package uniqueness
requires them.

---

> Generated and maintained by [Agentskill](https://github.com/airscripts/agentskill).
> Do not touch this file. It is automatically managed by Agentskill.
