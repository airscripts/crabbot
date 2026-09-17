# AGENTS.md

## Scope

- Path: crabbot-libs/file
- Parent: .
- Inheritance: additive

## Mission And Repository Map

Own the shared private, crash-safe filesystem primitives used by the runtime
and plugins. Keep this crate independent of host orchestration and providers.

## Non-Negotiables

- Preserve private-file permissions and atomic replacement semantics.
- Keep serialization bounded and deterministic.
- Reject unsafe paths and never log secret file contents.

## Quick Start

```bash
cargo test --locked -p crabbot-file
cargo clippy --locked -p crabbot-file --all-targets -- -D warnings
```

## Implementation Conventions

Use small typed APIs, explicit error propagation, and platform-specific code
only behind the narrow filesystem boundary.

## Testing And Validation

Test permissions, malformed data, interrupted replacement, path confinement,
and Windows-specific behavior without touching user files.

## Free Region

Keep filesystem policy reusable by both runtime and plugins; update callers when
the persistence contract changes.

## Further Context

See the root guidance and [Cargo.toml](Cargo.toml).

---

> Generated and maintained by [Agentskill](https://github.com/airscripts/agentskill).
> Do not touch this file. It is automatically managed by Agentskill.
