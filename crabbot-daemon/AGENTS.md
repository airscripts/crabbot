# AGENTS.md

## Scope

- Path: crabbot-daemon
- Parent: .
- Inheritance: additive

## Mission And Repository Map

Own the thin foreground daemon executable. The shared host behavior lives in
`crabbot-runtime`; this package only provides the daemon process entrypoint.

## Non-Negotiables

- Keep the daemon independent from the CLI package.
- Delegate lifecycle, IPC, sessions, and plugins to `crabbot-runtime`.
- Preserve environment-based configuration and avoid secret output.

## Quick Start

```bash
cargo run -p crabbot-daemon
cargo test -p crabbot-daemon
```

## Implementation Conventions

Keep `src/main.rs` small, explicit, and free of provider or plugin behavior.
Use typed errors and preserve the runtime exit status.

## Testing And Validation

Test startup delegation and failure propagation without launching live providers.
Run `cargo check --locked -p crabbot-daemon` after entrypoint changes.

## Free Region

Keep daemon changes focused on process startup and update the service guide for
public lifecycle changes.

## Further Context

See the root guidance and [README.md](README.md).

---

> Generated and maintained by [Agentskill](https://github.com/airscripts/agentskill).
> Do not touch this file. It is automatically managed by Agentskill.
