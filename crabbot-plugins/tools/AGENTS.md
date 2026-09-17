# AGENTS.md

## Scope

- Path: crabbot-plugins/tools
- Parent: crabbot-plugins
- Inheritance: additive

## Mission And Repository Map

Provide confined filesystem, search, patch, Git, and explicitly approved shell
tools for the active workspace.

## Non-Negotiables

- Confine every path to `CRABBOT_ROOT` or the active isolated workspace.
- Reject symlink and reparse-point escapes and require approvals for mutation.
- Keep shell disabled by default and use only the configured local sandbox.

## Quick Start

```bash
cargo test --locked -p crabbot-plugin-tools
cargo build --locked -p crabbot-plugin-tools
```

## Implementation Conventions

Use descriptor-relative no-follow access on Unix and canonical safe handles on
Windows. Bound searches, patches, command output, resources, and processes.

## Testing And Validation

Test path escapes, symlinks, approvals, shell sandboxing, Git boundaries,
resource limits, cancellation, malformed requests, and error redaction.

## Free Region

Keep tool policy inside this plugin and update safety documentation for any
permission or sandbox behavior change.

## Further Context

See the parent plugin guidance and [README.md](README.md).

---

> Generated and maintained by [Agentskill](https://github.com/airscripts/agentskill).
> Do not touch this file. It is automatically managed by Agentskill.
