# Documentation Home

Crabbot is a lightweight, open-source general-purpose agent runtime that turns
models, tools, memory, and messaging into an assistant you control. It is made
from a small host, a capability-free core, and replaceable process plugins.
Start with the first-run workflow, then use
these guides when operating or extending an installation:

1. [Workflows](workflows.md) — first run, development, updates, and operations.
2. [Configuration](configuration.md) — paths, credentials, channel policy,
   updates, state, and recovery.
3. [Crabfile](crabfile.md) — versioned export, validation, and import format.
4. [Providers](providers.md) — intelligence and messaging setup.
5. [Authentication](authentication.md) — credentials and channel trust.
6. [Commands](commands.md) — CLI, daemon, session, service, and plugin flows.
7. [Concepts](concepts.md) — sessions, turns, leases, events, and delivery.
8. [Architecture](architecture.md) — process boundaries, turn flow, and JSON-RPC.
9. [Plugins](plugins.md) — manifests, sources, permissions, and authoring.
10. [Safety](safety.md) — defaults, confinement, recovery, and resource limits.
11. [Security](security.md) — trust boundaries, approvals, and operational limits.
12. [Troubleshooting](troubleshooting.md) — diagnostics and common failures.
13. [Release](release.md) — packaging, checksums, and publication gates.
14. [License](license.md) — Apache-2.0 rationale and contribution terms.
15. [Product](product.md) — capabilities, boundaries, and engineering principles.
16. [Acceptance](acceptance.md) — deterministic product and release flows.

## Request Flow

```mermaid
flowchart LR
    channel[Channel Plugin] -->|Normalized Event| daemon[Daemon]
    daemon -->|Policy And Session State| core[Core Turn Loop]
    core --> model[Model Plugin]
    core --> tools[Tool Plugin]
    daemon -->|Durable Reply And Acknowledgement| channel
```

The daemon owns authorization, leases, delivery state, and operator-controlled
recovery. Plugins receive normalized protocol values and never call one another
directly. Keep plugin stdout reserved for JSON-RPC frames; write diagnostics to
stderr.

## Local Development

```sh
lefthook install
make verify
```

Tests use deterministic local fixtures. They do not call paid provider APIs.
Before opening a pull request, update the relevant guide and run the complete
verification workflow, including the 80% core and 50% host package line-coverage
gates.
