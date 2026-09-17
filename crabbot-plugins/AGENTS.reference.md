# AGENTS.reference.md

## Provenance And Decisions

- Agentskill Version: `2.1.0`.
- Evidence Schema Version: `4`.
- Repository Revision: initial workspace scaffold; no commit exists yet.
- Configuration: inherited from the repository root; default signature enabled.
- Maintainer-Confirmed Decisions: adopt crabbot-plugins as a nested scope; all official capabilities are separate process plugins and release in lockstep with the core.
- Unresolved Uncertainty: none specific to this scope.

## Boundary

Child plugin IDs and manifests define installation and capability discovery.
Each process uses `crabbot_core::plugin::serve` or `serve_with` and negotiates
protocol `0.1`.

---

> Generated and maintained by [Agentskill](https://github.com/airscripts/agentskill).
> Do not touch this file. It is automatically managed by Agentskill.
