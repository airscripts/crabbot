# AGENTS.reference.md

## Provenance And Decisions

- Agentskill Version: `2.1.0`.
- Evidence Schema Version: `4`.
- Repository Revision: `546ccdf8aa89ef4964b702439cd0dd8893477789`.
- Configuration: inherited from the repository root; default signature enabled.
- Maintainer-Confirmed Decisions: adopt krabb-plugins as a nested scope; all official capabilities are separate process plugins and release in lockstep with the core.
- Unresolved Uncertainty: none specific to this scope.

## Boundary

Child plugin IDs and manifests define installation and capability discovery.
The `intelligence/` and `messaging/` paths are organizational groupings,
not capability boundaries. Each process uses `krabb_core::plugin::serve` or
`serve_with`, emits only JSON-RPC protocol frames on stdout, and negotiates
protocol `0.1`.

---

> Generated and maintained by [Agentskill](https://github.com/airscripts/agentskill).
> Do not touch this file. It is automatically managed by Agentskill.
