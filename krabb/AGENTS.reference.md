# AGENTS.reference.md

## Provenance And Decisions

- Agentskill Version: `2.1.0`.
- Evidence Schema Version: `4`.
- Repository Revision: `546ccdf8aa89ef4964b702439cd0dd8893477789`.
- Configuration: inherited from the repository root; default signature enabled.
- Maintainer-Confirmed Decisions: adopt krabb as a nested scope; the host owns lifecycle and policy while optional behavior remains in process plugins.
- Unresolved Uncertainty: native service implementation remains platform-specific.

## Boundary

`src/main.rs` is the thin CLI entrypoint. It parses native commands and invokes
the shared runtime. Host lifecycle, authenticated local control, durable
sessions, platform services, and remote updates belong to `krabb-runtime`;
provider requests do not belong in this scope.

---

> Generated and maintained by [Agentskill](https://github.com/airscripts/agentskill).
> Do not touch this file. It is automatically managed by Agentskill.
