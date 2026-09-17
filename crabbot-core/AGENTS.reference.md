# AGENTS.reference.md

## Provenance And Decisions

- Agentskill Version: `2.1.0`.
- Evidence Schema Version: `4`.
- Repository Revision: `03c79ff484255bb0b14ac00111d65f9239e88300`.
- Configuration: inherited from the repository root; default signature enabled.
- Maintainer-Confirmed Decisions: adopt crabbot-core as a nested scope; keep this crate free of capability implementations and wire types.
- Unresolved Uncertainty: none specific to this crate.

## Boundary

The public types in `src/types.rs` cross every plugin boundary. `src/jsonl.rs`
is the bounded JSON-RPC transport framing. `src/plugin.rs` owns protocol
contracts; `src/policy.rs` owns path and shell gates; `src/agent.rs` owns the
turn seam. Host process lifecycle is implemented by `crabbot-runtime`.

---

> Generated and maintained by [Agentskill](https://github.com/airscripts/agentskill).
> Do not touch this file. It is automatically managed by Agentskill.
