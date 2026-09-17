# AGENTS.md

## Mission And Repository Map

Crabbot is a small, self-hosted Rust agent with an experimental process-plugin
protocol.

- `crabbot-core` owns normalized types, the turn loop, policy, and protocol.
- `crabbot-runtime` owns host orchestration, daemon state, IPC, and plugin
  lifecycle; `crabbot` and `crabbot-daemon` are thin entrypoints.
- `crabbot-libs/file` owns shared crash-safe filesystem primitives.
- `crabbot-plugins` owns optional model, channel, store, memory, timer, tool,
  MCP, speech, and client processes.
- `crabbot-docs/`, `crabbot-scripts/`, `.github/`, and release files own user guidance,
  automation, and project support.

## Non-Negotiables

- Keep the core capability-free and provider-neutral.
- Keep plugin stdout as protocol-only JSON-RPC messages framed by JSONL; send
  diagnostics to stderr.
- Normalize external payloads before they cross a plugin boundary.
- Keep filesystem access confined, shell disabled by default, and secrets out
  of logs, manifests, issues, and planning files.
- Use one precise word for names whenever it remains clear.

## Don’ts

- Do not add network clients, databases, UI code, or vendor wire types to the core.
- Do not add telemetry or silently emulate unsupported capabilities.
- Do not replay mutating tools after an interrupted turn.
- Do not use vague `utils`, `helpers`, `common`, `misc`, or `manager` modules.
- Do not commit, tag, push, publish, or change remotes unless explicitly asked.

## Quick Start

```bash
lefthook install
make verify
crabbot init
crabbot doctor
```

## Change Routing

Put shared contracts in `crabbot-core`, host orchestration in
`crabbot-runtime`, thin entrypoint behavior in `crabbot` or `crabbot-daemon`,
filesystem primitives in `crabbot-libs/file`, and provider/channel/capability
behavior in its plugin. Update `crabbot-docs/`, tests, and `CHANGELOG.md` with
public behavior changes.

## Implementation Conventions

Use four spaces, a 100-column limit, typed errors, bounded async work,
cancellation, and no unexplained comments. Prefer explicit phases and small
files. Public protocol changes require compatibility tests.

## Testing And Validation

Run `make verify` before handoff. Automated tests must not call paid provider
APIs. Test protocol framing, malformed plugins, path policy, approvals,
crashes, retries, and update rollback.

## Common Change Playbooks

For a plugin, update its manifest, protocol tests, README install line, and
release matrix. For a CLI change, update command tests and the command guide.
For a public behavior change, update the implementation ledger and changelog.

## Free Region

Maintainer policy: keep changes unstaged unless asked. Use lowercase
conventional commits. Keep the implementation ledger current. Do not run
release or publication commands without explicit approval. When the active plan
is complete, clear its completed details from `PLAN.md` and keep future work in
`ROADMAP.md`.

## Further Context

See [AGENTS.reference.md](AGENTS.reference.md) for provenance and decisions.

---

> Generated and maintained by [Agentskill](https://github.com/airscripts/agentskill).
> Do not touch this file. It is automatically managed by Agentskill.
