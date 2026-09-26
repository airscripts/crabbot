# Roadmap

## v0.2 | Reliability

Strengthen Crabbot’s engineering foundation while keeping the core small and
the public behavior stable.

- Resolve all known high and medium reliability findings.
- Improve recovery, cancellation, queue handling, persistence, and plugin updates.
- Design and implement native Atomic Updates: validate a staged release before
  promotion, preserve the previous version, and recover or roll back safely
  across interruption and failed health checks.
- Preserve stable JSON-RPC 0.x compatibility.
- Keep core coverage above 80% and runtime host coverage above 50%.
- Require clean dependency vulnerability audits.
- Keep formatting, Clippy, tests, rustdoc, security, and Agentskill checks passing.

## v0.3 | Experience

Make installation, first-run setup, daily CLI use, and project understanding
clearer for adopters and contributors.

- Improve `init`, `doctor`, `status`, configuration, and plugin installation.
- Improve errors, recovery guidance, and shell completion.
- Clarify architecture, plugin development, configuration, security, and troubleshooting documentation.
- Add deterministic onboarding and command-output tests.
- Preserve protocol and plugin compatibility.

## v0.4 | Ecosystem

Make it easy to create, review, discover, and securely install community
plugins without adding a hosted runtime to Crabbot.

- Add the `crabbot-plugins/template` Hello Crabworld Rust starter.
- Document the plugin authoring and catalog submission workflow.
- Create a separate Astro site deployed on Vercel.
- Add a curated public plugin catalog with reviewed pull requests.
- Include pinned sources, project details, capabilities, permissions, secrets, targets, maintainers, and security status.
- Generate copyable install commands for immutable GitHub revisions.
- Keep private plugins directly installable but outside the public catalog.

## v0.5 | Deployments

Make Crabbot straightforward to operate in containerized environments without
coupling users to one container engine or bundling optional plugins into the
core image.

- Publish reproducible OCI-compatible deployment images for the CLI and daemon.
- Keep Docker, Podman, and other OCI runtimes supported through the same image
  and documented entrypoints.
- Implement and document container configuration through environment variables
  and mounted secret files, including precedence and validation.
- Support persistent volumes for `CRABBOT_HOME` and `CRABBOT_ROOT`, health
  checks, graceful shutdown, and restart behavior.
- Keep plugins independently installable and hot-loadable, with explicit IPC,
  network, and volume boundaries in the container deployment guide.
- Avoid privileged defaults and add CI smoke coverage for an OCI runtime,
  image metadata, persistence, and daemon lifecycle.

## v0.6 | Multi-Agent Runtime

Let operators run multiple independent agents and later coordinate them across
process and machine boundaries, without weakening workspace or capability
isolation.

- Keep the existing single-agent home layout and behavior as the default.
- Add an explicit `crabbot init --multiagent` mode that puts every agent,
  including the first, under `agents/<id>` with its own configuration, state,
  workspace, and plugin installations; do not create a special root agent in
  this mode. Plugin configuration, enablement, and updates are scoped to their
  owning agent.
- Run one daemon per multi-agent home by default, supervising a separate
  runtime context for each agent. Partition sessions, memory, workspaces,
  plugin registries and processes, and configuration by agent; share only the
  host-level supervisor and explicitly global infrastructure.
- Offer a dedicated daemon or container per agent when stronger fault or
  security isolation is needed, while keeping the shared-daemon setup the
  straightforward local default.
- Provide an explicit, recoverable migration for existing installations.
  Move agent-owned data, including installed plugins and their configuration,
  into the default agent; keep only daemon-level runtime infrastructure
  shared. Verify the new layout before switching over and retain rollback data
  until migration succeeds.
- Allow a local agent workspace to run on the host or in an isolated container,
  with explicit filesystem, tool, secret, network, and resource boundaries.
- Define authenticated, permissioned agent-to-agent messaging and task handoff;
  do not implicitly share conversation history, memory, credentials, or files.
- Add supervision and lifecycle controls for independent agents, including
  health, restart, shutdown, and bounded resource use. Bound active agents,
  concurrent work, plugin processes, and storage per agent and across the host;
  avoid an arbitrary hard cap on configured agents, with an optional
  administrator-configured count limit where useful.
- Extend the topology to multiple containers and, later, multiple machines
  through a documented coordination protocol that tolerates disconnection and
  partial failure.
- Keep single-agent local operation simple and useful without requiring an
  orchestrator, container engine, or network service.

## v0.7 | Small-Footprint Runtime

Reduce Crabbot's idle and peak resource use for constrained hosts while
preserving the full feature set and clear behavior under load.

- Establish and document minimum supported resource targets instead of
  promising operation on literally any hardware.
- Measure daemon-only and configured-plugin memory and CPU at idle, during
  active turns, and under recovery or burst load; distinguish resident and
  proportional memory, peak use, and plugin overhead.
- Profile before optimizing. Investigate runtime worker/thread sizing, idle
  work, allocations, buffers, and duplicate process overhead, and retain only
  changes that improve representative constrained-device measurements.
- Apply bounded concurrency, queues, and per-agent resource budgets without
  silently dropping work or disabling capabilities; keep limits observable and
  configurable where operators need control.
- Add repeatable performance and low-memory smoke tests for representative
  supported targets, and verify behavior with the full configured plugin set.
