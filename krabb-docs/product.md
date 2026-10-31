# Product

Krabb is a self-hostable, open-source agent operating system for building,
running, and orchestrating native and external AI agents. This document
describes the product’s purpose, capabilities, boundaries, and engineering
principles. [PLAN.md](../PLAN.md) contains only active work, while
[ROADMAP.md](../ROADMAP.md) contains future expansions.

## Table Of Contents

- [Summary](#summary)
- [Architecture And Interfaces](#architecture-and-interfaces)
- [Product Behavior](#product-behavior)
- [Engineering, Governance, And Delivery](#engineering-governance-and-delivery)
- [Verification And Acceptance](#verification-and-acceptance)

## Summary

Build Krabb as a self-hosted Rust agent runtime with a deliberately
capability-free core. Model providers, messaging channels, persistence, tools,
memory, scheduling, speech recognition, MCP, and the TUI are independently
installed process plugins.

The supported experience includes Telegram, Discord, Codex, Anthropic and
OpenAI APIs, Ollama Local/Cloud, ACP, A2A, AG-UI, a full terminal UI, local
coding tools, persistent memory, scheduled tasks, MCP integrations, and local
Whisper transcription. Experimental browser automation and computer control are
available as optional plugins with separate system dependencies. Computer
control uses isolated Xvfb on Linux and explicitly enabled interactive desktop
access on macOS and Windows.

Follow Krabblab's workspace architecture, CI/CD conventions, documentation
organization, release automation, and Agentskill orchestration. Use Apache-2.0,
with a root license, notice, SPDX metadata, and no CLA/DCO requirement.

Brand positioning:

- Primary claim: A self-hostable agent OS for native and external AI agents.
- Primary description: Krabb is a self-hostable, open-source agent operating
  system for building, running, and orchestrating native and external AI agents.
- GitHub description: A self-hostable, open-source agent operating system for
  building, running, and orchestrating native and external AI agents.

## Architecture And Interfaces

- Rust edition 2024, MSRV 1.89, committed lockfile, strict formatting,
  Clippy/rustdoc, typed errors, bounded queues, cancellation, and unsafe
  forbidden in first-party application code.
- krabb-core owns normalized types, the agent loop, context, policy, routing,
  protocol/SDK contracts, and provider-neutral supervision primitives.
- krabb-runtime owns daemon lifecycle, bootstrap, configuration, service
  management, installers, and updater; krabb and krabb-daemon are thin
  entrypoints over that runtime.
- krabb-plugins contains one independently buildable Rust project per
  official plugin.
- The core contains no channel, model, database, memory, scheduler, tool,
  MCP, speech, or TUI implementation. Core-only startup still supports
  configuration, diagnostics, plugin management, and version reporting.
- Official plugins include intelligence providers, messaging channels, tools,
  memory and storage, browser and isolated desktop controls, and optional agent
  interoperability through ACP, A2A, and AG-UI.

### Core, Runtime, And Plugin Responsibilities

- **Core** owns provider-neutral agent behavior and contracts: normalized
  messages and tools, the turn loop, context, policy, and plugin protocol types.
  It does not know SQLite, plugin IDs, or database schemas.
- **Runtime** hosts the core behavior. It owns daemon lifecycle, configuration,
  plugin discovery and process management, IPC, and routing between agent
  requests and installed plugins. It can expose a first-party command or tool
  only when the plugin that implements its capability is available.
- **Plugins** implement capabilities and their external I/O. The SQLite plugin
  owns database and record persistence. Runtime exposes the agent's `data`
  tool and routes its requests to SQLite; core sees only the generic tool and
  plugin contracts.

This division means a capability can be a first-party Krabb feature while its
implementation remains an optional plugin. Its command can therefore be
conditional on that plugin being installed.

- Every plugin uses versioned JSON-RPC 2.0 over strict LF-delimited stdio. stdout is
  protocol-only and stderr is for redacted logs. Require handshake, protocol
  range negotiation, capability advertisement, health, shutdown, correlated
  requests, streams, cancellation, deadlines, bounded frames and queues,
  restart backoff with capped delays and retries until shutdown or recovery.
- Plugins may call host services but never call one another directly.
- Normalize content, model streams, channel events, tool schemas, approvals,
  results, and artifacts before crossing a plugin boundary.
- Model plugins advertise request capabilities, including image-input support,
  during handshake; the host performs that preflight before dispatching media.
- Plugin bundles contain a manifest, executable, configuration schema,
  permissions, secrets, capabilities, protocol range, target metadata, and
  checksums. plugins.lock records source, revision, hash, protocol, version,
  and approved permissions.
- Protocol 0.x is stable within minor releases. First-party plugins release in
  lockstep; community plugins use declared protocol ranges and may be written
  in any language.
- Support public/private Git revisions, verified archives, and local links.
  Source builds may use recognized Cargo builds; arbitrary install scripts are
  prohibited.
- The MCP plugin supports stdio and Streamable HTTP, tools, resources,
  prompts, OAuth where applicable, cancellation, and negotiation.

## Product Behavior

- Persistent daemon with authenticated local IPC, opt-in OS keyring
  credentials, protected-file fallback, and systemd-user, launchd-agent, and
  Windows Service support.
- CLI commands include init, doctor, status, version, plugin
  install/link/list/update/uninstall, session, delivery, service, export,
  validate, and import. Conditional native commands such as `session`,
  `delivery`, `schedule`, `memory`, `tui`, and `data` appear when their backing
  capability is available. The `ask` command is always listed and reports an
  actionable error if no model is ready. Other plugin-specific commands include
  `code` and `codex`. Human output is default; JSON and noninteractive
  confirmation are explicit.
- TUI supports chat, sessions, model/workspace switching, approvals, schedules,
  memory, plugin health, and streamed activity. Session transcripts retain
  displayed TUI command exchanges alongside model turns.
  Typed shell commands support Linux and Windows with descendant containment. Commands and
  manual compaction retain exclusive session ownership until execution and cleanup finish.
  Automatic compaction shares the model turn's deadline and retires failed requests before
  generation resumes. Stream failures finalize turns after cleanup; approval history writes
  remain available while uninstall waits for active turns.
- Sessions support new, resume, fork, cancel, and model switching. One turn
  runs per session; later messages queue; stop cancels. Bounds cover steps,
  time, and tokens. Crashes interrupt turns and mutating tools are never
  replayed.
- `KRABB_HOME/workspace/KRAB.md` defines Krabb's identity and communication
  style; `KLAW.md` defines behavioral guidance and workflows. Both are loaded
  into every model turn alongside the bounded session transcript. Krabb does
  not load `AGENTS.md` itself; delegated coding agents may use it.
- Telegram uses long polling by default; Discord uses Gateway. Support DMs,
  allowlisted Telegram topics, Discord channels/threads, mention-only groups,
  owner/admin/member roles, and signed expiring approvals.
- Stream responses with throttled edits, tool status, inline approvals, and
  channel-safe final chunks.
- Accept text, images, safe text/code files, and voice. Gate media by model
  support and limits. Delete raw voice after transcription and expire other
  cached attachments after 24 hours unless pinned.
- Browser observations can reach vision-capable models as temporary image
  inputs. The Playwright plugin requires separately installed Node.js,
  Playwright, and Chromium; network interception does not replace OS egress
  controls.
- The computer plugin starts an authenticated Xvfb session on Linux. On macOS
  and Windows it can control the signed-in desktop only when the operator sets
  interactive mode and explicitly enables host control. Native desktop access
  is not isolated; process, filesystem, and network isolation remain the
  operator's responsibility.
- Whisper downloads local models separately and is CPU-first.
- SQLite gives the agent durable structured databases and typed tables for
  user data, with scoped record CRUD and bounded queries. Its separate store
  methods also provide key/value values, leases, and deduplication; daemon
  message delivery remains in runtime state.
- The optional memory plugin provides scoped, auditable, searchable, editable,
  and deletable records. Guided learning is the default and saves only on an
  explicit user request; autonomous learning retains stable, useful facts
  without secrets or sensitive inferences. Its private `memory/` home is
  created only after the plugin is installed and first used.
- Scheduling supports approved one-time and recurring agent tasks, including
  cron expressions with IANA timezones.
  Missed tasks default to skip-and-notify, with configurable once recovery.
- Tools provide root-confined files, search, patches, Git status/diff/
  worktrees, and optional shell. Shell is disabled by default and each
  command requires approval. Docker/Podman isolation is optional on Linux and Windows
  and required on other Unix hosts for shell containment. Containers are local-only,
  networkless, read-only-root, and limited to a writable active workspace.
  Remote
  rooms default to isolated worktrees; owner DM/TUI defaults to in-place.
- Codex supports Platform API keys and personal Codex OAuth through its
  installed runtime, owner-only by default. Anthropic uses API keys with no
  Claude OAuth or
  Claude Code delegation. Ollama supports local unauthenticated use and Cloud
  with KRABB_OLLAMA_API_KEY.
- Personal Codex sign-in uses the installed Codex CLI app-server, with runtime
  protocol validation; shared deployments can use provider API keys without
  Codex account coupling.
- Collect no telemetry and redact credentials, tokens, authorization headers,
  and sensitive paths from logs.

## Engineering, Governance, And Delivery

- PLAN.md contains only active work; completed details move out of the active
  plan and future work belongs in ROADMAP.md.
- Project documentation covers contribution, conduct, security, support,
  governance, releases, plugins, dependencies, and licensing.
- Agentskill maintains root and scoped guidance with provenance, signatures,
  evidence validation, and semantic checks.
- CI covers verification, builds, tests, security, Agentskill, release
  preparation, packaging, and checksums.
- Stable and MSRV builds, all-target compilation, doctests, Clippy, formatting,
  dependency audits, documentation validation, and at least 80% core and 50% host
  package line coverage are release requirements.
- Releases provide separate core and plugin archives for Linux, macOS, and
  Windows on x86_64 and ARM64, with checksums and provenance.
- Unix and PowerShell installers support core and one-line plugin installation.
- Crates and containers are not part of the current distribution model.
- Update modes are off, check, prompt, and auto, with prompt default.
  Stage, verify, back up, migrate, health-check, activate atomically, and
  roll back failures. Never silently activate incompatibilities.

## Verification And Acceptance

- Test framing, schema compatibility, compaction, policy, path confinement,
  cron/DST behavior, migrations, redaction, malformed plugins, hangs, crashes,
  backpressure, cancellation, and protocol mismatch.
- Provider tests use deterministic fixtures and no live paid accounts.
- Channel tests cover deduplication, groups, threads, attachments, streams,
  roles, callback expiry, and approval spoofing.
- Storage/update tests cover crash recovery, outbox idempotency, checksum
  failure, rollback, permission expansion, and interrupted turns.
- Cross-platform tests cover IPC, services, TUI snapshots, installers,
  archives, and plugin discovery.
- Acceptance flows cover a Telegram DM with text/image/voice/tools/memory/
  scheduler, a Discord thread with authorization and a worktree, provider
  switching without history loss, core-only diagnostics, and update rollback.
  The deterministic evidence matrix is maintained in
  [Compatibility And Deployment Checks](../COMPATIBILITY.md).

Browser automation, video, text-to-speech, Telegram webhooks,
email/calendar/smart-home integrations, hosted multi-tenancy, Claude Code
delegation, and Claude subscription OAuth are outside the current product
boundary. They require a proven use case and a design that preserves Krabb’s
small core before entering the roadmap.
