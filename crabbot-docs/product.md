# Product

Crabbot is a local-first, open-source general-purpose agent. This document
describes the product’s purpose, capabilities, boundaries, and engineering
principles. [PLAN.md](../PLAN.md) contains only active work, while
[ROADMAP.md](../ROADMAP.md) contains future expansions.

## Summary

Build Crabbot as a self-hosted Rust agent runtime with a deliberately
capability-free core. Model providers, messaging channels, persistence, tools,
memory, scheduling, speech recognition, MCP, and the TUI are independently
installed process plugins.

The supported experience includes Telegram, Discord, Codex, Claude,
Ollama Local/Cloud, a full terminal UI, local coding tools,
persistent memory, scheduled tasks, MCP integrations, and local Whisper
transcription.

Follow Gitfleet's workspace architecture, CI/CD conventions, documentation
organization, release automation, and Agentskill orchestration. Use Apache-2.0,
with a root license, notice, SPDX metadata, and no CLA/DCO requirement.

Brand positioning:

- Primary claim: Your last next agent.
- Primary description: Crabbot is a lightweight, open-source general-purpose
  agent that runs on your machine and meets you wherever you work—starting
  with Telegram, Discord, and the terminal.
- GitHub description: A lightweight, open-source general-purpose agent for
  Telegram, Discord, and the terminal—powered by the models and tools you
  choose.

## Architecture And Interfaces

- Rust edition 2024, MSRV 1.89, committed lockfile, strict formatting,
  Clippy/rustdoc, typed errors, bounded queues, cancellation, and unsafe
  forbidden in first-party application code.
- crabbot-core owns normalized types, the agent loop, context, policy, routing,
  protocol/SDK contracts, and provider-neutral supervision primitives.
- crabbot-runtime owns daemon lifecycle, bootstrap, configuration, service
  management, installers, and updater; crabbot and crabbot-daemon are thin
  entrypoints over that runtime.
- crabbot-plugins contains one independently buildable Rust project per
  official plugin.
- The core contains no channel, model, database, memory, scheduler, tool,
  MCP, speech, or TUI implementation. Core-only startup still supports
  configuration, diagnostics, plugin management, and version reporting.
- Official plugins are codex, claude, gemini, ollama, openrouter, telegram,
  discord, whatsapp, signal, slack, sqlite, memory, timer, tools, mcp, whisper,
  tui, and pi.
- Every plugin uses versioned JSON-RPC 2.0 over strict LF-delimited stdio. stdout is
  protocol-only and stderr is for redacted logs. Require handshake, protocol
  range negotiation, capability advertisement, health, shutdown, correlated
  requests, streams, cancellation, deadlines, bounded frames and queues,
  restart backoff, and circuit breaking.
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
  install/link/list/update/remove, session, delivery, service, ask, export,
  import, and plugin-registered commands such as `code`, `codex`, and `tui`.
  Human output is default; JSON and noninteractive confirmation are explicit.
- TUI supports chat, sessions, model/workspace switching, approvals, timers,
  memory, plugin health, and streamed activity.
- Sessions support new, resume, fork, cancel, and model switching. One turn
  runs per session; later messages queue; stop cancels. Bounds cover steps,
  time, and tokens. Crashes interrupt turns and mutating tools are never
  replayed.
- Context includes persona, workspace AGENTS.md, selected skills and prompts,
  scoped memory, transcript, and summaries. Raw text history remains until
  deletion.
- Telegram uses long polling by default; Discord uses Gateway. Support DMs,
  allowlisted Telegram topics, Discord channels/threads, mention-only groups,
  owner/admin/member roles, and signed expiring approvals.
- Stream responses with throttled edits, tool status, inline approvals, and
  channel-safe final chunks.
- Accept text, images, safe text/code files, and voice. Gate media by model
  support and limits. Delete raw voice after transcription and expire other
  cached attachments after 24 hours unless pinned.
- Whisper downloads local models separately and is CPU-first.
- SQLite supplies WAL persistence, migrations, leases, deduplication, and
  durable outbox support.
- Memory modes are off, suggest, and automatic; suggest is the default and
  requires approval. Memories are scoped, auditable, editable, and deletable.
- Scheduling supports one-shot reminders and cron prompts with IANA zones.
  Missed tasks default to skip-and-notify, with configurable once recovery.
- Tools provide root-confined files, search, patches, Git status/diff/
  worktrees, and optional shell. Shell is disabled by default and each
  command requires approval. Docker/Podman isolation is optional, local-only,
  networkless, read-only-root, and limited to a writable active workspace.
  Remote
  rooms default to isolated worktrees; owner DM/TUI defaults to in-place.
- Codex supports Platform API keys and personal Codex OAuth through its
  installed runtime, owner-only by default. Claude uses API keys with no
  Claude OAuth or
  Claude Code delegation. Ollama supports local unauthenticated use and Cloud
  with CRABBOT_OLLAMA_API_KEY.
- The Pi agent plugin uses an external Pi runtime as a coding harness while
  Crabbot remains the session, intelligence, tool, workspace, and approval
  owner.
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
  dependency audits, documentation validation, and at least 80% core and host
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
  timer, a Discord thread with authorization and a worktree, provider
  switching without history loss, core-only diagnostics, and update rollback.
  The deterministic evidence matrix is maintained in
  [Acceptance](acceptance.md).

Browser automation, video, text-to-speech, Telegram webhooks,
email/calendar/smart-home integrations, hosted multi-tenancy, Claude Code
delegation, and Claude subscription OAuth are outside the current product
boundary. They require a proven use case and a design that preserves Crabbot’s
small core before entering the roadmap.
