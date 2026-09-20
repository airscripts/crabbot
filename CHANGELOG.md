# Changelog

All notable Crabbot changes are documented here using Keep a Changelog and
Semantic Versioning.

## [Unreleased]

### Added

- Added safe, repeatable `crabbot init` behavior with explicit `--force`
  reinitialization and a friendly first-run completion message.
- Added conservative `crabbot doctor --fix` repairs for missing default local
  state without overwriting configuration or plugin files.
- Added human and structured health summaries to `crabbot doctor` output.
- Added pretty-printed JSON diagnostics for all `crabbot doctor` modes.
- Added the shorter `crab` executable alias for the `crabbot` CLI.
- Added executable-aware shell completions for both `crabbot` and `crab`.
- Added structured JSON output for native initialization, diagnostics, version,
  Crabfile, service, plugin, session, and delivery operations; capability status
  now includes explicit status and plugin arrays.
- Added explicit `--yes` confirmation for session deletion and readable human
  output for session transcripts, while retaining structured `--json` output.
- Added the host-managed `crabbot ask` command through the external command
  path, exposing it only when an installed model plugin can run.
- Added runtime-enriched CLI help with separate native and plugin command lists;
  unknown external commands now display that help.
- Added descriptive CLI command help, global JSON and diagnostic modes, and
  `-h`/`-H`/`--help`, `--verbose`, and `-v`/`-V`/`--version` options.
- Added structured `tracing` diagnostics for CLI and daemon failures.
- Added warning-level operational diagnostics and best-effort redacted debug
  reports for `--debug` failures.
- Added current product and onboarding documentation covering cross-platform
  installation, first-run setup, provider and messaging prerequisites, custom
  plugin authoring, and deterministic acceptance flows.
- Added contributor guidance for local development, repository conventions,
  verification, documentation updates, and pull requests.
- Added an optional Docker/Podman sandbox for approved shell commands, with
  bounded resources, no network access, and an active-workspace-only mount.
- Added authenticated plugin hot-loading, unloading, and in-place updates, with
  independent plugin archives and a plugin-free core release artifact.
- Added authenticated, persistent per-session workspace selection in the TUI.
- Added authenticated TUI controls for listing and resolving pending tool
  approvals.
- Added confined, bounded Telegram image inputs for OpenAI-compatible,
  Anthropic, Ollama, and Codex app-server model adapters.
- Added timer and session-scoped memory controls to the TUI through bounded,
  authenticated daemon capability calls.
- Added native binary startup and release archive content checks to CI.
- Added exhaustive revloop finding reports while retaining bounded blocker-first
  worker cycles.
- Added canonical `crabbot-plugin-pi` naming to Pi builds and release archives
  so packaged agent plugins are discoverable by the runtime.
- Added checked local archive installer smoke tests through
  `CRABBOT_RELEASE_BASE` while keeping release installs on HTTPS.
- Added an independently installable runtime library with thin CLI and daemon
  entrypoints, plus a bounded orchestrator/worker review loop.
- Added native service definitions that preserve configured paths and
  credentials through protected service environment settings, with service
  removal that stops the service before deleting its definition.
- Added safe session deletion that preserves dirty Git worktrees, serializes
  cleanup with turn setup, persists Discord Gateway cursors until host
  acknowledgement, and terminates plugin descendants after leader exit.
- Added cross-platform advisory locking for daemon and offline session state,
  including read-only session fallbacks before loading persisted state.
- Added the capability-free Rust kernel, versioned JSONL plugin protocol,
  bounded turn loop, policy checks, and host CLI.
- Added persistent TUI sessions with resume, creation, per-session model
  selection, and transcript controls through authenticated daemon IPC.
- Added bounded Telegram voice transcription through the optional Whisper
  plugin, with media-cache confinement and raw-file deletion after success.
- Added safe Telegram text/code attachment expansion with per-file size limits
  and path redaction for unsupported files.
- Added first-party plugin projects and manifests for providers, channels,
  storage, memory, timers, tools, MCP, speech, and the TUI.
- Added Gemini and OpenRouter intelligence plugins alongside Codex, Claude, and
  Ollama.
- Added WhatsApp Cloud, Signal, and Slack messaging plugins with normalized
  text and rich attachment support.
- Added model adapters for OpenAI-compatible chat completions, Anthropic
  Messages, and Ollama Local or Cloud APIs.
- Added Codex app-server support for Codex-managed ChatGPT sign-in, including
  browser and device-code flows, without exposing OAuth tokens to Crabbot.
- Added bounded Telegram and Discord edit operations for existing messages.
- Added coalesced provider-to-channel text streaming, tool progress edits, and
  durable completion of the streamed message through the channel outbox.
- Added expiring signed inline approvals for mutating tools in Telegram and
  Discord, with channel-bound authorization and explicit `off`, `prompt`, and
  `auto` approval modes.
- Added Telegram long polling, Discord Gateway receive and REST replies,
  aggregate streaming responses, bounded MCP stdio/HTTP execution, and
  staged local plugin updates with atomic byte activation.
- Added Apache-2.0 licensing, support files, Lefthook hooks, installers,
  release scripts, and durable implementation planning.
- Added bounded session history, Codex credential discovery, plugin permission
  metadata, private state files, allowlisted Git inspection, and six-target
  release packaging with checksums.
- Added channel authentication, provider response validation, and workspace
  search confinement.
- Added plugin startup cleanup, session-save rollback, Telegram error
  redaction, and release checksum filenames.
- Added explicit approval for MCP process execution and preserved plugin
  executable permissions during installs and updates.
- Added staged plugin installation and linking with rollback when replacement
  or lock persistence fails.
- Added scoped memory modes, durable timer due claims, channel content
  normalization, workspace context loading, and configurable update/channel
  policy.
- Added MCP loopback URL checks against hostname-prefix SSRF bypasses.
- Added protected JSON credential-file fallback for provider and channel
  plugins.
- Added accurate release checksum counts and self-registering plugin archives
  for the Unix and PowerShell installers.
- Added bounded chunked channel replies, streamed body limits for channel and
  MCP transports, private state/database loading, persisted delivery IDs with
  retry accounting, and stricter timer and Discord validation.
- Added Git patch path checks and Codex credential-file permissions.
- Added SQLite outbox retry accounting and bounded retention.
- Added crash-safe private state replacement across Unix and Windows, with
  interrupted-session restoration and owner-only Windows ACLs.
- Added explicit group-channel allowlisting while keeping direct messages
  available by default.
- Added opt-in operating-system keyring credentials and calendar cron scheduling
  with IANA timezone support.
- Added bounded queued turns with policy decisions preserved while a session is
  working.
- Added host approvals, plugin launch integrity, IPC framing and admission,
  state rollback, outbox dead-lettering, credential isolation, archive limits,
  and filesystem confinement.
- Added persistent Discord Gateway sessions with sequence tracking and resume,
  bounded timer and memory persistence, scoped routing policy, and explicit
  cancellation acknowledgement. Added actionable Codex CLI prerequisite
  guidance for personal ChatGPT sign-in.
- Added focused regression tests that keep the complete Rust workspace above the
  80% line-coverage gate.
- Added channel tool exposure controls, session-capacity recovery, restart
  circuit handling, nested file creation, bounded search, and post-acceptance
  event deduplication. Added session deletion, Discord thread classification,
  core approval isolation, Whisper command environment forwarding, queue
  backpressure, and offline state locking.
- Added monotonic channel offsets, age-based deduplication retention, active
  session deletion protection, replay-safe in-flight turn recovery, terminal
  cancellation, worktree reclamation on deletion with visible pending cleanup,
  and bounded search workers with cooperative cancellation and a hard response
  deadline.
- Added locked capability checks and pre-activation health checks for model
  launches and plugin updates; preserved unsupported Telegram update IDs,
  bounded forks and archive metadata, and rejected ambiguous memory keys.
- Added crash-recovery handling that discards canceled in-flight turns.
- Added at-most-once channel delivery tracking with uncertain-send quarantine
  and authenticated `delivery list`, `retry`, and `drop` commands.
- Added bounded serialized model history and workspace instructions, a bounded
  memory forget audit, and blank-session-model validation.
- Added installer registration of release plugins in `plugins.lock` before the
  daemon can launch them.
- Added clear protocol-size errors for oversized current prompts.
- Added explicit Git revision pin preservation across plugin updates,
  canonical local plugin sources, and default workspace build binaries.
- Added symlinked workspace instruction rejection, active-workspace worktree
  confinement, and committed failed group-isolation events.
- Added bounded IPC session listings and tool response frames, queued role
  authorization retention, canceled-queue draining, and Telegram caption
  normalization for mention policies.
- Added bounded remote plugin download timeouts.
- Added dangling symbolic-link write-target rejection and failed tool transport
  restarts before turn retries.
- Added release archive layout, plugin activation health checks, credential
  routing, process-tree cancellation, IPC detail responses, workspace
  selection, and state-file loading.
- Added bounds for channel metadata, manifests, lock files, provider bodies,
  memory and timer responses, SQLite values, and startup Git/archive operations.
- Added SQLite size limits based on each database's actual page size.
- Added scoped plugin environment credentials; Codex file credentials are
  forwarded only for explicitly declared secrets.
- Added plugin byte restoration when lock persistence fails during updates or
  removal, and channel restarts after reply-delivery transport failures.
- Added Rustls 0.23.45 as the release dependency baseline.
