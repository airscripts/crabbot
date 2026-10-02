# Changelog

All notable Crabbot changes are documented here using Keep a Changelog and
Semantic Versioning.

## [Unreleased]

### Added

- Refresh shared TUI session transcripts and working status across multiple
  windows; prevent conflicting turns and report a busy session as System.
- Let explicitly linked plugins pass integrity checks after their local binary
  is rebuilt in place, while still requiring the recorded symlink target and
  manifest to match. A daemon restart loads rebuilt plugin executables.
- Add the shared `CRAB.md` and `CLAW.md` system context to TUI model turns, matching
  CLI and messaging-channel turns. Give both starter templates practical defaults
  for personality, communication, uncertainty, context, permissions, sensitive
  actions, and task workflow. Document their shared 32 KiB context budget and
  truncation behavior in the starter files and configuration guide.
- Keep the TUI palette to the terminal's default foreground, orange accents, and gray shades;
  render live generation status and list headings in orange.
- Show TUI slash and `!` command replies as neutral gray System entries, expose
  pending tool approvals as actionable notices, and disable new chat messages
  while a turn is active. Allow `/` approval commands to be typed from an empty
  input during a turn and decode them correctly before resolving approvals.
  Render live approval confirmations as System entries while keeping the
  resumed model reply under Crabbot. Format approval notices with the tool,
  exact command or arguments, action summary, and highlighted `/approve` and
  `/deny` commands. Show per-plugin install progress and daemon guidance only
  when installed plugins need the daemon; omit redundant active-plugin status.
  Suppress stray punctuation-only stream fragments immediately after approval.
  Use aligned left-side role rails for transcript messages and slash/approval
  pickers, add a horizontally navigable approval picker, and show slash-command
  suggestions above a prompt. Give transcript messages a little more separation
  and visually distinguish actor names from their subdued timestamps. Report
  interrupted turns as System and reveal local message timestamps on hover as
  “Actor, YYYY-MM-DD at HH:MM”.
  Use the statusline gray for System messages, and improve command help and
  paginated session/plugin lists with five entries per page and tree-formatted
  metadata, replacing status diamonds with connected tree branches. Add streamed
  `!<command>`
  execution through the daemon's configured shell without requiring the general
  Tools plugin. Keep streamed message rails fixed while preserving typewriter
  animation.
- Resolve TUI workspace paths from the launch directory, show the active workspace by basename,
  display local message times, use a neutral user-message foreground, and align command help rows.
  Add `[clients.tui] theme = false` to render TUI content with the terminal's default foreground.

- Add validated `crab config get/set` commands, live-apply the TUI tools setting,
  and support restarting a running daemon with `--force` for other changes.
- Namespace TUI sessions separately from channel conversations while keeping
  the internal prefix out of displayed session names.
- Require the daemon for interactive and one-shot TUI use. Route TUI model turns
  through the shared daemon executor and approval/tool policy, with TUI tool
  access disabled unless `[clients.tui] tools = true` is explicitly configured.
- Show a concise two-line startup message when `crab tui` is run without the
  required daemon.
- Summarize plugin installations with progress and a success count; show
  runtime guidance only for plugins that are not active yet. Format TUI command errors as
  readable messages instead of Rust debug wrappers.
- Keep user and assistant turns visually separate when fast replies complete,
  and reveal daemon-streamed TUI responses as they arrive.
- Avoid replacing a local command reply with a stale session snapshot while its
  output is streaming or being revealed.
- Let an explicitly selected session workspace replace the default tool root for
  that session, while keeping every tool operation confined to the selected
  directory. Use `CRABBOT_ROOT` when no session workspace is selected.
- Return daemon-hosted read and list results in the `text` field expected by
  intelligence plugins, so workspace tool output is available to the model.
- Advertise shell to intelligence providers only when `shell = true`, reject
  undeclared tool calls before approval, and execute at most one mutating tool
  call from each model response before asking the model to continue. Direct
  Codex not to repeat a completed file operation through another tool.
- Treat an active TUI reservation as a valid mutation lease for approval-aware
  tool calls, and allow up to one minute for Codex app-server control requests.
- Persist TUI message times as UTC Unix-epoch milliseconds and render them in
  the local timezone on each computer.
- Add `crab service restart` to restart the installed daemon through its native
  service manager without changing whether the service is enabled.
- Persist streamed TUI replies in the daemon before acknowledging turn
  completion, so a client switch or exit cannot lose them. Place padded
  speaker/date titles inside rounded message borders.
- Keep shell-disabled `!` command diagnostics in session history and identify
  the config file used by the running daemon. Align command-picker descriptions
  after the longest command.
- Move production session state to `data/sessions/` with a global index,
  opaque-keyed session records, client state, and separate delivery records.
  Import legacy daemon and offline TUI sessions on first daemon startup while
  retaining source files and preferring existing daemon sessions on collisions.

- Allow installing or linking multiple bundled plugins in one command, with
  `plugin install --link` retaining the local development workflow. Reuse the
  Codex app-server between turns and clarify that declared workspace tools
  should be used before reporting them unavailable.
- Add Codex subcommand help and account model listing with the concise
  `crab codex models` command; negotiate the experimental app-server API needed
  for workspace roots and clarify foreground availability. Keep the TUI session
  usable after provider failures, coalesce long streamed replies, and stop
  streaming updates safely before the plugin event budget is exhausted while
  retaining the complete final reply. Stop a turn if Codex repeats an identical
  tool call after it failed, and make workspace confinement explicit in tool
  guidance. Show model-selection guidance only for invalid-model errors.
- Resolve bundled plugin IDs from category directories such as
  `crabbot-plugins/intelligence/codex`, so `crabbot plugin install codex` works
  from the repository root after building the plugin.
- Pass the documented default workspace root to the tools plugin when no
  `CRABBOT_ROOT` override is configured, so plugin installation health checks
  can complete their protocol handshake. The daemon supplies the active session
  workspace to TUI tool calls, and the tools plugin confines operations to it.
- Added toggleable Codex Fast mode for Revloop. It is off by default and can be
  enabled with `--fast` or `CRABBOT_REVLOOP_FAST=true`.
- Added a globally configurable Crabbot display name and improved the TUI with
  distinct user/assistant labels, multiline input, bounded input history,
  capability-aware command help, argument validation, and a persistent configurable statusline.
- Organized TUI-owned state under `CRABBOT_HOME/data/plugins/tui/`, restore
  the selected session's saved conversation on startup and session switches. Persist TUI
  commands and their replies as well as completed model turns, including
  command-only sessions when no intelligence plugin is installed. Added
  `/statusline reset` to restore the built-in format. Invalid session switches
  no longer change the displayed session. The default statusline labels model
  and session explicitly, updates when `/session switch` changes the active
  session, and shows `model: unset` when no intelligence plugin is installed.
- Made `crabbot plugin update` preview the exact available plugin changes;
  rerun it with `--yes` to apply, with unchanged plugins left untouched.
- Reworked `crab tui` as a full-screen terminal chat with editable input,
  scrollable conversation history, and one-shot prompt mode. The TUI uses the
  daemon-managed session store, tools, and approvals; opening it requires the
  background runtime. Abandoned TUI
  session reservations expire and recover automatically.
- Added TUI session help, rename, reversible archive/restore, and confirmed
  permanent deletion; session history is restored on switches, and the active
  session cannot be archived or deleted. Session listing includes state,
  model, timestamps, and message counts; repeated archive actions report the
  existing state, and command action errors avoid redundant operation-failed
  wording when the cause is already clear. Channel-backed sessions cannot be
  renamed, preserving their inbound message routes.
- Styled TUI conversation turns, system notices, and command output for easier
  scanning. Session listings use compact two-line entries. Local command replies
  and streamed model text use a configurable typewriter reveal; model generation
  shows 25 rotating "The Crabbot..." progress messages, and Escape-to-interrupt
  support that preserves partial replies.
- Removed the transcript-wide frame, added left-aligned orange Crabbot bubbles
  and right-aligned green user bubbles with rounded corners, and rounded the
  input border to match. Saved user and assistant messages show UTC timestamps,
  and generation status includes elapsed time. When the `tools` plugin is
  installed, the TUI gives intelligence plugins confined `read`, `list`, and
  `search` tools; mutating
  tools remain unavailable in this direct interactive path. Consecutive Codex
  assistant message items are separated by a blank line, and final model text
  no longer duplicates text already streamed.
- Paginated TUI session listings at ten entries per page, with in-progress and
  selected sessions first, then idle sessions by recency and archived sessions
  last. Each page reads a fresh snapshot for independent concurrent TUI clients.
- Reworked TUI scrolling with predictable three-line conversation wheel steps,
  one-line input wheel steps, page-sized conversation navigation, automatic
  follow-latest behavior, and cursor-aware input positioning.
- Expanded the TUI and CLI plugin lists with version, health, protocol,
  capabilities, commands, and permissions in a plain-text layout.
- Improved TUI session listing labels and active/working state, added multiple
  session targets and `--all` for archive, restore, and confirmed deletion, and
  removed the `/sessions` shortcut in favor of `/session list`. The footer now
  shows the Crabbot version and help hint at the bottom-right, while conversation
  wrapping is cached to keep scrolling responsive. Clarified workspace scope.
- Kept long words intact when wrapping TUI bubbles, hid only soft-wrap-leading
  spaces in the display, and showed an exit notice while the engine shuts down.
- Launch the TUI as a foreground terminal process so keyboard input is read
  from the user's terminal rather than the plugin protocol stream. Clarified
  that installing the TUI does not require a running background runtime. The
  default TUI session ID is `default`. Standardized user-facing terminology
  for the background runtime and OS service.
  The TUI launch now receives only its configured environment and the selected
  model plugin's declared secrets.
- Split CLI help into native, conditional, and plugin-contributed commands.
  The host-managed `ask` and `session`
  commands now require an installed intelligence plugin, and `delivery`
  requires an installed messaging plugin.
- Prevented accidental uninstallation of the last intelligence or messaging plugin
  while dependent state remains; `plugin uninstall -y --force` can explicitly
  purge sessions or outbox and dead-letter deliveries tied to that capability.
- Made workspace test and coverage runs continue after failures and summarize
  all failing packages at the end, so one broken crate does not hide later CI
  failures.
- Added native macOS and Windows x86_64 coverage checks for the CLI, core,
  filesystem, runtime, and daemon packages, with 60% line coverage for those
  packages and 40% for the runtime host; Linux requires 80% for every package.
  Windows ARM64 remains test-only while coverage instrumentation is unsupported.
- Added optional scoped memory learning with guided and autonomous modes, a
  bounded per-conversation memory index, editable Markdown records, and a
  plugin-registered `memory` command. The memory home is created only when the
  installed plugin first persists a learning setting or record.
- Added a global Crabbot workspace with editable `CRAB.md` identity and
  `CLAW.md` behavior instructions, loaded into each model turn alongside the
  bounded conversation; the workspace is the default confined tool root.
- Made `init --force` a confirmed full-home reset requiring `--yes` for
  non-interactive use, and extended `doctor --fix` to seed missing workspace
  instruction files without overwriting user edits.
- Added safe, repeatable `crabbot init` behavior with explicit `--force`
  reinitialization and a friendly first-run completion message.
- Added crash-safe Signal inbound acknowledgement and bounded attachment
  caching with expiration and aggregate-size limits.
- Added conservative `crabbot doctor --fix` repairs for missing default local
  state without overwriting configuration or plugin files.
- Added human and structured health summaries to `crabbot doctor` output.
- Added pretty-printed JSON diagnostics for all `crabbot doctor` modes.
- Added the shorter `crab` executable alias for the `crabbot` CLI.
- Added executable-aware shell completions for both `crabbot` and `crab`.
- Added structured JSON output for native initialization, diagnostics, version,
  Crabfile, service, plugin, session, and delivery operations; capability status
  now includes explicit status and plugin arrays.
- Hardened service installation and removal with explicit `--force` and `--yes`
  confirmations, and made plugin inventory and delivery errors fail clearly
  instead of being silently ignored.
- Made service status report the service-manager state and name, and made
  repeated service starts and stops explain when no state change was needed.
- Standardized the macOS launchd and credential-store identifier as
  `it.airscript.crabbot`, and use the friendly service name `Crabbot` on macOS
  and Linux.
- Made bare `crabbot service` display native Clap help, including inherited
  global options and the `-H` help alias, instead of implicitly running status.
- Added adaptive `revloop` review scope: uncommitted changes by default, the
  latest commit or feature branch when the tree is clean, and an explicit
  `--global` mode for repository-wide audits.
- Hardened `revloop` convergence with explicit verification short-circuiting, a
  three-pass clean stability check for every scope, a 50-cycle default limit,
  60-minute Codex invocation limits, and non-blocking environment warnings,
  automatic repository spacing before verification, and unambiguous release
  smoke-test binary selection; verification phases now have configurable
  timeouts with one retry and a separately logged worker recovery pass after
  two timeouts; repeated focused failures now preserve diagnostics and continue
  autonomously instead of stopping for manual repair.
- Added clear import errors for missing confirmation, missing Crabfiles, unreadable
  files, and invalid Crabfile syntax, including parse locations.
- Added the version `0.1` Crabfile specification and read-only
  `crabbot validate` command sharing validation rules with import.
- `crabbot export` now defaults to `./Crabfile` and accepts a positional or
  `--path` directory destination, while requiring `--force` to overwrite an
  existing output.
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
- Added the supported `gemini-3.8-flash` Gemini default model and ignored the
  host's generic `gpt-6-luna` fallback for Gemini workflows.
- Added Gemini 3 thought-signature round-tripping for tool calls while
  retaining compatibility with explicitly selected Gemini 2.x models.
- Added structured tool calls and Gemini 3 thought-signature support within
  the unreleased 0.1 plugin protocol.
- Added `gpt-6-luna` as the default revloop model, with
  `CRABBOT_REVLOOP_MODEL` available for overrides.
- Added `gpt-6-luna` as the default model for new sessions and one-shot requests.
