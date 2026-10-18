# Changelog

All notable Crabbot changes are documented here using Keep a Changelog and
Semantic Versioning.

## [Unreleased]

### Fixed

- Show scheduled TUI result timestamps from the recorded outcome time and ignore opaque schedule IDs when formatting message dates.

### Changed

- Run schedules by unique Docker-style names, show concise outcomes with history commands, deliver explicitly requested messages to local TUI sessions, and activate delegated tasks without another approval prompt.
- Say clearly when there are no schedules to list.
- Accept human-readable schedule intervals and local date/time input; assign funny random names to unnamed schedules.
- Require CLI-created schedules to capture an explicit provider and model.
- Format CLI and TUI schedule and memory help consistently, listing accepted options and
  aligning command descriptions.
- Show TUI usage for both `crab tui help` and `crab tui --help`.
- List ask as an always-available native command with required provider and
  model options; list scheduler, memory, and TUI conditionally.

### Added

- Organize per-command captures under `logs/debug/` and name schedule outcome logs `<schedule-name>-<id>.jsonl`.
- Keep complete append-only schedule outcome histories with structured run and agent context.
- Capture the CLI's current directory for scheduled workspace tasks and allow enabled shell tasks to discover requested files, folders, and binaries across the daemon machine.
- Retry one-time schedules skipped while the daemon was offline.
- Add explicit authorized destinations for CLI-created scheduled messages.
- Import persisted timer tasks into the scheduler when upgrading.
- Keep removed imported timer tasks from returning after a scheduler restart.
- Restrict the agent's global schedule listing to local CLI and TUI turns.
- Run a self-hosted agent with independently installed plugins.
- Add a Claude plugin for Anthropic intelligence.
- Add a Codex plugin for OpenAI models, account sign-in, and model listing.
- Add a Gemini plugin for Google model access.
- Add Gemini 3 tool calls and a default model.
- Add an Ollama plugin for local and cloud models.
- Add an OpenRouter plugin for routed model access.
- Add a Pi plugin for coding-agent workflows.
- Add a Telegram plugin for messaging.
- Add a Discord plugin for messaging.
- Add a WhatsApp Cloud plugin for messaging.
- Add a Signal plugin for messaging.
- Add a Slack plugin for messaging.
- Add a SQLite plugin for persistent sessions and reliable delivery.
- Add a memory plugin with scoped search and guided or autonomous learning.
- Manage persistent memory as editable Markdown records.
- Store memory files in Crabbot's data directory.
- Use saved global and conversation memories in TUI replies.
- Include saved global memories in one-shot requests and require an explicit provider.
- Add memory command help and clear empty-search feedback.
- Add `crab memory converge` to find likely preference conflicts and identify newer memories.
- Schedule and immediately activate one-time and recurring agent tasks with IANA time zones.
- Tell the agent which tools, shell access, and messaging channels scheduled tasks can use.
- Run scheduled tasks with workspace tools, optional shell access, and channel messaging.
- Bind scheduled messages to the exact destination authorized when the task is created.
- Run up to four due scheduled agent tasks concurrently.
- Process concurrent model plugin requests with bounded concurrency.
- Pause tasks for review when an action may have completed before a failure.
- Allow explicitly configured, authorized messaging channels to schedule host shell actions.
- Interrupt a running scheduled agent turn when its schedule is cancelled.
- Run scheduled tasks on demand and opt into bounded retries with backoff.
- Schedule one-time tasks at explicit RFC 3339 timestamps.
- Schedule agent tasks at fixed intervals from the CLI or through the agent.
- Choose whether overdue schedules skip, coalesce, or replay missed work.
- Keep recurring schedules active after a failed occurrence exhausts retries.
- Recover interrupted schedules when the scheduler plugin restarts.
- Rerun completed or skipped one-time tasks on demand.
- Preserve terminal missed-task outcomes in schedule listings.
- Pause schedules when the agent does not explicitly confirm completion.
- Paginate schedule listings in the CLI, TUI, and scheduler API.
- Support the same full schedule timing and retry options in TUI commands as in the CLI.
- Add an explicit `--shell` opt-in for CLI-created scheduled tasks when shell access is enabled in configuration.
- Show full timing, execution, and recovery options in `crab schedule help`.
- Show the latest scheduled task outcome in schedule listings.
- Keep existing schedules when the scheduler reaches its task limit.
- Add a tools plugin for confined files and approved shell access.
- Add optional Docker and Podman sandboxes for shell commands.
- Add an MCP plugin for external tools, resources, and prompts.
- Add a Whisper plugin for local voice transcription.
- Add a TUI plugin with full-screen chat, editable input, and saved sessions.
- Manage sessions with fork, archive, restore, compact, and deep delete.
- Sync TUI transcripts and activity across clients.
- Stream replies and tool progress with timestamps, interrupts, and approvals.
- Browse sessions, deliveries, and plugins in paginated lists.
- Add runnable slash-command suggestions and plugin command descriptions.
- Render labeled Markdown code blocks in the TUI.
- Show context usage in configurable TUI statuslines.
- Add TUI controls for session-scoped memory and schedules.
- Paginate memory lists in the CLI and TUI, label record types, and filter by type.
- Resolve TUI memory forget commands by scope and explain ambiguous keys.
- Add workspace file reading, listing, searching, and session-specific roots.
- Run approved `!<command>` shell commands through the daemon.
- Fetch public web pages over HTTPS.
- Send images to supported model providers.
- Transcribe Telegram voice messages with the optional Whisper plugin.
- Edit messages and approve requests inline on Telegram and Discord.
- Manage pending and uncertain deliveries with list, retry, and drop commands.
- Apply channel policies for direct messages, groups, and allowlists.
- Load shared `CRAB.md` and `CLAW.md` guidance into model conversations.
- Add `crab init`, `crab doctor`, and `crabbot validate` commands.
- Read and update settings with `crab config get` and `crab config set`.
- Export, validate, and import portable Crabfiles.
- Install, link, preview, update, and uninstall plugins from the CLI.
- Install or link multiple plugins in one command.
- Inspect plugin health, versions, permissions, capabilities, and commands.
- Add plugin-contributed commands and capability-aware CLI help.
- Add `crabbot ask` for one-shot prompts.
- Add the `crab` alias, shell completions, and JSON command output.
- Save raw per-command application logs for verbose and debug CLI runs.
- Add structured command diagnostics.
- Prompt before destructive actions, with `--yes` for non-interactive use.
- Add native service management, including service restart.
- Add verified plugin archives and installers for Linux, macOS, and Windows.
- Protect provider credentials with private files or the operating system keyring.
- Add severity-based logs for commands, the daemon, and plugins.
- Record successful CLI state changes and safe message-processing outcomes at
  appropriate log levels.
- Report health with actionable reasons for incomplete local setups.
