# Changelog

All notable Crabbot changes are documented here using Keep a Changelog and
Semantic Versioning.

## [Unreleased]

### Added

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
- Add a timer plugin for calendar reminders with IANA time zones.
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
- Add TUI controls for session-scoped memory and timers.
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
- Add redacted debug reports and structured command diagnostics.
- Prompt before destructive actions, with `--yes` for non-interactive use.
- Add native service management, including service restart.
- Add verified plugin archives and installers for Linux, macOS, and Windows.
- Protect provider credentials with private files or the operating system keyring.
- Add severity-based logs for commands, the daemon, and plugins.
- Report health with actionable reasons for incomplete local setups.
