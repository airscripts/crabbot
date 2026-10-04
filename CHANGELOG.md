# Changelog

All notable Crabbot changes are documented here using Keep a Changelog and
Semantic Versioning.

## [Unreleased]

### Added

- Report health as healthy or unhealthy, with clear reasons for local-only setups.
- Add deep session deletion and `/compact` history summaries to the TUI.
- Show context usage in configurable TUI statuslines.
- Render fenced Markdown code blocks with language labels in the TUI.
- Fetch public web pages over HTTPS.
- Load shared `CRAB.md` and `CLAW.md` instructions into model conversations.
- Add `crab config get` and `crab config set` commands.
- Add per-session workspaces and confined workspace tools.
- Add TUI file reading, listing, searching, and optional approved shell access.
- Add full-screen TUI chat, saved sessions, and editable input.
- Add TUI session management, including archive, restore, and deletion.
- Sync TUI transcripts and session activity across clients.
- Add streamed replies, interrupt controls, timestamps, and approval controls.
- Add slash-command suggestions, command descriptions, and paginated lists.
- Run `!<command>` through the daemon's configured shell.
- Add `crab service restart` and native service support across platforms.
- Add `crab init`, `crab doctor`, and `crabbot validate` commands.
- Add Crabfile import and export commands.
- Add the `crab` alias, shell completions, and JSON output.
- Add plugin installation, linking, updates, previews, and in-place activation.
- Install or link multiple bundled plugins in one command.
- Show plugin health, versions, permissions, capabilities, and commands.
- Add model plugins for Claude, Codex, Gemini, Ollama, and OpenRouter.
- Add messaging plugins for Discord, Signal, Slack, Telegram, and WhatsApp Cloud.
- Add Codex ChatGPT sign-in and account model listing with `crab codex models`.
- Add Gemini 3 tool calls and set `gemini-3.8-flash` as the default model.
- Add image inputs for supported model providers.
- Add Telegram voice transcription through the optional Whisper plugin.
- Add message edits and inline approvals for Telegram and Discord.
- Stream model replies and tool progress through supported messaging channels.
- Add authenticated delivery listing, retry, and drop commands.
- Add scoped memory with guided and autonomous modes and editable Markdown records.
- Add persistent timers with calendar scheduling and IANA time zone support.
- Add TUI controls for session-scoped memory and timers.
- Add channel policies for direct messages and group allowlists.
- Add keyring credentials and protected credential files for providers and channels.
- Add Docker and Podman sandboxes for approved shell commands.
- Add plugin archives and installers for six release targets.
- Add CLI and daemon executables with plugin-contributed commands.
- Add `crabbot ask` and capability-aware CLI help.
- Add JSON diagnostics and redacted debug reports with `--debug`.
