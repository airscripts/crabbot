# Changelog

All notable Crabbot changes are documented here using Keep a Changelog and
Semantic Versioning.

## [Unreleased]

### Added

- Build a self-hosted agent runtime with independently installed model, channel, and capability plugins.
- Schedule one-time and recurring agent work with isolated run context, timezone-aware timing, bounded retries, five-run paginated history, and authorized tool and message access.
- Add a full-screen TUI and CLI controls for sessions, schedules, scoped memory, deliveries, and plugins.
- Add confined workspace tools, optional shell isolation, MCP integrations, voice transcription, and supported image inputs.
- Add setup, diagnostics, configuration, service, and portable Crabfile management commands.

### Changed

- Advance the plugin protocol to 0.2 for the scheduler capability while continuing to accept 0.1 plugins.
