# Changelog

All notable Krabb changes are documented here using Keep a Changelog and
Semantic Versioning.

## Unreleased

### Added

- Expose SQLite session-scoped values through the TUI `/store` command.
- Tool plugins can publish schemas through `tools/list`; the runtime routes
  calls across active providers and preserves validated image results for
  vision-capable model requests.
- Add browser and isolated X11 computer tool plugins with inline screenshot
  observations and explicit runtime dependencies.
- Add explicitly enabled interactive computer control for macOS and Windows.
- Support viewport, full-page, and element browser screenshots with explicit
  coordinate-space metadata.

### Fixed

- Keep one-shot TUI diagnostics out of CLI output so temporary session IDs are
  not written to standard output or error.
- Start the computer plugin's Xvfb server on its configured display.
- Simplify the computer session idle check to satisfy current Clippy.
- Preserve required Windows runtime variables when launching the browser worker
  and Chromium.
