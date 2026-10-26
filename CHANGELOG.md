# Changelog

All notable Krabb changes are documented here using Keep a Changelog and
Semantic Versioning.

## Unreleased

### Added

- Tool plugins can publish schemas through `tools/list`; the runtime routes
  calls across active providers and preserves validated image results for
  vision-capable model requests.
- Add browser and isolated X11 computer tool plugins with inline screenshot
  observations and explicit runtime dependencies.
- Support viewport, full-page, and element browser screenshots with explicit
  coordinate-space metadata.
