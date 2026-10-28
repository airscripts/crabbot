# Compatibility And Functional Verification

This file tracks end-to-end behavior by operating system and functional tests
for plugin capabilities. CI tracks architecture-specific builds and tests; use
the CI run for a commit when checking architecture coverage.

## Table Of Contents

- [CI Coverage](#ci-coverage)
- [Operating-System Functional Status](#operating-system-functional-status)
- [Operating-System Checklist](#operating-system-checklist)
- [Plugin Functional Tests](#plugin-functional-tests)
- [Recording Results](#recording-results)

## CI Coverage

The native test workflow runs on Linux, macOS, and Windows across x86_64 and
ARM64. The Windows ARM job runs supported tests without coverage collection.
Linux CI also runs the browser and isolated Xvfb computer integration fixtures.
These checks verify builds and automated tests; they do not replace the
operating-system or plugin functional checks below.

## Operating-System Functional Status

Run the checklist on one representative installation per operating system.
CPU architecture results remain in CI and do not need separate rows here.

| Operating System | Status | Evidence And Remaining Checks |
| --- | --- | --- |
| Linux | Partial | Local daemon and capability smoke checks passed on 2026-10-27. Browser and isolated Xvfb fixtures passed on Linux x86_64. Complete the end-to-end checklist, including a provider and channel round trip. |
| macOS | Not Run | CI covers native builds and tests. Complete the end-to-end checklist on a macOS installation, including any enabled interactive desktop behavior. |
| Windows | Not Run | CI covers native builds and tests. Complete the end-to-end checklist in a logged-in user session, including any enabled interactive desktop behavior. |

`Complete` means the checklist passed on an installed system and the result is
recorded in that OS's row. CI-only builds, unit tests, and plugin protocol
handshakes do not qualify as an operating-system functional check.

## Operating-System Checklist

Run these checks on Linux, macOS, and Windows. Use test accounts and a
disposable channel where an external provider is required.

- [ ] Initialize Krabb, run `krabb doctor`, inspect plugin health, and start,
  stop, and restart the daemon.
- [ ] Complete a model turn and a message round trip through a configured
  channel.
- [ ] Use a TUI session and verify the configured tool and approval behavior.
- [ ] Install, update, and uninstall an optional plugin; confirm diagnostics
  and persistent state after restarting the daemon.
- [ ] Exercise the optional plugins and platform modes intended for this
  installation, following their plugin READMEs.

See [workflows](krabb-docs/workflows.md), [providers](krabb-docs/providers.md),
[commands](krabb-docs/commands.md), and [safety](krabb-docs/safety.md) for setup
and operation instructions.

## Plugin Functional Tests

Protocol smoke checks verify manifest identity and capabilities, `ping`, and
clean shutdown. They do not prove that a plugin's advertised feature works.
The functional evidence below records local fixture results separately from
checks that still need a real provider, channel, backend, or installation.

| Plugins | Functional Evidence | Remaining Check |
| --- | --- | --- |
| Anthropic, Codex, Gemini, Ollama, OpenAI, OpenRouter | Workspace tests passed in `make verify` on 2026-10-27. | Send and stream a request through a test account or local Ollama service. |
| Discord, Signal, Slack, Telegram, WhatsApp | Workspace tests and protocol smoke passed on 2026-10-27. | Verify receive, send, acknowledgement, media, and reconnect with test accounts. |
| A2A, ACP, AG-UI | Workspace tests and protocol smoke passed on 2026-10-27. | Exercise a request lifecycle with a compatible external agent or client. |
| MCP | Protocol smoke and a loopback HTTP `tools/list` fixture passed on 2026-10-27. | Exercise the configured transport against an intended MCP server. |
| Memory, Scheduler, SQLite, Tools, TUI | Local capability smoke and daemon or IPC checks passed on 2026-10-27. | Repeat the relevant workflow in the target installation. |
| Whisper | Process smoke with a deterministic local transcriber fixture passed on 2026-10-27. | Verify transcription with the intended local backend. |
| Browser | Linux x86_64 Playwright fixture passed on 2026-10-27. | Verify the installed browser dependencies and workflows on each target OS. |
| Computer | Linux x86_64 isolated Xvfb fixture passed on 2026-10-27. | Verify interactive desktop behavior on macOS and Windows where enabled. |

The plugin rows group capability checks rather than crate test counts. Use
`make verify` and the CI run for automated test results; update this table when
a functional fixture or real integration check passes.

## Recording Results

For each operating-system run, record the OS version, date, Krabb version,
plugins exercised, and outcome in the corresponding status row. Keep fixture,
credentialed integration, and full installation results distinct. Do not mark
an operating system complete based only on a successful build or protocol
handshake.
