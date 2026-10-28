# Compatibility And Deployment Checks

This matrix records declared support, automated coverage, and manual deployment
evidence separately. A platform is not marked manually verified until its
interactive behavior has been exercised on a real deployment.

## Operating-System Matrix

| Platform | Browser Availability | Browser Verification | Computer Availability | Computer Verification | Manual Deployment |
| --- | --- | --- | --- | --- | --- |
| Linux x86_64 | Headless Chromium with matching Playwright install | Workspace CI; browser integration fixture in Linux verification | Isolated X11 through Xvfb | Workspace CI; authenticated Xvfb integration fixture | Pending |
| Linux aarch64 | Headless Chromium with matching Linux ARM64 Playwright browser install | Workspace CI only; integration fixture runs on Linux x86_64 | Isolated X11 through Xvfb | Workspace CI only; integration fixture runs on Linux x86_64 | Pending |
| macOS x86_64 | Headless Chromium with matching Playwright install | Workspace CI only; no browser integration run configured | Interactive signed-in desktop; Accessibility and Screen Recording permissions required | Native workspace tests/build; no desktop integration run configured | Pending |
| macOS aarch64 | Headless Chromium with matching Playwright install | Workspace CI only; no browser integration run configured | Interactive signed-in desktop; Accessibility and Screen Recording permissions required | Native workspace tests/build; no desktop integration run configured | Pending |
| Windows x86_64 | Headless Chromium with matching Playwright install | Workspace CI only; no browser integration run configured | Interactive signed-in user session; services, session 0, secure desktops, and elevation prompts are unsupported | Native workspace tests/build; no desktop integration run configured | Pending |
| Windows aarch64 | Playwright 1.64.1 selects x64 Chromium; browser use depends on Windows x64 emulation | Workspace Rust tests/build only; Node.js and Chromium integration not configured | Interactive signed-in user session; services, session 0, secure desktops, and elevation prompts are unsupported | Native workspace tests/build; no desktop integration run configured | Pending |

The test workflow has native runners for all six platform and architecture
rows. Its Windows ARM job runs workspace tests without coverage. Those Rust
checks validate platform compilation and backend-independent behavior; they do
not exercise the browser worker or interactive computer desktop outside the
Linux x86_64 integration fixture.

Browser support depends on installing the documented Node.js, Playwright, and
matching browser binaries for the target OS and architecture. The browser plugin
does not download executable packages at runtime. Playwright 1.64.1 selects its
x64 Chromium build on Windows ARM64. That configuration has no integration
check in this repository and must remain unverified until tested with the
intended Node.js runtime and Windows x64 emulation.

Linux computer mode controls only the plugin-owned Xvfb display and uses a
private Xauthority cookie. macOS and Windows use the logged-in desktop and are
available only when both `KRABB_COMPUTER_MODE=interactive` and
`KRABB_COMPUTER_ALLOW_HOST_CONTROL=1` are set. Those modes are not isolated
desktops; use them only where the operator permits host input and screen
capture. Native OS privacy settings can still deny access. Windows must be run
inside an interactive logged-in user session, not as a service or on the secure
desktop.

## Automated Verification

The native test workflow runs the workspace on Linux x86_64/aarch64, macOS
x86_64/aarch64, and Windows x86_64/aarch64. The Windows ARM job builds and runs
its supported checks without the coverage job. The build workflow also compiles
the release targets. The Linux verification workflow runs the Playwright browser
fixture and the authenticated Xvfb computer integration fixture.

These workflows show configured automated coverage. Their presence does not
record the result of a particular CI run. Check the associated Actions run for
the commit being reviewed.

## Verification From This Worktree

The integration and cross-target checks below ran locally on 2026-10-26.
Workspace verification was rerun for this change on 2026-10-27. Cross-target
checks compile Rust tests for the target; they do not run the test binaries or
exercise native OS desktop behavior.

| Check | Result | Scope |
| --- | --- | --- |
| `make verify` (2026-10-26) | Passed | Linux x86_64 formatting, Clippy, workspace checks/tests, coverage, build, and metrics |
| `make verify` (2026-10-27) | Passed | Linux x86_64 formatting, spacing, Clippy, workspace checks/tests, coverage, build, and metrics |
| Plugin Package Completeness Audit (2026-10-27) | Passed | All 23 plugins have a manifest, README, Rust binary source, and at least four tests; no `todo!`, `unimplemented!`, or `FIXME`. One Gemini 2 compatibility cleanup TODO remains documented in source |
| Plugin Protocol Smoke (2026-10-27) | Passed | All 23 built plugins matched manifest identity/capabilities, answered `ping`, and shut down cleanly using isolated temporary state and no provider credentials. WhatsApp used an ephemeral loopback listener |
| MCP HTTP Loopback Smoke (2026-10-27) | Passed | Built MCP plugin completed `hello`, forwarded an authenticated `tools/list` request to a local HTTP fixture, returned its response, and shut down over JSONL |
| Whisper Process Smoke (2026-10-27) | Passed | Plugin binary completed `hello`, `ping`, `transcribe`, and `shutdown` over JSONL using an isolated media file and deterministic local transcriber fixture; no real transcription backend was exercised |
| TUI Workspace Tools IPC (2026-10-27) | Passed | Daemon `session.answer` used an advertised read tool with a local fixture provider and tool process; the fixture verified the tool result was included in the final answer |
| Local Capability Smoke (2026-10-27) | Passed | Memory remember/list/forget, SQLite put/get/delete, pending scheduler add/list, and confined tools list/read against temporary state |
| Local Daemon Capability Smoke (2026-10-27) | Passed | In an isolated home, six local capability plugins plus Codex loaded with healthy daemon status; Memory status/remember/list/forget and Scheduler list/add/pause/list passed. No provider request was sent; the temporary schedule was left paused |
| Browser Integration Fixture | Passed | Linux x86_64 with Node.js 24.22.0, Playwright 1.64.1, and matching Chromium |
| Computer Integration Fixture (2026-10-27) | Passed | Linux x86_64 local rerun with the built plugin, authenticated isolated Xvfb, and a Tk fixture; verified dimensions, PNG capture, click, typing, and close |
| Browser Target Checks | Passed | Windows x86_64/aarch64 MSVC and macOS x86_64/aarch64; `cargo check --tests` |
| Computer Target Checks | Passed | Windows x86_64/aarch64 MSVC and macOS x86_64/aarch64; `cargo check --tests` |

These local results do not replace checking the CI run for the commit or the
manual deployment record below.

## Plugin Manual Smoke Status

The automated and protocol checks cover the repository's local baseline. They
do not contact paid providers or use real channel credentials. Complete these
live checks with operator-owned test accounts before release:

| Group | Plugins | Manual smoke still needed |
| --- | --- | --- |
| Model providers | Anthropic, Codex, Gemini, Ollama, OpenAI, OpenRouter | Generate and stream a short response with a test account or local Ollama service; verify tool and image behavior where supported |
| Messaging channels | Discord, Signal, Slack, Telegram, WhatsApp | Verify account setup, receive, send, acknowledgement, media, and reconnect behavior with a test channel/account |
| Agent and protocol bridges | A2A, ACP, AG-UI, MCP | Connect to a real external agent, client, or MCP server and exercise the advertised request lifecycle |
| Local capabilities | Memory, Scheduler, SQLite, Tools, TUI, Whisper | Memory and Scheduler passed daemon operations; SQLite passed daemon IPC smoke with the installed plugin binary; Tools passed direct capability smoke. TUI workspace tools passed both the daemon IPC fixture and installed CLI fixture smoke. The smoke exposed and verified a startup-readiness race fixed by publishing IPC after installed plugins finish loading. Whisper passed the fixture process smoke above. Still exercise Whisper with a real local transcription backend |
| Browser and computer | Browser, Computer | The isolated Computer Xvfb fixture passed locally; interactive macOS/Windows desktop behavior remains deployment-specific. Browser's local egress fixture passed earlier; this worktree lacks Node.js, Playwright, and Chromium for a rerun. Complete the deployment checklists on each supported OS |

All five groups passed the 2026-10-27 protocol smoke and workspace verification.
The plugin-specific manual checks in this table remain outstanding until their
external services, local backends, or installed deployments have been exercised.

## Manual Deployment Checklist

Run these checks on each target deployment before describing that deployment as
manually verified. Record the OS version, CPU architecture, dependency versions,
date, and outcome beside each checked item in the release evidence. A Rust
workspace test result alone does not count as a browser or desktop integration
result.

### Browser Plugin

- [ ] Install the supported Node.js release, Playwright package, and matching
  Chromium browser without relying on runtime downloads.
- [ ] On Windows, confirm `SystemRoot`, user profile, and temporary directory
  values are available to the plugin worker and Chromium.
- [ ] Open a local test page, navigate, inspect the page, and take viewport,
  full-page, and element screenshots.
- [ ] Exercise click, typing, scrolling, and fresh-coordinate handling.
- [ ] Confirm loopback/private-address navigation and blocked redirects or
  popups are rejected by the configured network policy.
- [ ] Close the browser session and confirm browser child processes exit.

### Linux Computer Plugin

- [ ] Install Xvfb, `xauth`, `xdotool`, ImageMagick `import`, and the local Tk
  fixture dependency.
- [ ] Open the isolated display and confirm the returned dimensions and window
  list match the fixture.
- [ ] Capture a PNG and JPEG, then exercise pointer, keyboard, and text input.
- [ ] Close the session and confirm Xvfb, the fixture process, and the private
  Xauthority file are removed.
- [ ] Confirm the plugin never connects to the user's ambient `DISPLAY`.

### macOS Computer Plugin

- [ ] Enable interactive mode and explicitly allow host desktop control.
- [ ] Grant the required Accessibility and Screen Recording permissions, then
  restart the plugin if the OS requires it.
- [ ] Capture the primary display and enumerate visible windows.
- [ ] Exercise pointer, keyboard combinations, text entry, and scrolling in a
  disposable local application.
- [ ] Close the session and confirm later calls require a new owned session.
- [ ] Confirm the operator understands actions affect the signed-in desktop.

### Windows Computer Plugin

- [ ] Enable interactive mode and explicitly allow host desktop control.
- [ ] Run the plugin in the logged-in user's interactive session, not as a
  service, in session 0, on a secure desktop, or against an elevation prompt.
- [ ] Capture the primary display and enumerate visible windows under the
  deployment's user/session policy.
- [ ] Exercise pointer, keyboard combinations, text entry, and scrolling in a
  disposable local application.
- [ ] Close the session and confirm later calls require a new owned session.
- [ ] Confirm the operator understands actions affect the signed-in desktop.

## Manual Deployment Record

Update the matching row after running the checklist on an installed deployment.
Keep CI results and manual deployment results distinct.

| Platform | Status | OS Version And Build | Architecture | Dependency Versions | Date And Tester | Result Or Evidence |
| --- | --- | --- | --- | --- | --- | --- |
| Linux x86_64 | Not Run | — | — | — | — | — |
| Linux aarch64 | Not Run | — | — | — | — | — |
| macOS x86_64 | Not Run | — | — | — | — | — |
| macOS aarch64 | Not Run | — | — | — | — | — |
| Windows x86_64 | Not Run | — | — | — | — | — |
| Windows aarch64 | Not Run | — | — | — | — | — |

## Product Acceptance Flows

| Flow | Evidence |
| --- | --- |
| Telegram Text And Duplicate Delivery | `loads_channel_and_model_during_bridge`, `drains_queued_channel_events` |
| Telegram Image Handling | `resolves_telegram_media_references`, `prepares_only_bounded_confined_images`, `pins_image_media_outside_the_expiring_cache` |
| Telegram Voice Handling | `transcribes_voice_with_the_speech_plugin_and_removes_raw_audio`, `omits_unsafe_voice_files_without_leaking_their_paths` |
| Telegram Tools And Approvals | `runs_a_tool_turn_before_replying`, `prompts_for_signed_approval_before_mutating_tools`, `denied_prompt_never_dispatches_a_mutating_tool` |
| Discord Media And Uploads | Discord normalization and media confinement tests, including text files, images, and voice-message audio |
| Memory And Schedules | `memory_can_remember_list_and_forget`, scheduler lifecycle, retry, recovery, and execution tests |
| Browser Automation | Browser ownership, worker timeout, and routing tests; Playwright fixture covers contexts, actions, screenshots, coordinate freshness, and blocked loopback redirects/popups |
| Computer Tools | Linux unit and Xvfb integration tests; macOS/Windows native desktop checks are listed above and remain pending until run |
| Discord Routing And Authorization | Discord `classifies_gateway_events`, `normalizes_gateway_messages`, and `prepares_approval_controls_and_normalizes_interactions` |
| Isolated Worktrees | `isolates_group_workspaces` |
| Provider And Model Switching | `manages_local_sessions_and_plugins`, `runs_session_commands_without_daemon` |
| Core-Only Diagnostics | `runs_non_interactive_commands`, `checks_plugin_readiness` |
| Plugin Update And Rollback | `updates_active_plugins_without_restarting_the_daemon`, `updates_plugins_offline_when_the_daemon_is_absent`, `manages_local_sessions_and_plugins` |

Live provider, channel, and service-manager credentials are not required for
automated acceptance. Operators should perform credentialed smoke checks before
publishing a release, following the [Release](krabb-docs/release.md) checklist.
