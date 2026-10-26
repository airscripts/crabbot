# Native Browser And Computer Implementation Ledger

## Repository Inventory

- `krabb-core` owns JSON-RPC types and bounded framing. `Content` already has
  normalized image content, and `ModelRequest` already carries `ToolSpec`s.
- `krabb-runtime` owns plugin processes, turns, sessions, approvals, cancellation,
  and restart handling. Before this change it exposed one `Tool` plugin and
  converted tool outputs to text.
- `krabb-plugins/tools` implements the current workspace operations. Its
  schemas were previously assembled in the runtime.
- Model plugins for OpenAI, Anthropic, Codex, Gemini, Ollama, and OpenRouter
  already contain image serializers. The native event based loop can now pass
  tool images as `Role::Tool` content; Codex and ACP's separate host callback
  adapters still consume the compatibility text field.
- Approval policy and unsafe turn tracking live in the runtime. Plugin
  manifests validate capabilities and permissions; package lists and CI
  matrices are maintained separately in the root scripts and workflows.
- The worktree started clean on branch `interaction`, at the Krabb names and
  directory layout. No browser or computer plugin existed.

## Decisions

- Additive tool discovery uses `tools/list` on each active `tool` plugin. The
  runtime owns the name-to-plugin map, rejects duplicate names, and keeps the
  existing built-in schemas as a compatibility fallback for the legacy `tools`
  plugin.
- Tool results use normalized text and image content with optional structured
  metadata. Inline image data is validated and bounded before it enters model
  context. Temporary tool images stay in the active turn and are replaced by
  text in persisted session history.
- Provider-specific image wire formats remain inside their provider plugins.
  The core stays provider-neutral.
- The browser backend is a supervised Playwright worker pinned to Playwright
  1.64.1. It provides semantic locators, browser contexts, and automatic waits;
  a Rust-native CDP option avoids Node.js but exposes a less complete locator
  abstraction and has active lifecycle/action reliability issues. The browser
  package and Chromium are separately installed and never downloaded during a
  tool call.
- The browser worker applies per-request URL, host allowlist, and DNS address
  checks, but those checks are not an OS-level egress boundary. Hard network
  containment remains an implementation requirement before production use.

## Phase Status

| Phase | Status | Evidence |
|---|---|---|
| 1. Architecture investigation | Complete | Source inspection and browser backend decision recorded above. |
| 2. Generic tool registry | Complete | `tools/list`, ownership routing, duplicate rejection, schema checks, and trusted session/turn context are implemented. |
| 3. Multimodal tool results | Complete | Bounded image content reaches vision-capable providers and is omitted from persisted transcripts; screenshot tools are hidden from providers without vision support. |
| 4. Browser foundation | In progress | Rust plugin, Playwright worker, session ownership, tool schemas, exact-host filtering, required proxy, and worker process-group cancellation are present. Live Playwright tests and externally enforced egress remain. |
| 5. Browser interaction | In progress | Navigation, tabs, snapshots, viewport/full-page/element screenshots, semantic actions, coordinate freshness, bounded waits, and mutating-tool approval are present; live interaction and timeout recovery need validation. |
| 6. Computer foundation | In progress | The plugin starts an authenticated, fixed-size Xvfb server on Linux and can launch one fixed operator-configured application. It does not touch the ambient desktop. |
| 7. Computer interaction | In progress | Bounded pointer, click, drag, scroll, key, text, and screenshot calls use `xdotool`; cancellation schedules release of mouse buttons and common modifiers. Real desktop integration remains unverified. |
| 8. Security hardening | In progress | Tool schemas, trusted owner routing, image limits, existing approval policy, private X authority, and cancellation paths exist. OS network/process/filesystem isolation remains deployment policy; browser proxy and remote-channel integration need validation. |
| 9. End-to-end integration | In progress | A fake provider verifies that a tool screenshot reaches a later vision request as image data. Local Playwright and Xvfb interaction harnesses now run in dedicated CI jobs; those jobs have not run from this worktree. |
| 10. Documentation and distribution | Complete | Plugin READMEs, user docs, Cargo workspace, package scripts, coverage lists, and CI package matrices include both plugins. Runtime dependencies remain external and explicit. |

## Validation

- The latest `make verify` passed on Linux, including formatting, the Rust
  spacing check, workspace Clippy with warnings denied, workspace check,
  workspace tests, package coverage gates, build, and source metrics. All
  changed packages cleared their configured coverage gates.
- The macOS scheduler failure in the supplied CI log was traced to its fake
  scheduler not answering `reports`, adding the runtime RPC timeout to the
  recovery wait. The fixture now returns an empty report page, and the test
  passed three local repetitions and the full runtime suite. The restart test
  and two other short scheduler readiness waits were given longer deadlines.
- A separate shutdown test had been signaling after a fixed 100 ms while its
  fake tools plugin did not answer `tools/list`. The fixture now exercises the
  legacy listing fallback, echoes request IDs, and signals shutdown only after
  its stalled `read` call reaches the plugin. This removes the startup timing
  race and verifies interruption during the actual tool call.
- Browser screenshots default to the viewport and also support bounded
  full-page or semantic-element captures. Their metadata identifies the
  coordinate space; only viewport screenshots authorize coordinate clicks.
- The multimodal tool test confirms screenshot bytes reach a later vision
  request and are not retained in saved session history. Runtime tests passed
  277/277.
- A scan of runtime asynchronous test waits found the shutdown fixture's
  fixed-delay race described above. Other waits were kept where they represent
  cancellation or protocol deadlines rather than readiness polling.
- `node --check` passed for the browser worker and local browser integration
  harness. Live Playwright behavior remains unverified because Playwright is
  not installed and system Chromium cannot start in this environment. The
  fixture covers storage isolation, dynamic controls, viewport/full-page/
  element PNG/JPEG captures, coordinate freshness, and redirect and popup
  attempts to reach loopback.
- Computer unit tests passed. A new local integration harness launches a
  disposable Tk application under the plugin-owned Xvfb session and checks
  screenshots, window discovery, clicks, typing, and cleanup. This environment
  lacks Xvfb, `xdotool`, ImageMagick `import`, and tkinter, so it could not run
  here.
- Dedicated browser and computer integration jobs were added to `verify.yml`.
  They must pass on CI before those end-to-end acceptance requirements are
  considered verified.
- Browser's eight tests and computer's eight tests pass locally. The protocol
  cancellation test and all 277 runtime tests passed in the latest `make verify`
  run. Windows GNU `cargo check` passes for both plugins.
- A final process check found no leftover Cargo, plugin, Node, or Xvfb test
  processes.
- The macOS fix still needs CI confirmation. If it repeats, replace the longer
  readiness deadline with deterministic synchronization.
- Cancellation is a JSON-RPC notification keyed to the active request ID. The
  core server test confirms it aborts only the matching handler and continues
  serving requests. Browser cancellation kills the worker process group;
  computer cancellation releases held input asynchronously.
- The browser enforces an exact host allowlist and validates DNS answers for
  each routed request, but the required egress proxy and OS/container network
  policy are operator-provided. This repository does not implement or verify
  that external network boundary; SSRF containment therefore remains a
  deployment requirement.
