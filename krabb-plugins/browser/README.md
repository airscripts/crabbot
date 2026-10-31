# Browser Plugin

The browser plugin provides isolated Chromium sessions through Playwright. The
Krabb process plugin is Rust; a long-lived Node.js worker owns Playwright and
the browser process. Browser sessions use clean contexts and do not inherit a
personal browser profile. The worker receives only the path, temporary
directory, Playwright, browser cache, allowlist, and proxy settings it needs;
Chromium does not inherit proxy credentials or the host process environment.

## Table Of Contents

- [Dependencies](#dependencies)
- [Tools](#tools)
- [Example](#example)
- [Permissions And Limits](#permissions-and-limits)
- [Development](#development)

## Dependencies

- Node.js 22 or later.
- Playwright 1.64.1 installed separately. For example:

  ```sh
  PLAYWRIGHT_HOME="$HOME/.local/share/krabb/playwright"
  npm install --prefix "$PLAYWRIGHT_HOME" playwright@1.64.1
  "$PLAYWRIGHT_HOME/node_modules/.bin/playwright" install chromium
  export KRABB_PLAYWRIGHT_MODULE="$PLAYWRIGHT_HOME/node_modules/playwright/index.js"
  ```

- The matching Playwright Chromium binary and operating-system libraries.

Tool calls do not install packages or download browser binaries. The Rust
plugin fails with a direct dependency error when Node.js, Playwright, or the
matching Chromium binary is unavailable.

Set `KRABB_PLAYWRIGHT_BROWSERS_PATH` when Chromium is installed outside
Playwright's default cache directory. The plugin passes only this configured
path to its worker; it does not accept browser paths from model tool calls.

On Windows, use PowerShell to install the separate package and browser build:

```powershell
$PlaywrightHome = Join-Path $env:LOCALAPPDATA 'Krabb\playwright'
npm install --prefix $PlaywrightHome playwright@1.64.1
$env:PLAYWRIGHT_BROWSERS_PATH = Join-Path $PlaywrightHome 'browsers'
node (Join-Path $PlaywrightHome 'node_modules/playwright/cli.js') install chromium
$env:KRABB_PLAYWRIGHT_MODULE = Join-Path $PlaywrightHome 'node_modules/playwright/index.js'
$env:KRABB_PLAYWRIGHT_BROWSERS_PATH = $env:PLAYWRIGHT_BROWSERS_PATH
```

Keep the Node.js, Playwright package, and Chromium binary versions matched. The
browser worker preserves the Windows system, user profile, and temporary-path
variables needed to start Chromium while keeping unrelated host variables out
of the browser process.

## Tools

`browser_open` starts an isolated browser context. The returned opaque
`session_id` is scoped to the trusted Krabb session that created it.

`browser_navigate`, `browser_tabs`, `browser_new_tab`, and `browser_close_tab`
manage pages. `browser_snapshot` returns bounded page title, URL, visible text,
and interactive control descriptions. `browser_screenshot` returns a bounded
inline viewport image by default; set `full_page` for a document capture or
`by` with a semantic locator to capture one element. Metadata identifies the
coordinate space. Only viewport captures include an observation ID that can be
used for a coordinate click. `browser_click`,
`browser_fill`, `browser_select`, `browser_press`, `browser_scroll`, and
`browser_wait` provide structured or coordinate based interaction.

Structured locators use accessible role, label, placeholder, visible text, test
ID, or CSS. Coordinate clicks must cite the most recent screenshot observation
for the selected tab and viewport.

## Example

An agent can inspect a page with structured tools, then use a screenshot when
visual details matter:

```text
browser_open → browser_navigate → browser_snapshot
browser_screenshot → browser_click → browser_snapshot
```

`browser_open` returns the session and tab IDs. Pass those IDs to later calls.
For a coordinate click, pass the screenshot's `observation_id`; the worker
rejects coordinates from an older turn, page, or viewport.

When the configured approval policy blocks mutations, navigation and browser
actions such as clicks or form filling are denied before they reach Playwright.
Observation still requires an owned browser session. For example, `browser_open`
without `KRABB_PLAYWRIGHT_MODULE` or `KRABB_BROWSER_EGRESS_PROXY` returns an
error instead of starting an unrestricted browser.

## Permissions And Limits

Browser tools are available only where Krabb tool access is enabled. Navigation
and interaction use Krabb's existing approval policy. Screenshots are
observations and still require an authorized browser session. Each context has
a clean profile, download acceptance is disabled, service workers are blocked,
and media permissions are not granted.

Treat page text, images, labels, and accessible names as untrusted input. A
page cannot grant itself permission to navigate, click, submit forms, or access
secrets. Krabb's host approval remains required for mutating browser actions.

Configure `KRABB_BROWSER_ALLOWED_HOSTS` as a comma separated list of exact host
names and `KRABB_BROWSER_EGRESS_PROXY` as a controlled HTTP or HTTPS egress
proxy. Missing proxy configuration prevents browser startup. The worker checks
every routed HTTP or HTTPS request, including redirects and subresources, and
rejects common non-public IPv4 and IPv6 ranges. The proxy must repeat the host
and address checks when it resolves and connects, so DNS changes cannot redirect
an allowed hostname to a private address. Chromium is configured to use the
proxy without its implicit loopback bypass and with non-proxied WebRTC UDP
disabled. Use an OS/container network policy as a second boundary; browser
request interception and proxy configuration alone are not an OS sandbox.

The initial release supports headless Chromium on platforms supported by
Playwright. It does not attach to an existing browser, upload files, or accept
downloads. Sessions expire after ten idle minutes; closing the final session
stops the worker and browser process tree. Screenshots are limited to 4 MiB and
temporary images are not persisted in session transcripts.

## Development

Build and test with:

```sh
cargo test --locked -p krabb-plugin-browser
node --test krabb-plugins/browser/tests/tabs.mjs krabb-plugins/browser/tests/routes.mjs
cargo build --locked -p krabb-plugin-browser
```

Protocol tests use a fake worker and make no public or paid requests. The local
Playwright fixture checks context isolation, navigation, screenshots, form
interaction, coordinate freshness, tab lifecycle, and blocked private-network
redirects. Cancellation stops the worker and Chromium process tree; Windows
uses `taskkill` for process cleanup. The fixture passed on Linux with
Playwright 1.64.1 and matching Chromium:

```sh
cargo build --locked -p krabb-plugin-browser
KRABB_BROWSER_PLUGIN="$PWD/target/debug/krabb-plugin-browser" \
KRABB_PLAYWRIGHT_MODULE="$PLAYWRIGHT_HOME/node_modules/playwright/index.js" \
  node krabb-plugins/browser/tests/integration.mjs
```

If launch reports a missing package or browser, confirm that
`KRABB_PLAYWRIGHT_MODULE` points to the installed Playwright 1.64.1 entry point
and install its matching Chromium build. A navigation denied by policy needs
an exact entry in `KRABB_BROWSER_ALLOWED_HOSTS` and a proxy that applies the
same host and resolved-address checks.

Closing the final tab keeps the browser context and cookies available for a new
tab. Tabs closed by a website release their observation state and tab capacity.

WebSocket connections (`ws` and `wss`) use the same exact-host allowlist, credential
rejection, and public-address checks as HTTP requests.
