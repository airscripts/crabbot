# Plugins

Official plugins are small Rust binaries under `krabb-plugins/`. Each folder
contains a manifest and a one-line installation example. Community plugins may
use another language if they implement the JSON-RPC 2.0 protocol.

## Manifest

```toml
id = "example"
version = "0.1.0"
capabilities = ["model"]
permissions = ["network"]
secrets = ["KRABB_CODEX_KEY"]

[protocol]
major = 0
minor = 1

[[commands]]
name = "example"
description = "Run the example command."
interactive = false
```

Use a stable one-word ID. Declare secret names, never secret values. Declare
every permission and external endpoint so installation and updates can show
the operator what changes. Command names must be unique across installed plugins
and cannot shadow native commands. Capability, permission, and secret lists are
validated for known, unique values before a plugin can be installed or started.
Protocol minor versions are backward-compatible: this host accepts plugins
declaring 0.0 or 0.1. The current protocol is 0.1, and all bundled plugins
declare 0.1.

`plugins.lock` records the canonical source, revision marker, SHA-256 of the
manifest and executable, protocol, version, capabilities, permissions, and
secrets. An explicit Git `--revision` is pinned for later updates; unpinned Git
sources follow their configured default revision. Local and remote updates
health-check staged binaries before activation and refresh the hash only after
staged activation succeeds; lock persistence failure restores the previous
plugin bytes. Git
sources may use HTTPS, SSH, or local `file://` URLs. Archive
sources must use `.tar`, `.tar.gz`, `.tgz`, or `.zip` and include a SHA-256
fragment; remote archives require HTTPS. Krabb rejects embedded URL
credentials, unsafe archive paths, and archive symbolic links. Release archives
place plugin executables in `bin/` beside their manifest.

## Requests

Model plugins that accept images declare the `vision` capability in both their
manifest and `hello` response. The host preflights that capability before it
dispatches an image turn; the capability describes request-shape support, not
whether every selected model accepts vision. Model plugins accept `generate`
with a `ModelRequest` containing a model name,
normalized messages, a stream flag, and an optional active workspace path. The
workspace field is backward-compatible and gives providers the isolated session
root instead of making them infer it from process state. Streamed requests may
emit bounded `event` notifications before the correlated response; each event
must contain a normalized `Event` object, and malformed notifications are
rejected. Providers that support streamed tool calls route each request through
the host's policy callback. They return `ModelReply` or a JSON-RPC error. Keep
stdout reserved for JSON-RPC frames; write diagnostics to stderr.

Agent plugins can call `host/model_stream` to receive normalized text and tool
events in `ModelReply.events`. The host collects these events and returns them
with the completed reply.

Test plugins with malformed frames, oversized frames, cancellation, process
exit, unknown methods, and protocol mismatch before publishing an archive.

## Authoring

Start a plugin as an independent process that reads and writes bounded
JSON-RPC 2.0 messages over the host framing transport. Complete the
`hello` handshake before serving requests, keep diagnostics on stderr, and
return a JSON-RPC error for malformed or unsupported calls. Use bounded reads,
bounded writes, and a clean `shutdown` path; the host may terminate the process
tree after a timeout.

The smallest useful plugin usually needs these handlers:

```text
hello     describe the plugin ID, version, protocol, and capabilities
ping      confirm that the process is healthy
shutdown  stop accepting work and exit cleanly
command   run a manifest-registered CLI command
```

Capability methods are intentionally typed by the normalized protocol rather
than by vendor wire formats. A model plugin implements `generate`; a channel
plugin implements `poll`, `send`, and acknowledgement methods; a tool plugin
implements only the operations it declares. Media-capable channels may expose
`media` to resolve a normalized external reference into a bounded private file.
Do not add a direct dependency from one plugin to another.

### Tool Discovery And Results

A plugin advertising the `tool` capability can expose its model-callable tools
through `tools/list`. Return an object with a `tools` array; every entry uses
the normalized `ToolSpec` fields `name`, optional `description`, and JSON
Schema `schema`. Keep names unique across all active tool plugins. The runtime
rejects duplicate names rather than routing a call to an arbitrary provider.
The legacy `tools` plugin keeps its built-in schemas as a fallback for older
installed binaries that do not implement `tools/list`.

Tool methods may return the legacy `{ "text": "..." }` result or normalized
multimodal content:

```json
{
  "content": [
    { "kind": "text", "text": "Screenshot captured." },
    {
      "kind": "image",
      "uri": "data:image/png;base64,...",
      "alt": "Browser viewport"
    }
  ],
  "metadata": { "width": 1280, "height": 720 }
}
```

For non-`tools` providers, invocation parameters use a trusted envelope:
`{ "arguments": <model arguments>, "host": { "session": "...", "turn": "..." } }`.
The runtime supplies the host values after validating the model arguments. A
plugin must use them for ownership and turn-scoped observations; it must never
accept model-supplied values as a substitute.

The runtime validates inline PNG/JPEG image data and enforces its image and
protocol limits before adding it to the next model request. The selected model
must advertise vision support. Temporary tool images remain available during
the current turn and are replaced with text in persisted transcripts.

The `browser` plugin requires Node.js, Playwright 1.64.1, and a separately
installed Chromium build. Install those dependencies explicitly; browser turns
never download executable packages. Its URL allowlist and request filters are
defense in depth and do not replace an operating-system egress boundary. See
[`krabb-plugins/browser/README.md`](../krabb-plugins/browser/README.md).

The `computer` plugin supports an isolated X11 session on Linux. Set
`KRABB_COMPUTER_MODE=isolated` and `KRABB_COMPUTER_DISPLAY=:N` (`N` greater
than zero). The plugin starts Xvfb with a private Xauthority cookie and can
start one operator-configured application. It also supports interactive desktop
control on macOS and Windows when both `KRABB_COMPUTER_MODE=interactive` and
`KRABB_COMPUTER_ALLOW_HOST_CONTROL=1` are set. Interactive mode controls the
signed-in desktop and requires the OS privacy permissions for input and screen
capture; it does not provide an isolated desktop. Its opaque session ID is
required for every later action, and sessions expire after ten idle minutes.
Linux requires Xvfb, `xauth`, `xdotool`, and ImageMagick `import`. A virtual
display does not itself confine processes, files, or network access.
See
[`krabb-plugins/computer/README.md`](../krabb-plugins/computer/README.md).

## Create A Custom Plugin

Custom plugins are supported today. A plugin source directory needs a manifest
and a built executable with this layout:

```text
hello/
├── krabb-plugin.toml
└── bin/
    └── krabb-plugin-hello
```

On Windows, use `krabb-plugin-hello.exe`. The manifest ID, executable name,
and command install ID must match. A command-only plugin can start with no
capabilities:

```toml
id = "hello"
version = "0.1.0"
capabilities = []
permissions = []
secrets = []

[protocol]
major = 0
minor = 1

[[commands]]
name = "hello"
description = "Run the Hello plugin."
interactive = false
```

The executable must complete the `hello` handshake and implement `ping`,
`shutdown`, and `command`. Keep protocol responses on stdout, diagnostics on
stderr, and use bounded JSON-RPC 2.0 lines. Add capability-specific handlers
only for the capabilities declared in the manifest.

Build the executable, then link it into a local Krabb installation for
development, or install a copy for regular use:

```sh
krabb plugin install hello --source ./hello --link
krabb plugin list
krabb doctor
krabb hello
```

Linking validates the manifest, checks the executable, runs a health check,
records the canonical source and checksum in `plugins.lock`, and hot-loads the
plugin when the daemon is running. A stopped daemon discovers it on its next
start. For distribution, publish the source or a checksummed archive and
install it with a pinned revision:

```sh
krabb plugin install hello https://github.com/example/hello.git \
  --revision COMMIT_SHA
```

Private repositories work when Git authentication is already configured on the
operator’s machine. Review permissions, secrets, and source revisions before
installing a custom plugin. The repository’s planned `krabb-plugins/template`
starter will provide a complete Rust example; until then, use this guide and
the existing plugins as protocol references.

## Install And Update Lifecycle

When `plugin install` is given one or more IDs without `--source`, Krabb looks
for each plugin in the local `krabb-plugins/<id>` directory or in a category
directory such as `krabb-plugins/<id>`. Run the command from the
repository root; each plugin must already be built in the workspace's
`target/debug` directory. Otherwise, pass an explicit local path or Git URL with
`--source` (explicit sources apply to one plugin ID per command).

By default, `krabb plugin install` copies the verified executable into the
Krabb home so it is independent of its source. Add `--link` to use the local
development workflow instead: Krabb canonicalizes the source, validates the
manifest, stages the executable, runs a health check, and links the built
executable into the Krabb home (copying it if symlinks are unavailable).
Rebuilding and running the command again picks up local changes. `krabb plugin update`
repeats the process for locked entries and keeps installed files in place until
each staged replacement passes validation.

After `install` commits the plugin and lock entry, the CLI asks a
running daemon to start it through authenticated local IPC. The plugin becomes
available without restarting the daemon or reinstalling the core. When no
daemon is running, it is discovered at the next daemon start. A selected model
plugin loads when installed and reports missing authentication when a request
needs it. Channel plugins load only when their required credentials are ready;
an activation error leaves the verified plugin installed and reports the reason.
`plugin uninstall` unloads an active process before deleting its files. `plugin
update` unloads active plugin processes before replacing their files, then
starts those plugins again through IPC. Inactive plugins remain inactive, and
the daemon itself does not restart. Active plugins are unavailable while
updates are fetched, validated, and activated. If the daemon is stopped,
updates are applied offline and the new versions load at its next start.

When a plugin changes its capabilities, permissions, secrets, or protocol
range, bump its version and update its README and release matrix. Keep explicit
Git revisions pinned for reproducible deployments. Never place credentials in
the manifest, source URL, archive name, or diagnostic output.

Provider and channel plugins prefer their dedicated environment variables and
fall back to secrets in the JSON file at `KRABB_CREDENTIALS`. Keep that file
outside version control with owner-only permissions.

The MCP plugin exposes bounded `stdio` and Streamable HTTP bridges. Stdio
requests require an explicit `approved: true` flag because they start an
operator-selected process. HTTP accepts only HTTPS or loopback endpoints,
with strict host-boundary checks, never invokes a shell, and forwards
normalized JSON-RPC requests without allowing one plugin to call another
directly.

Channel plugins split outgoing text at Telegram, Discord, and WhatsApp message
limits and retain normalized attachment metadata for providers. Telegram,
Discord, and WhatsApp media operations resolve opaque provider references into
bounded private files; generated daemon replies currently remain text-only.

The SQLite plugin uses WAL mode and idempotent schema migrations. Its store
methods cover key/value data, owner-scoped leases, an idempotent durable
outbox, acknowledgements, and expiring event deduplication.
When installed, the TUI exposes `/store get`, `/store put`, and `/store delete`
for session-scoped text values.
Host outbox entries expose stable IDs and explicit uncertainty; plugins receive
the ID with each send so operators can correlate a provider receipt.

The memory plugin persists scoped records with `guided` and `autonomous`
learning modes. Guided learning is the default and saves only when the user
explicitly asks; autonomous learning may retain stable, useful facts while
excluding secrets and sensitive inferences. Both modes preserve audit history,
and records can be listed, searched, edited, recalled, or forgotten through
the registered `memory` command. Memory tools are exposed only when the plugin
is installed and channel tool policy permits them. The plugin lazily creates
`KRABB_HOME/data/memories/` on its first persisted setting or record;
`KRABB_MEMORY` can override the JSON index path. Markdown record files are linked from the index,
stored under `records/`. Global memories apply across conversations, while
provider and TUI memories stay within their conversation. The model receives
only a bounded summary of global and current-conversation memories.

The scheduler plugin persists pending tasks and active schedules atomically
under `KRABB_HOME/data/schedules/`. Explicit schedule requests become active
immediately; agent scheduling tool calls are authorized before activation.
The daemon executes active tasks as
isolated agent turns using their captured provider, model, and workspace, then
reports results to the originating conversation and records append-only JSONL
history under `KRABB_HOME/logs/schedules/<schedule-name>-<id>.jsonl`. Each
record includes a schema version, RFC 3339 timestamp, schedule/run identifiers,
status, attempt/retry details, scheduled/next-run times, captured
provider/model/workspace, and redacted result text bounded to 4,000 characters.
The outcome history has no fixed record-count limit and can be inspected
directly. Tasks support one-time delays and
exact RFC 3339 run times, plus five-field cron expressions in IANA timezones,
including daylight-saving transitions. Active tasks can be run immediately;
failed tasks can opt into up to five retries with exponential backoff or be
retried manually after pausing. Interrupted runs pause for review because an
action may already have happened. Missed recurring runs default to `skip`;
schedules can choose `coalesce` or bounded `replay`.

The Whisper plugin is an optional local adapter. It invokes an explicitly
configured executable against a root-confined path and returns text, keeping
native model dependencies outside the workspace core.

## Agent Interoperability

ACP starts a configured local agent, creates or resumes its protocol sessions,
and forwards text, progress, and tool status updates. Its filesystem callbacks
use Krabb's confined tools and approval policy. The current adapter does not
advertise terminal access, cannot answer generic ACP permission requests, and
does not implement cancellation. ACP agents run their own tools and model
workflow.

A2A is a client for discovering remote agents, sending messages, streaming
task updates, checking status, and requesting cancellation. It does not expose
Krabb agents as A2A services or implement OAuth. AG-UI accepts runs on a
loopback HTTP endpoint and emits run, text, frontend tool-call, state snapshot,
and error events. Frontend tools are returned to the client for execution; the
backend does not invoke client-supplied tools.

AG-UI's transport emits chunked SSE events, but the host model call returns a
complete reply before text and tool events are written. It does not provide
live token delivery, persist AG-UI thread state, mutate frontend state, or
propagate client disconnects as model cancellation. Human approval for
frontend tools remains the frontend's responsibility.

The retired coding-session adapter supplied a model-context handoff and an
authenticated local bridge for host read, write, patch, search, Git, shell, and
list tools. No current protocol recreates that combined workflow. ACP agents
use their own tools, while A2A and AG-UI have remote-collaboration and
user-interface roles. Workflows that depended on the old host-mediated coding
tools must be adapted to the chosen external agent and its supported protocol.

Secret declarations must use the `KRABB_` prefix. The host rejects manifests
that declare unprefixed configuration variables.
