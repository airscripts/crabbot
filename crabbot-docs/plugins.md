# Plugins

Official plugins are small Rust binaries under `crabbot-plugins/`. Each folder
contains a manifest and a one-line installation example. Community plugins may
use another language if they implement the JSON-RPC 2.0 protocol.

## Manifest

```toml
id = "example"
version = "0.1.0"
capabilities = ["model"]
permissions = ["network"]
secrets = ["CRABBOT_CODEX_KEY"]

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

`plugins.lock` records the canonical source, revision marker, SHA-256 of the
manifest and executable, protocol, version, capabilities, permissions, and
secrets. An explicit Git `--revision` is pinned for later updates; unpinned Git
sources follow their configured default revision. Local and remote updates
health-check staged binaries before activation and refresh the hash only after
staged activation succeeds; lock persistence failure restores the previous
plugin bytes. Git
sources may use HTTPS, SSH, or local `file://` URLs. Archive
sources must use `.tar`, `.tar.gz`, `.tgz`, or `.zip` and include a SHA-256
fragment; remote archives require HTTPS. Crabbot rejects embedded URL
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

## Create A Custom Plugin

Custom plugins are supported today. A plugin source directory needs a manifest
and a built executable with this layout:

```text
hello/
├── crabbot-plugin.toml
└── bin/
    └── crabbot-plugin-hello
```

On Windows, use `crabbot-plugin-hello.exe`. The manifest ID, executable name,
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

Build the executable, then link it into a local Crabbot installation:

```sh
crabbot plugin link hello ./hello --yes
crabbot plugin list
crabbot doctor
crabbot hello
```

Linking validates the manifest, checks the executable, runs a health check,
records the canonical source and checksum in `plugins.lock`, and hot-loads the
plugin when the daemon is running. A stopped daemon discovers it on its next
start. For distribution, publish the source or a checksummed archive and
install it with a pinned revision:

```sh
crabbot plugin install hello https://github.com/example/hello.git \
  --revision COMMIT_SHA --yes
```

Private repositories work when Git authentication is already configured on the
operator’s machine. Review permissions, secrets, and source revisions before
installing a custom plugin. The repository’s planned `crabbot-plugins/template`
starter will provide a complete Rust example; until then, use this guide and
the existing plugins as protocol references.

## Install And Update Lifecycle

`crabbot plugin link` is the fastest local development loop. It canonicalizes
the source, validates the manifest, stages the executable, runs a health check,
and records the result in `plugins.lock`. `crabbot plugin install` follows the
same path for a Git or checksummed archive source. `crabbot plugin update`
repeats the process for locked entries and keeps installed files in place until
each staged replacement passes validation.

After `install` or `link` commits the plugin and lock entry, the CLI asks a
running daemon to start it through authenticated local IPC. The plugin becomes
available without restarting the daemon or reinstalling the core. When no
daemon is running, it is discovered at the next daemon start. A selected model
or channel plugin is loaded only when its required credentials are ready; an
activation error leaves the verified plugin installed and reports the reason.
`plugin remove` unloads an active process before deleting its files. `plugin
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
fall back to secrets in the JSON file at `CRABBOT_CREDENTIALS`. Keep that file
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
Host outbox entries expose stable IDs and explicit uncertainty; plugins receive
the ID with each send so operators can correlate a provider receipt.

The memory plugin persists scoped records with `guided` and `autonomous`
learning modes. Guided learning is the default and saves only when the user
explicitly asks; autonomous learning may retain stable, useful facts while
excluding secrets and sensitive inferences. Both modes preserve audit history,
and records can be listed, searched, edited, recalled, or forgotten through
the registered `memory` command. Memory tools are exposed only when the plugin
is installed and channel tool policy permits them. The plugin lazily creates
`CRABBOT_HOME/memory/` on its first persisted setting or record;
`CRABBOT_MEMORY` can override the JSON index path. Markdown record files are linked from the index,
scoped by provider and conversation, and only a bounded summary is added to a
model turn.

The timer plugin persists one-shot and repeating tasks atomically. It validates
five-field cron expressions and resolves IANA timezones through the system
timezone database on Unix and the platform bundle where a system database is
unavailable. Cron entries therefore calculate calendar and daylight-saving
transitions. Its `due` operation claims ready work exactly once for one-shot
tasks.

The Whisper plugin is an optional local adapter. It invokes an explicitly
configured executable against a root-confined path and returns text, keeping
native model dependencies outside the workspace core.

Secret declarations must use the `CRABBOT_` prefix. The host rejects manifests
that declare unprefixed configuration variables.
