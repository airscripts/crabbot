# Crabbot

[![Main](https://github.com/airscripts/crabbot/actions/workflows/main.yml/badge.svg)](https://github.com/airscripts/crabbot/actions/workflows/main.yml)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-2024-orange.svg)](https://www.rust-lang.org/)

Crabbot is a lightweight, open-source general-purpose agent that turns models,
tools, memory, and messaging into one assistant you control. It runs on your
machine, connects to the capabilities you choose, and stays understandable
and extendable through isolated plugins while keeping the host small. You can
shape it around the way you work instead of adopting a fixed workflow or
locking your data into a hosted service.

## Table Of Contents

- [For Agents](#for-agents)
- [Installation](#installation)
- [Usage](#usage)
- [Plugins](#plugins)
- [Documentation](#documentation)
- [Repository Layout](#repository-layout)
- [Development](#development)
- [Contributing](#contributing)
- [Support](#support)
- [Security](#security)
- [License](#license)

## For Agents

> Install Crabbot from https://github.com/airscripts/crabbot with the official installer for your operating system, then run `crabbot init` and `crabbot doctor`.

When working on the repository, read `AGENTS.md`, the nearest scoped guidance,
and the relevant local documentation before making changes. Treat the
repository’s implementation and documentation as the source of truth.

## Installation

### Prebuilt

Install the core on your operating system:

```sh
curl -fsSL https://raw.githubusercontent.com/airscripts/crabbot/main/install.sh | sh
```

On Unix-like systems, use `install.sh`:

```sh
curl -fsSL https://raw.githubusercontent.com/airscripts/crabbot/main/install.sh | sh -s -- codex
curl -fsSL https://raw.githubusercontent.com/airscripts/crabbot/main/install.sh | sh -s -- telegram
```

On Windows, use `install.ps1` from PowerShell:

```powershell
.\install.ps1
.\install.ps1 telegram
```

The core archive ships the `crabbot` CLI and `crabbot-daemon` binaries, plus
license files; it contains no plugin binaries. Each official plugin is a
separate, optional archive. The installer verifies its checksum, installs it
under the configured Crabbot home
(`CRABBOT_HOME/plugins/<id>` when `CRABBOT_HOME` is set), and registers it in
`plugins.lock`. Archives are
verified against the release `SHA256SUMS` file before installation.

For a local archive smoke test, set `CRABBOT_RELEASE_BASE` to a `file://`
directory containing the selected archive and `SHA256SUMS`.

Installing or linking a plugin while the daemon is running loads it immediately
over authenticated local IPC, provided a selected intelligence or messaging
plugin has its required credentials. No daemon or core reinstall is needed. If
the daemon is stopped, the plugin is loaded the next time it starts. Installing
the core does not install or start a background service; see [Usage](#usage) to
run or install the daemon service.

### From Source

Building locally requires Rust 1.89 or newer, Cargo, Git, and a working C
compiler for native dependencies. Clone the repository and install only the CLI
and daemon; these commands do not build or install plugin executables:

```sh
git clone https://github.com/airscripts/crabbot.git
cd crabbot
cargo install --path crabbot --locked
cargo install --path crabbot-daemon --locked
```

Ensure `~/.cargo/bin` is on `PATH`, then verify the installation:

```sh
crabbot --version
crabbot help
```

`make install` is an equivalent repository-local shortcut. Re-run both
`cargo install --path ... --locked --force` commands after changing
Rust source.

### Local Plugins

Build only the plugin you want to use. The default link source is the matching
directory under `crabbot-plugins/`, so run these commands from the repository
root or pass an absolute source path:

```sh
cargo build -p crabbot-plugin-telegram --locked
crabbot init
crabbot plugin link telegram --yes
crabbot doctor
```

Use `crabbot plugin list` to inspect installed capabilities. A local link
records the canonical source and executable in `plugins.lock`; rebuild and run
`crabbot plugin link telegram --yes` again to validate and hot-load your
changes. `crabbot plugin update` also applies verified updates to the running
daemon: it unloads and reloads only plugins that were active, without
restarting the daemon. Review every manifest's permissions and declared secrets
before linking community plugins.

## Usage

### Quick Start

For a private Telegram assistant with the Codex API, install the core,
`codex`, and `telegram` plugins using the platform instructions above. Then
set the credentials, initialize Crabbot, and start the daemon:

```sh
export CRABBOT_CODEX_KEY=...
export CRABBOT_TELEGRAM_TOKEN=...
crabbot init
crabbot doctor
crabbot-daemon
```

On Windows PowerShell, use `$env:CRABBOT_CODEX_KEY = "..."` and
`$env:CRABBOT_TELEGRAM_TOKEN = "..."` instead. Direct messages work with the
default configuration; group chats require an explicit channel allowlist.

### Commands

```sh
crabbot help
crabbot init
crabbot doctor
crabbot status
crabbot plugin list
crabbot code "Inspect the repository and explain the next fix."
crabbot-daemon
```

Keep `crabbot-daemon` in the foreground while configuring the first channel. In
another terminal, use `crabbot status`, `crabbot session list`,
`crabbot session show <id>`, and `crabbot doctor` to inspect local state. Stop
the foreground process with `Ctrl-C`; use the service commands below for a
background daemon.

Configure provider credentials through a protected environment or a 0600 JSON
file referenced by `CRABBOT_CREDENTIALS`. To use the operating-system keyring,
set `CRABBOT_KEYRING=1`:

```sh
export CRABBOT_CODEX_KEY=...
crabbot plugin link codex --yes
```

For personal Codex authentication, [install Codex CLI](https://developers.codex.com/codex/cli/)
and run `crabbot codex login` (or `crabbot codex login --device` on a headless host).
The Codex app-server owns token storage and refresh. Teams and unattended
services should use an OpenAI API key. Codex CLI is only required for personal
Codex sign-in; it is not required for API-key authentication or other providers.

Set `CRABBOT_HOME` to keep state in a dedicated directory. File tools remain
inside `CRABBOT_ROOT`; shell tools are disabled by default. Group turns use
isolated Git worktrees when possible. Enable channel tools only with
`tools = true` and daemon approvals enabled. Group chats require an explicit
allowlist; mention, owner, admin, member, topic, and thread filters are
available for Telegram and Discord.

Use `crabbot service install` followed by `crabbot service start` to activate
the native service. `crabbot service stop` and `crabbot service remove` reverse
those actions. See the [configuration guide](crabbot-docs/configuration.md)
for the full `config.toml` reference and recovery behavior.

## Plugins

Every plugin is optional and distributed independently from the core. Install
only the capabilities you need; plugin binaries never ship inside the core
archive.

| Plugin | Capability | Notes |
| --- | --- | --- |
| `codex` | Intelligence | Chat completions, streamed replies, or Codex-managed sign-in |
| `claude` | Intelligence | Messages API and streamed replies |
| `gemini` | Intelligence | Gemini Developer API with vision and tools |
| `ollama` | Intelligence | Local or Cloud API with streamed replies |
| `openrouter` | Intelligence | Routed OpenAI-compatible models |
| `telegram` | Messaging | Long polling |
| `discord` | Messaging | Gateway receive, media, and REST send |
| `whatsapp` | Messaging | Official Cloud API |
| `signal` | Messaging | Managed signal-cli process |
| `slack` | Messaging | Slack Web API and Socket Mode credentials |
| `sqlite` | Store | Local durable state |
| `memory` | Memory | In-process memory seam |
| `timer` | Timer | In-process reminders seam |
| `tools` | Tool | Confined files, patches, Git worktrees, and approved shell |
| `mcp` | MCP | Protocol seam for tools and resources |
| `whisper` | Speech | Protocol seam for local transcription |
| `pi` | Agent | Pi coding harness managed by Crabbot |
| `tui` | Client | Registers the optional `crabbot tui` command |

## Documentation

The [documentation home](crabbot-docs/home.md) is the index for the complete
guides. Start with the [workflows](crabbot-docs/workflows.md), then read
[configuration](crabbot-docs/configuration.md) and
[commands](crabbot-docs/commands.md). Use the [architecture](crabbot-docs/architecture.md),
[plugins](crabbot-docs/plugins.md), and [security](crabbot-docs/security.md)
guides when extending Crabbot or enabling tools.

## Repository Layout

```text
crabbot-core/                 provider-neutral types, policy, turn loop, and protocol
crabbot/                      thin CLI executable
crabbot-daemon/               thin standalone daemon executable
crabbot-runtime/              daemon lifecycle and host orchestration
crabbot-libs/file/            shared filesystem and persistence primitives
crabbot-plugins/              intelligence, messaging, agents, and capabilities
crabbot-docs/                 architecture, commands, configuration, and operations
crabbot-scripts/              packaging, release, metrics, checks, and review automation
```

## Development

Local development requires Rust 1.89 or newer, Cargo, Git, a working C
compiler for native dependencies, and
[Lefthook](https://github.com/evilmartians/lefthook). Set up a checkout with:

```sh
git clone https://github.com/airscripts/crabbot.git
cd crabbot
rustup toolchain install 1.89.0
lefthook install
cargo build --workspace --locked
```

Run the complete local gates:

```sh
make verify
```

The workflow checks formatting, Clippy, locked compilation, tests, coverage,
builds, and metrics. Workspace line coverage must remain at or above 80%:

```sh
make coverage
```

See [AGENTS.md](AGENTS.md) for repository boundaries and conventions,
[ROADMAP.md](ROADMAP.md) for planned work, and the
[product guide](crabbot-docs/product.md) for Crabbot’s current capabilities and
boundaries.

Run the independent review loop:

```sh
./crabbot-scripts/revloop.sh
```

Use `CRABBOT_REVLOOP_OUTPUT=verbose` when the full orchestrator and worker
stream is useful during diagnosis. Each orchestrator pass performs a deep
review and records every distinct material finding it identifies. Blocking
findings still control worker cycles and the bounded
`CRABBOT_REVLOOP_MAX_CYCLES` convergence limit; non-blocking findings remain
visible without forcing additional cycles.

## Contributing

Read [CONTRIBUTING.md](CONTRIBUTING.md) before opening a pull request. Keep
changes focused, preserve the provider-neutral core, add deterministic tests
for behavior changes, update the relevant guide, and run `make verify`.
Provider tests must use local fixtures and must not call paid APIs.

## Support

Run `crabbot help` for the current command tree and `crabbot doctor` for local
diagnostics. For a reproducible issue, include the Crabbot version, operating
system, command, expected behavior, actual behavior, and redacted diagnostic
output. Never include tokens, credential files, or private message content.

## Security

Crabbot collects no telemetry. It keeps sender identity and room scope in
normalized events, requires approval for risky actions, confines filesystem
paths, and redacts secrets in logs. Durable history and media retention remain
behind the same protocol boundary.

Read [SECURITY.md](SECURITY.md) before reporting a vulnerability.

## License

Crabbot is licensed under [Apache-2.0](LICENSE). See the
[license policy](crabbot-docs/license.md) for the rationale and contribution
terms.
