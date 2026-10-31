# Configuration

Krabb reads configuration from `config.toml` in `KRABB_HOME`. The home
directory defaults to the platform application-data directory; set
`KRABB_HOME` when the daemon should use a dedicated location.
Plugin-owned persistent data is kept separately under
`KRABB_HOME/data/plugins/<plugin-id>/`; agent workspaces remain the place for
agent instructions, prompts, and other working files.
Scheduled tasks are stored under `KRABB_HOME/data/schedules/`, and their
execution history is stored under `KRABB_HOME/logs/schedules/`.

Read or change settings without opening the TOML file with `krabb config get`
and `krabb config set KEY=VALUE`. For example, enable TUI workspace tools with
`krabb config set clients.tui=true` (the long form is
`krabb config set clients.tui.tools=true`). TOML values use normal TOML syntax,
such as `krabb config set 'approval="prompt"'` or
`krabb config set 'channels.telegram.allow=["123"]'`. The command
validates the complete config before saving and preserves existing comments.
The TUI tools switch is applied immediately to a running daemon for new turns;
the TUI theme setting takes effect the next time `krabb tui` starts. TUI shell
access takes effect after the daemon restarts. Other settings are saved and
take effect the next time the daemon starts. Add
`--force` to restart a running daemon and apply restart-required settings.

Media-capable channels use `KRABB_MEDIA` for temporary downloaded content;
when it is unset, media is stored under `KRABB_HOME/media` and expired files
are removed during the next media request. Images accepted into a durable
session are copied under `media/pinned` and are not removed by cache cleanup.

## Table Of Contents

- [Example](#example)
- [Paths](#paths)
- [Channel Policy](#channel-policy)
- [Update Policy](#update-policy)
- [State And Recovery](#state-and-recovery)
- [Troubleshooting](#troubleshooting)

## Example

```toml
name = "Krabb"
update = "prompt"
approval = "off"

[channels.telegram]
allow = ["-1001234567890"]
mention = "@krabb"
owner = "42"
admin = ["7"]
member = ["9"]
topic = ["12"]
thread = ["topic-12"]
tools = false
shell = false
worktree = true

[channels.discord]
allow = ["123456789012345678"]
mention = "@krabb"
tools = true
shell = false
worktree = true

[clients.tui]
tools = true
shell = false
theme = true

[scheduler]
timezone = "Europe/Rome"
missed = "skip"
```

The scheduler timezone is the default for cron expressions; ask for a timezone
when it is not configured. `missed` controls what happens to tasks that became
due while the daemon was offline: `skip` (the default) skips overdue
occurrences, and `coalesce` runs once for the missed period. A task may opt
into bounded `replay` when it is created. Explicitly requested schedules become
active immediately after any configured approval step. A task interrupted
while it is running pauses for review because its action may already have
happened.

Set `[clients.tui] theme = false` to disable TUI colors and use the terminal's
default foreground color. This can improve contrast in terminals with unusual
foreground/background palettes. The theme is enabled by default; change it with
`krabb config set clients.tui.theme=false` and relaunch `krabb tui`.

Unknown update and approval modes fail validation before the daemon starts.
`name` sets the globally displayed name for Krabb (1–64 visible characters).
The default is `Krabb`.
The default update mode is `prompt`, approval is `off`, shell execution is
disabled, and isolated Git worktrees are enabled for group turns.

On Linux and Windows, approved shell commands run directly on the host unless a container
sandbox is configured. macOS and other Unix hosts require the sandbox for shell containment.
Set `KRABB_SANDBOX_RUNTIME` to `docker` or `podman`
and `KRABB_SANDBOX_IMAGE` to an image already present in the local runtime.
Krabb does not pull images. The sandbox disables networking, makes its root
filesystem read-only, drops Linux capabilities, applies process, CPU, memory,
and temporary-storage limits, and mounts only the active workspace as
writable. The image must contain `sh` and its configured user must be able to
write to the workspace. Runtime failures do not fall back to host execution.
Use a local runtime context; remote contexts cannot safely mount the active
host workspace. This boundary supplements, but does not replace, host and
container-engine security.

Approval modes are:

- `off`: Do not expose tools to channel turns.
- `prompt`: Require a signed, single-use inline approval for each mutating
  tool call. Approval buttons expire after five minutes and are bound to the
  channel, chat, thread, session, tool, and exact arguments.
- `auto`: Permit mutating tool calls without an interactive confirmation.

Use `prompt` for interactive Telegram and Discord channels. Use `auto` only
when the configured channel policy is trusted to run workspace mutations
without a person confirming each request.

## Paths

Krabb keeps operational state, working files, and installed plugin files in
separate locations under `KRABB_HOME`:

| Location | Purpose |
| --- | --- |
| `data/` | Durable runtime state, including session records, client and delivery state, and plugin-owned data or settings. This is storage, not telemetry collection. |
| `logs/` | Daily application diagnostics, including plugin stderr, in sortable UTC-dated `YYYY-MM-DD.log` or `.jsonl` files. |
| `logs/debug/` | Raw per-command log captures created by `--verbose` and `--debug`. |
| `logs/schedules/` | Append-only per-schedule JSONL outcome logs named `<schedule-name>-<id>.jsonl`. |
| `workspace/` | The default agent working area for project files and context instructions. `KRAB.md` and `KLAW.md` are loaded into turns; file tools are confined to the active workspace. |
| `plugins/<plugin-id>/` | Installed plugin files and their `krabb-plugin.toml` manifests. A manifest declares capabilities; plugin code supplies them through the plugin protocol. |

`KRABB_HOME` contains configuration, the plugin registry, credential
references, durable session state, IPC markers, and a `workspace/` directory
for your Krabb's global personal data and instructions. When the optional
memory plugin is installed, it lazily creates `memory/` on its first saved
record or learning-mode setting; no memory directory is created when the
plugin is absent or unused.
`init` creates
`workspace/KRAB.md` and `workspace/KLAW.md`; Krabb injects their current
contents into each model turn alongside the persisted, bounded conversation.
Changes to either file apply on the next turn. These files guide model behavior
but cannot grant capabilities or override host policy.

Together, they share an 8 KiB context budget; excess content is truncated, so
put the most important guidance first and keep both files concise.

TUI-created session IDs use an internal `tui-` namespace. Names shown in the
TUI omit that prefix, and channel sessions remain separately routed, so a TUI
session named `work` does not share history with a channel conversation.

`KRABB_ROOT` is the default workspace exposed to file and search tools. If
unset, it defaults to `KRABB_HOME/workspace`; setting it selects a different
default. A session's explicit workspace replaces that default for the session.
Tool paths are canonicalized and confined to the active session workspace, or
to `KRABB_ROOT` when no session workspace is selected. A root by itself does
not enable tools: channel `tools = true` and daemon approvals are still
required, and shell remains separately disabled by default.

The TUI is a local daemon client. Its `tools` and `shell` settings default to
`false`; enable them under `[clients.tui]` for TUI turns. Global approval still
controls mutating tools. Move legacy top-level `shell` settings here.

Provider and channel plugins read declared secrets from their own environment
variables or from the JSON file named by `KRABB_CREDENTIALS`. The file must
be private to the current user. `KRABB_KEYRING=1` opts into the operating
system keyring. Personal Codex
authentication is managed by the Codex CLI app-server. Krabb never reads or
forwards Codex access tokens. `KRABB_CODEX_HOME` selects the Codex profile
directory and defaults to the standard `.codex` directory. Set
`KRABB_CODEX_BINARY` to select a non-default executable. When installing the
background service, Krabb records the absolute path of `codex` found on the
installing shell's `PATH`, so user-local installs remain available to the
service. Reinstall with `krabb service install --force` after changing the
executable or its path.

All Krabb configuration variables use the `KRABB_` prefix:

| Variable | Purpose |
| --- | --- |
| `KRABB_HOME` | Krabb state, plugins, and global agent workspace |
| `KRABB_ROOT` | Override the default `KRABB_HOME/workspace` tool root |
| `KRABB_CREDENTIALS` | Protected JSON credential file |
| `KRABB_KEYRING` | Set to `1` to use the operating-system keyring |
| `KRABB_CHANNEL` | Default messaging plugin ID |
| `KRABB_MODEL_PLUGIN` | Default intelligence plugin ID |
| `KRABB_MODEL` | Default model identifier |
| `KRABB_MEMORY` | Override the optional memory plugin's JSON index path; Markdown records live under a sibling `records/` directory |
| `KRABB_TIMER` | Legacy timer state path, imported into the scheduler on upgrade |
| `KRABB_DB` | SQLite plugin database path |
| `KRABB_MEDIA` | Private media cache directory |
| `KRABB_WHISPER` | Default speech plugin ID |
| `KRABB_WHISPER_COMMAND` | Local Whisper-compatible executable |
| `KRABB_PLAYWRIGHT_MODULE` | Explicit Playwright package entry point for the browser plugin |
| `KRABB_PLAYWRIGHT_BROWSERS_PATH` | Optional Chromium cache directory when it differs from Playwright's default |
| `KRABB_NODE` | Optional Node.js executable path for the browser worker |
| `KRABB_BROWSER_ALLOWED_HOSTS` | Comma-separated exact host allowlist for browser requests |
| `KRABB_BROWSER_EGRESS_PROXY` | Required controlled HTTP proxy for browser network access |
| `KRABB_COMPUTER_MODE` | `isolated` for Linux Xvfb, or `interactive` for native macOS and Windows desktop control |
| `KRABB_COMPUTER_ALLOW_HOST_CONTROL` | Must be `1` to enable interactive macOS or Windows desktop control |
| `KRABB_COMPUTER_DISPLAY` | Dedicated Xvfb display number, above `:0`, for Linux computer control |
| `KRABB_XVFB` | Optional path to the Xvfb executable; defaults to `Xvfb` on `PATH` |
| `KRABB_COMPUTER_STARTUP` | Optional absolute path to one fixed application started in the Linux isolated display |
| `KRABB_COMPUTER_STARTUP_ARGS` | Optional JSON array of fixed arguments for that Linux application |
| `KRABB_CODEX_KEY` | Optional Codex compatible API key |
| `KRABB_CODEX_BASE_URL` | Optional Codex compatible API base |
| `KRABB_OPENAI_KEY` | OpenAI API key |
| `KRABB_OPENAI_MODEL` | Default OpenAI model |
| `KRABB_OPENAI_BASE_URL` | OpenAI-compatible API base |
| `KRABB_ANTHROPIC_KEY` | Anthropic API key |
| `KRABB_ANTHROPIC_BASE_URL` | Anthropic-compatible API base |
| `KRABB_GEMINI_KEY` | Gemini Developer API key |
| `KRABB_GEMINI_BASE_URL` | Gemini-compatible API base |
| `KRABB_OLLAMA_HOST` | Ollama endpoint |
| `KRABB_OLLAMA_API_KEY` | Ollama cloud API key |
| `KRABB_TELEGRAM_TOKEN` | Telegram bot token |
| `KRABB_DISCORD_TOKEN` | Discord bot token |
| `KRABB_DISCORD_GATEWAY_URL` | Optional Discord Gateway URL |
| `KRABB_DISCORD_INTENTS` | Discord Gateway intent bitmask |
| `KRABB_OPENROUTER_KEY` | OpenRouter API key |
| `KRABB_OPENROUTER_MODEL` | Default OpenRouter model slug |
| `KRABB_OPENROUTER_BASE_URL` | OpenRouter-compatible API base |
| `KRABB_OPENROUTER_REFERER` | Optional OpenRouter attribution URL |
| `KRABB_OPENROUTER_TITLE` | Optional OpenRouter attribution title |
| `KRABB_WHATSAPP_TOKEN` | WhatsApp Cloud API token |
| `KRABB_WHATSAPP_APP_SECRET` | WhatsApp webhook signing secret |
| `KRABB_WHATSAPP_VERIFY` | WhatsApp webhook verification token |
| `KRABB_WHATSAPP_PHONE` | WhatsApp phone number ID |
| `KRABB_WHATSAPP_LISTEN` | Local WhatsApp webhook listener |
| `KRABB_WHATSAPP_GRAPH_URL` | Versioned Graph API base |
| `KRABB_SIGNAL_ACCOUNT` | Signal account managed by signal-cli |
| `KRABB_SIGNAL_COMMAND` | Optional signal-cli executable |
| `KRABB_SLACK_BOT_TOKEN` | Slack bot token |
| `KRABB_SLACK_APP_TOKEN` | Slack Socket Mode app token |
| `KRABB_SLACK_CHANNELS` | Comma-separated Slack channel IDs |
| `KRABB_CODEX_HOME` | Codex configuration directory |
| `KRABB_CODEX_BINARY` | Optional Codex CLI executable |
| `KRABB_ACP_COMMAND` | External ACP agent executable |
| `KRABB_ACP_ARGS` | JSON array of ACP executable arguments |
| `KRABB_A2A_AGENT_CARD` | Remote A2A agent card URL |
| `KRABB_A2A_TOKEN` | Optional A2A bearer token |
| `KRABB_AG_UI_LISTEN` | Loopback AG-UI address; defaults to `127.0.0.1:8787` |
| `KRABB_SANDBOX_RUNTIME` | `docker`, `podman`, or `off`; shell sandbox required on macOS and other non-Linux Unix hosts |
| `KRABB_SANDBOX_IMAGE` | Locally available image used by the shell sandbox |

The default host model is unset when `KRABB_MODEL` is unset; select a model
explicitly in the TUI or configure `KRABB_MODEL`. When Gemini is selected,
the Gemini plugin maps the unset host model to its own `gemini-3.8-flash` default.

Operating-system variables such as `HOME`, `USERPROFILE`, `PATH`, and
`XDG_CONFIG_HOME` retain their platform-defined names and are not Krabb
configuration. Tool-native variables used to constrain subprocesses, such as
Git's `GIT_CONFIG_NOSYSTEM`, also retain their required names.

## Channel Policy

Direct messages are accepted by default, but tool schemas are not exposed
unless the sender also satisfies the channel policy and daemon approval mode.
Group messages require an `allow` entry. `owner`, `admin`, and `member` match
sender IDs; `topic` and `thread` match platform metadata; `mention` requires
the configured marker in message text. A message must satisfy every configured
policy filter. Set `tools = true` only for channels whose senders are trusted.
Set `shell = true` as well to allow authorized turns to run shell commands on
the daemon's machine; it defaults to `false` and has no effect unless tools are
enabled and the sender is authorized. Use `approval = "prompt"` or
`approval = "auto"` to enable channel tools. Scheduled tasks capture these
effective permissions when created and use them for their later runs.

`worktree = true` creates an isolated Git worktree for a group session. Set it
to `false` only when the channel is explicitly allowed to operate in the
configured workspace. Private messages use the configured workspace directly.

## Update Policy

Plugin sources and hashes are stored in `plugins.lock`. The `update` setting
controls startup checks:

- `off` never checks remote sources.
- `check` reports changed sources.
- `prompt` reports changes and waits for `krabb plugin update`.
- `auto` stages, health-checks, and activates updates automatically.

Explicit Git revisions remain pinned. Archive sources require HTTPS and a
`#sha256=` fragment. Updates are staged before activation, and a failed health
check or lock write keeps the previous plugin active.

## State And Recovery

Session transcripts, queued turns, leases, delivery outboxes, and deduplication
records are persisted through the configured store. In-flight turns are
recovered only when their lease is replay-safe; turns that may have run a
mutating tool are marked interrupted. Cancellation clears the lease without
requeuing the turn. Delivery sends are marked uncertain after ambiguous
transport failures and require an explicit operator retry or drop. Session
deletion is rejected while work is active and reports worktree cleanup failures
for retry at the next daemon start.

## Troubleshooting

Run these commands from the same user account as the daemon:

```sh
krabb doctor
krabb plugin list
krabb service status
```

`doctor` is read-only unless `--fix` is supplied. Use `krabb doctor --fix` to
create missing safe defaults, including the workspace and instruction files;
existing config and instruction files are not overwritten. If a provider is
missing, install or link its plugin and run `krabb doctor` again. If a channel
is silent, verify its token, plugin status, `allow` list, mention filter, and
daemon logs. If tools are unavailable, check `KRABB_ROOT`,
`tools = true`, and that approval is set to `prompt` or `auto`.
When the daemon is stopped, session commands may use the locked offline store;
stale IPC marker files do not establish daemon ownership.
