# Configuration

Crabbot reads configuration from `config.toml` in `CRABBOT_HOME`. The home
directory defaults to the platform application-data directory; set
`CRABBOT_HOME` when the daemon should use a dedicated location.

Media-capable channels use `CRABBOT_MEDIA` for temporary downloaded content;
when it is unset, media is stored under `CRABBOT_HOME/media` and expired files
are removed during the next media request. Images accepted into a durable
session are copied under `media/pinned` and are not removed by cache cleanup.

## Example

```toml
update = "prompt"
approval = "off"
shell = false

[channels.telegram]
allow = ["-1001234567890"]
mention = "@crabbot"
owner = "42"
admin = ["7"]
member = ["9"]
topic = ["12"]
thread = ["topic-12"]
tools = false
worktree = true

[channels.discord]
allow = ["123456789012345678"]
mention = "@crabbot"
tools = true
worktree = true
```

Unknown update and approval modes fail validation before the daemon starts.
The default update mode is `prompt`, approval is `off`, shell execution is
disabled, and isolated Git worktrees are enabled for group turns.

Approved shell commands run directly on the host unless an optional container
sandbox is configured. Set `CRABBOT_SANDBOX_RUNTIME` to `docker` or `podman`
and `CRABBOT_SANDBOX_IMAGE` to an image already present in the local runtime.
Crabbot does not pull images. The sandbox disables networking, makes its root
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

`CRABBOT_HOME` contains configuration, the plugin registry, credentials
references, durable session state, and IPC markers. `CRABBOT_ROOT` is the
workspace exposed to file and search tools. Tool paths are canonicalized and
must remain below this root; leave it unset to keep filesystem tools
unavailable.

Provider and channel plugins read declared secrets from their own environment
variables or from the JSON file named by `CRABBOT_CREDENTIALS`. The file must
be private to the current user. `CRABBOT_KEYRING=1` opts into the operating
system keyring. Personal Codex
authentication is managed by the Codex CLI app-server. Crabbot never reads or
forwards Codex access tokens. `CRABBOT_CODEX_HOME` selects the Codex profile
directory and defaults to the standard `.codex` directory. Set
`CRABBOT_CODEX_BINARY` only when the Codex executable is not named `codex`.

All Crabbot configuration variables use the `CRABBOT_` prefix:

| Variable | Purpose |
| --- | --- |
| `CRABBOT_HOME` | Crabbot state and plugin directory |
| `CRABBOT_ROOT` | Workspace exposed to file and search tools |
| `CRABBOT_CREDENTIALS` | Protected JSON credential file |
| `CRABBOT_KEYRING` | Set to `1` to use the operating-system keyring |
| `CRABBOT_CHANNEL` | Default messaging plugin ID |
| `CRABBOT_MODEL_PLUGIN` | Default intelligence plugin ID |
| `CRABBOT_MODEL` | Default model identifier |
| `CRABBOT_MEMORY` | Memory plugin persistence path |
| `CRABBOT_TIMER` | Timer plugin persistence path |
| `CRABBOT_DB` | SQLite plugin database path |
| `CRABBOT_MEDIA` | Private media cache directory |
| `CRABBOT_WHISPER` | Default speech plugin ID |
| `CRABBOT_WHISPER_COMMAND` | Local Whisper-compatible executable |
| `CRABBOT_CODEX_KEY` | OpenAI-compatible API key |
| `CRABBOT_CODEX_BASE_URL` | Codex-compatible API base |
| `CRABBOT_CLAUDE_KEY` | Anthropic API key |
| `CRABBOT_CLAUDE_BASE_URL` | Claude-compatible API base |
| `CRABBOT_GEMINI_KEY` | Gemini Developer API key |
| `CRABBOT_GEMINI_BASE_URL` | Gemini-compatible API base |
| `CRABBOT_OLLAMA_HOST` | Ollama endpoint |
| `CRABBOT_OLLAMA_API_KEY` | Ollama cloud API key |
| `CRABBOT_TELEGRAM_TOKEN` | Telegram bot token |
| `CRABBOT_DISCORD_TOKEN` | Discord bot token |
| `CRABBOT_DISCORD_GATEWAY_URL` | Optional Discord Gateway URL |
| `CRABBOT_DISCORD_INTENTS` | Discord Gateway intent bitmask |
| `CRABBOT_OPENROUTER_KEY` | OpenRouter API key |
| `CRABBOT_OPENROUTER_MODEL` | Default OpenRouter model slug |
| `CRABBOT_OPENROUTER_BASE_URL` | OpenRouter-compatible API base |
| `CRABBOT_OPENROUTER_REFERER` | Optional OpenRouter attribution URL |
| `CRABBOT_OPENROUTER_TITLE` | Optional OpenRouter attribution title |
| `CRABBOT_WHATSAPP_TOKEN` | WhatsApp Cloud API token |
| `CRABBOT_WHATSAPP_APP_SECRET` | WhatsApp webhook signing secret |
| `CRABBOT_WHATSAPP_VERIFY` | WhatsApp webhook verification token |
| `CRABBOT_WHATSAPP_PHONE` | WhatsApp phone number ID |
| `CRABBOT_WHATSAPP_LISTEN` | Local WhatsApp webhook listener |
| `CRABBOT_WHATSAPP_GRAPH_URL` | Versioned Graph API base |
| `CRABBOT_SIGNAL_ACCOUNT` | Signal account managed by signal-cli |
| `CRABBOT_SIGNAL_COMMAND` | Optional signal-cli executable |
| `CRABBOT_SLACK_BOT_TOKEN` | Slack bot token |
| `CRABBOT_SLACK_APP_TOKEN` | Slack Socket Mode app token |
| `CRABBOT_SLACK_CHANNELS` | Comma-separated Slack channel IDs |
| `CRABBOT_PI_COMMAND` | Optional Pi agent executable |
| `CRABBOT_CODEX_HOME` | Codex configuration directory |
| `CRABBOT_CODEX_BINARY` | Optional Codex CLI executable |
| `CRABBOT_SANDBOX_RUNTIME` | Optional `docker`, `podman`, or `off` shell sandbox |
| `CRABBOT_SANDBOX_IMAGE` | Locally available image used by the shell sandbox |

The default model is `gpt-6-luna` when `CRABBOT_MODEL` is unset. When Gemini is
selected, the Gemini plugin maps that host default to its own `gemini-3.8-flash`
default.

Operating-system variables such as `HOME`, `USERPROFILE`, `PATH`, and
`XDG_CONFIG_HOME` retain their platform-defined names and are not Crabbot
configuration. Tool-native variables used to constrain subprocesses, such as
Git's `GIT_CONFIG_NOSYSTEM`, also retain their required names.

## Channel Policy

Direct messages are accepted by default, but tool schemas are not exposed
unless the sender also satisfies the channel policy and daemon approval mode.
Group messages require an `allow` entry. `owner`, `admin`, and `member` match
sender IDs; `topic` and `thread` match platform metadata; `mention` requires
the configured marker in message text. A message must satisfy every configured
filter. Set `tools = true` only for channels whose senders are trusted.

`worktree = true` creates an isolated Git worktree for a group session. Set it
to `false` only when the channel is explicitly allowed to operate in the
configured workspace. Private messages use the configured workspace directly.

## Update Policy

Plugin sources and hashes are stored in `plugins.lock`. The `update` setting
controls startup checks:

- `off` never checks remote sources.
- `check` reports changed sources.
- `prompt` reports changes and waits for `crabbot plugin update`.
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
crabbot doctor
crabbot plugin list
crabbot service status
```

`doctor` is read-only unless `--fix` is supplied. Use `crabbot doctor --fix` to
create a missing default configuration or plugins directory; existing config
and plugin files are not overwritten. If a provider is missing, install or
link its plugin and run `crabbot doctor` again. If a channel is silent, verify
its token, plugin status, `allow` list,
mention filter, and daemon logs. If tools are unavailable, check `CRABBOT_ROOT`,
`tools = true`, and that approval is set to `prompt` or `auto`.
When the daemon is stopped, session commands may use the locked offline store;
stale IPC marker files do not establish daemon ownership.
