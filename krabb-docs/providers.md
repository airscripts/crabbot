# Providers

Providers are plugins selected by capability, not compiled into the core. A
provider must normalize its API responses into Krabb protocol types and keep
vendor-specific fields inside the plugin process.

## Table Of Contents

- [Models](#models)
- [Channels](#channels)
- [Platform Setup](#platform-setup)
- [Selection](#selection)
- [Other Capabilities](#other-capabilities)
- [Agent Interoperability](#agent-interoperability)

## Models

| Plugin | Service | Credential route |
| --- | --- | --- |
| `codex` | OpenAI-compatible chat completions or Codex app-server | `KRABB_CODEX_KEY` or Codex-managed sign-in; optional `KRABB_CODEX_BASE_URL` |
| `openai` | OpenAI API | `KRABB_OPENAI_KEY`; optional `KRABB_OPENAI_MODEL` and `KRABB_OPENAI_BASE_URL` |
| `anthropic` | Anthropic Messages | `KRABB_ANTHROPIC_KEY` or credential JSON; optional `KRABB_ANTHROPIC_BASE_URL` |
| `gemini` | Gemini Developer API | `KRABB_GEMINI_KEY` or credential JSON |
| `ollama` | Ollama local or cloud API | `KRABB_OLLAMA_HOST`, `KRABB_OLLAMA_API_KEY`, or credential JSON |
| `openrouter` | OpenAI-compatible routed models | `KRABB_OPENROUTER_KEY`, `KRABB_OPENROUTER_MODEL`, or credential JSON |

The OpenAI API plugin is independent from Codex and uses OpenAI API credentials.
Codex preserves the Codex app-server integration and optional compatible API
key route. Model plugins accept normalized messages and return a bounded `ModelReply`.
They validate provider status codes, response bodies, tool calls, and stream
events before returning them to the host. The Codex plugin uses an explicit
API key when configured; otherwise it delegates to the installed Codex CLI
app-server and its ChatGPT sign-in. Personal Codex sign-in requires Codex CLI
to be installed; other providers and API-key authentication do not require it.
The adapter validates app-server responses and fails closed if required
operations are incompatible. Run `krabb codex login` or
`krabb codex login --device` to authenticate.

OpenAI and OpenRouter send the full tool catalog supplied by the host. The
runtime permits up to 16 tool invocations per turn, counted once each across
model steps. OpenAI, OpenRouter, Anthropic, Ollama, and the Codex API-key adapter
coalesce text by size or a short timer and send at most 256 text notifications
per request. The final reply retains all generated text within
the response size limit, including text received after notifications stop.
OpenRouter uses `KRABB_OPENROUTER_MODEL` for new daemon sessions when `KRABB_MODEL`
is absent, as well as for empty or `default` model requests. Explicit models take precedence.

Telegram images are inlined only after the host confines and bounds their
cached files. Codex-compatible chat completions use image URL content parts;
Anthropic uses base64 or HTTPS image blocks; Ollama receives base64 image data;
and Codex app-server receives inline data URLs. Model plugins advertise image
request support during their handshake, and the host rejects image turns when
the selected adapter does not advertise it. The selected model must still
support vision; Krabb does not silently substitute another model. Historical
downloaded images expire from the cache after 24 hours, while images committed
to a session are retained in its protected media area.

Codex sign-in forwards normalized screenshot results as app-server image items,
with the complete response bounded by the protocol frame limit. Gemini preserves
inline screenshot pixels alongside function responses and legacy tool history.

## Channels

| Plugin | Transport | Required secret |
| --- | --- | --- |
| `telegram` | Bot API long polling | `KRABB_TELEGRAM_TOKEN` |
| `discord` | Gateway receive and REST send | `KRABB_DISCORD_TOKEN` |
| `whatsapp` | Cloud API webhook receive and Graph API send | `KRABB_WHATSAPP_TOKEN`, `KRABB_WHATSAPP_APP_SECRET`, `KRABB_WHATSAPP_VERIFY` |
| `signal` | Managed signal-cli receive and send | `KRABB_SIGNAL_ACCOUNT` |
| `slack` | Web API polling and Socket Mode credentials | `KRABB_SLACK_BOT_TOKEN`, `KRABB_SLACK_APP_TOKEN` |

Channel plugins own polling cursors, provider limits, message chunking and
edits, and attachment normalization. They never decide whether a sender is
authorized; that decision belongs to the daemon policy.

## Platform Setup

Create the provider account or application before installing its plugin:

- Telegram: create a bot with BotFather and set its bot token.
- Discord: create a bot application, enable the message content intent, invite
  it to the server, and set its token.
- WhatsApp: create a Meta Cloud API application, configure the phone number,
  app secret, verification token, and a public HTTPS webhook proxy.
- Signal: install and register `signal-cli`, then set the registered account.
- Slack: create a Slack app with Socket Mode, issue bot and app tokens, and
  set the channel IDs the bot may poll.

Use the environment names in the channel table or the protected credentials
file. Keep group channels out of the allowlist until sender and workspace
policy have been reviewed.

## Selection

Use `krabb plugin list` and `krabb doctor` to inspect installed providers.

## Other Capabilities

| Capability | Plugins | What it provides |
| --- | --- | --- |
| `tool` | Tools, Browser, Computer | Filesystem and Git operations, approved shell access, browser automation, and desktop control. |
| `mcp`, `resource` | MCP | Access to tools and resources exposed by MCP servers. |
| `memory` | Memory | Scoped agent memory records with search and management commands. |
| `scheduler` | Scheduler | Persistent scheduled agent tasks and controlled execution. |
| `store` | SQLite | Agent-accessible structured data, key/value values, leases, durable queue records, and event deduplication. |
| `speech` | Whisper | Voice transcription through a local Whisper-compatible executable. |
| `client` | TUI | A terminal interface connected to the daemon. |

Model plugins also advertise the `vision` capability when they accept image
inputs. The host checks that capability before dispatching an image turn.

## Agent Interoperability

The `acp` plugin starts a configured local executable that implements ACP. The
`a2a` plugin discovers and calls a remote A2A agent. The `ag-ui` plugin serves
a loopback run endpoint with run, text, frontend tool-call, state snapshot, and
error events. These protocols connect compatible agents; they do not imply
compatibility with unrelated agent-specific interfaces. See each plugin README
for supported features and limits. ACP and A2A coalesce updates after the first
255 notifications, reserving one final notification for the remaining text.
ACP filesystem calls are routed through Krabb's tools plugin with the selected
workspace confined to `KRABB_ROOT` and writes subject to the approval mode.
The daemon reports a missing or incompatible capability instead of silently
substituting another provider. Select a model for a session with
`krabb session model <id> <model>`. When a model plugin is installed, use
`krabb ask --provider <id> --model <model> <prompt...>` for a one-shot request.
