# Providers

Providers are plugins selected by capability, not compiled into the core. A
provider must normalize its API responses into Crabbot protocol types and keep
vendor-specific fields inside the plugin process.

## Models

| Plugin | Service | Credential route |
| --- | --- | --- |
| `codex` | OpenAI-compatible chat completions or Codex app-server | `CRABBOT_CODEX_KEY` or Codex-managed sign-in; optional `CRABBOT_CODEX_BASE_URL` |
| `claude` | Anthropic Messages | `CRABBOT_CLAUDE_KEY` or credential JSON; optional `CRABBOT_CLAUDE_BASE_URL` |
| `gemini` | Gemini Developer API | `CRABBOT_GEMINI_KEY` or credential JSON |
| `ollama` | Ollama local or cloud API | `CRABBOT_OLLAMA_HOST`, `CRABBOT_OLLAMA_API_KEY`, or credential JSON |
| `openrouter` | OpenAI-compatible routed models | `CRABBOT_OPENROUTER_KEY`, `CRABBOT_OPENROUTER_MODEL`, or credential JSON |

Model plugins accept normalized messages and return a bounded `ModelReply`.
They validate provider status codes, response bodies, tool calls, and stream
events before returning them to the host. The Codex plugin uses an explicit
API key when configured; otherwise it delegates to the installed Codex CLI
app-server and its ChatGPT sign-in. Personal Codex sign-in requires Codex CLI
to be installed; other providers and API-key authentication do not require it.
The adapter validates app-server responses and fails closed if required
operations are incompatible. Run `crabbot codex login` or
`crabbot codex login --device` to authenticate.

Telegram images are inlined only after the host confines and bounds their
cached files. Codex-compatible chat completions use image URL content parts;
Anthropic uses base64 or HTTPS image blocks; Ollama receives base64 image data;
and Codex app-server receives inline data URLs. Model plugins advertise image
request support during their handshake, and the host rejects image turns when
the selected adapter does not advertise it. The selected model must still
support vision; Crabbot does not silently substitute another model. Historical
downloaded images expire from the cache after 24 hours, while images committed
to a session are retained in its protected media area.

## Channels

| Plugin | Transport | Required secret |
| --- | --- | --- |
| `telegram` | Bot API long polling | `CRABBOT_TELEGRAM_TOKEN` |
| `discord` | Gateway receive and REST send | `CRABBOT_DISCORD_TOKEN` |
| `whatsapp` | Cloud API webhook receive and Graph API send | `CRABBOT_WHATSAPP_TOKEN`, `CRABBOT_WHATSAPP_APP_SECRET`, `CRABBOT_WHATSAPP_VERIFY` |
| `signal` | Managed signal-cli receive and send | `CRABBOT_SIGNAL_ACCOUNT` |
| `slack` | Web API polling and Socket Mode credentials | `CRABBOT_SLACK_BOT_TOKEN`, `CRABBOT_SLACK_APP_TOKEN` |

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

Use `crabbot plugin list` and `crabbot doctor` to inspect installed providers.
The daemon reports a missing or incompatible capability instead of silently
substituting another provider. Select a model for a session with
`crabbot session model <id> <model>` or for a one-shot request with
`crabbot ask --plugin <id> --model <model> <prompt...>`.
