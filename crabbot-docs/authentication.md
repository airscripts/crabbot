# Authentication

Crabbot has two separate authentication decisions: whether a plugin can reach
its provider and whether a channel sender can use a capability.

## Provider Credentials

Plugins prefer their declared environment variables. The host can also pass a
secret from the JSON file named by `CRABBOT_CREDENTIALS`:

```json
{
  "CRABBOT_CODEX_KEY": "...",
  "CRABBOT_TELEGRAM_TOKEN": "..."
}
```

Keep the file outside version control with owner-only permissions. Set
`CRABBOT_KEYRING=1` to use the operating-system keyring. Credential values are
never written to manifests, logs, session transcripts, or plugin lock entries.

## Codex Compatibility

For personal ChatGPT sign-in, the Codex plugin invokes the installed Codex
CLI's app-server and delegates authentication and token refresh to it. Install
a compatible Codex CLI using the [official Codex CLI setup guide](https://developers.codex.com/codex/cli/),
then run:

```sh
crabbot codex login
```

For a headless host, use `crabbot codex login --device`. Check or clear the
Codex-owned sign-in with `crabbot codex status` or `crabbot codex logout`.
The default Codex home is the user's standard `.codex` directory; set
`CRABBOT_CODEX_HOME` to use another profile. Crabbot does not read, persist, or
pass Codex OAuth tokens to the Codex HTTP client.

Crabbot does not pin a Codex CLI release. It checks that the CLI starts and
validates each required app-server response. If a Codex update changes the
protocol or a required operation, Crabbot fails closed with an error. Codex
CLI is required only for personal Codex sign-in; OpenAI API-key authentication
and other providers do not require it. Use an OpenAI API key for unattended
shared deployments.

## Channel Trust

Direct messages are accepted by default, but tools still require a trusted
policy and daemon approval. Group chats must appear in the channel `allow`
list. Optional owner, admin, member, topic, thread, and mention filters narrow
that allowlist. Configure `tools = true` only for senders who may access the
workspace.

## Service Credentials

`crabbot service install` materializes declared provider variables into a
protected service credential file and references it from the native service
definition. Inspect the generated definition with `crabbot service status` and
remove it with `crabbot service remove` when rotating credentials.
