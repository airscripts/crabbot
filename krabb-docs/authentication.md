# Authentication

Krabb has two separate authentication decisions: whether a plugin can reach
its provider and whether a channel sender can use a capability.

## Provider Credentials

Plugins prefer their declared environment variables. The host can also pass a
secret from the JSON file named by `KRABB_CREDENTIALS`:

```json
{
  "KRABB_CODEX_KEY": "...",
  "KRABB_TELEGRAM_TOKEN": "..."
}
```

Keep the file outside version control with owner-only permissions. Set
`KRABB_KEYRING=1` to use the operating-system keyring. Credential values are
never written to manifests, logs, session transcripts, or plugin lock entries.

## Codex Compatibility

For personal ChatGPT sign-in, the Codex plugin invokes the installed Codex
CLI's app-server and delegates authentication and token refresh to it. Install
a compatible Codex CLI using the [official Codex CLI setup guide](https://developers.codex.com/codex/cli/),
then run:

```sh
krabb codex --help
krabb codex login
```

For a headless host, use `krabb codex login --device`. Check or clear the
Codex-owned sign-in with `krabb codex status` or `krabb codex logout`.
Use `krabb codex login --help` for login options and `krabb codex models` to
see model IDs available to the signed-in account.
The default Codex home is the user's standard `.codex` directory; set
`KRABB_CODEX_HOME` to use another profile. Krabb does not read, persist, or
pass Codex OAuth tokens to the Codex HTTP client.

Krabb does not pin a Codex CLI release. It checks that the CLI starts and
validates each required app-server response. If a Codex update changes the
protocol or a required operation, Krabb fails closed with an error. Codex
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

`krabb service install` materializes declared provider variables into a
protected service credential file and references it from the native service
definition. Inspect the generated definition with `krabb service status` and
uninstall it with `krabb service uninstall -y` when rotating credentials.
