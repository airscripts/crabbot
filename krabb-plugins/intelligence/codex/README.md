# Codex

Install the released plugin with:

```sh
curl -fsSL https://raw.githubusercontent.com/krabblab/krabb/main/install.sh | sh -s -- codex
```

The plugin supports OpenAI-compatible chat completions and Server-Sent Events
for streamed replies. Set `KRABB_CODEX_KEY` to use the OpenAI API, or install
Codex CLI and run `krabb codex login` for personal ChatGPT sign-in.
Codex app-server owns browser or device-code login, credential storage, and
refresh. Krabb does not read or forward OAuth tokens. Set `KRABB_CODEX_HOME`
to select a Codex profile, or `KRABB_CODEX_BINARY` for a non-default
executable. Krabb checks the app-server calls it needs and fails closed if a
Codex update changes the protocol. No exact Codex CLI release is pinned;
compatibility is determined by the app-server operations Krabb uses. Set
`KRABB_KEYRING=1` to use the `codex` operating-system keyring entry.

After installation, `krabb codex` commands and foreground TUI model requests
can use the plugin immediately. Agent turns hosted by the background runtime
require that runtime to be running.

Discover the available operations with `krabb codex --help`. Use
`krabb codex login --help` for sign-in options, `krabb codex status` or
`krabb codex logout` to manage sign-in, and `krabb codex models` to list
account models. In the TUI, `/model help` shows model controls and
`/model list` lists Codex models; select one with `/model set <id>`.

The plugin advertises image-request support during its handshake. The HTTP
adapter sends inline image inputs using the OpenAI-compatible chat completions
format. The Codex adapter sends inline image inputs through the app-server
`turn/start` protocol and negotiates the experimental API needed for workspace
roots. Use a vision-capable selected model; Krabb does not pin the Codex CLI
release.
