# Troubleshooting

## Check The Installation

```sh
crabbot --version
crabbot plugin list
crabbot doctor
```

Confirm that the daemon and CLI use the same `CRABBOT_HOME`. If a plugin is
missing, build or install it, then run `crabbot doctor` again.

`crabbot doctor` does not change local state by default. If the home directory,
default configuration, or plugins directory is missing, use
`crabbot doctor --fix` to create those safe defaults without overwriting an
existing configuration. Invalid configuration and plugin integrity failures
require manual correction or plugin reinstall/update. Use `crabbot init
--force` only when intentionally recreating the default configuration.

## No Channel Replies

Check the channel token, plugin status, and configured `allow` list. For group
messages, verify the chat ID, mention filter, topic or thread filter, and the
sender role. A channel may poll successfully while policy intentionally drops
an unauthorized event.

## Tools Are Unavailable

Set `CRABBOT_ROOT` to an existing workspace and confirm that the channel has
`tools = true`. The daemon must also use `approval = "prompt"` for inline
confirmation or `approval = "auto"` for unattended approval. Shell execution
requires the separate `shell = true` setting and the configured approval mode.

## Provider Errors

Verify the plugin's declared credential name and inspect the redacted daemon
diagnostic. Do not paste provider responses or authorization headers into an
issue. Use a local fixture when reproducing a provider parsing problem.

## Stale State

Run `crabbot service status` and stop a duplicate daemon before using offline
session commands. Stale IPC marker files do not prove ownership; the advisory
lock is authoritative. Interrupted sessions and pending worktree cleanup are
reported by `crabbot doctor` and retried during the next daemon start.

## Report A Bug

Include the version, operating system, command, expected result, actual result,
and redacted logs. Never include tokens, credential files, private message
content, or a full workspace transcript.
