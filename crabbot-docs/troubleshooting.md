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
default configuration, plugins directory, workspace, or instruction files are
missing, use `crabbot doctor --fix` to create those safe defaults without
overwriting existing configuration or instructions. Invalid configuration and
plugin integrity failures require manual correction or plugin reinstall/update.
`crabbot init --force`
resets all Crabbot home data and requires confirmation; use it only when
intentionally erasing configuration, plugins, sessions, and workspace data.

## No Channel Replies

Check the channel token, plugin status, and configured `allow` list. For group
messages, verify the chat ID, mention filter, topic or thread filter, and the
sender role. A channel may poll successfully while policy intentionally drops
an unauthorized event.

## Tools Are Unavailable

The default tool root is `CRABBOT_HOME/workspace`; set `CRABBOT_ROOT` to use a
different existing workspace. Confirm that the channel has `tools = true`. The
daemon must also use `approval = "prompt"` for inline
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
