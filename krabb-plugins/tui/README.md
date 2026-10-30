# TUI

The TUI is a terminal client for daemon-managed conversations, sessions, tools,
and approvals. Install it with:

```sh
curl -fsSL https://raw.githubusercontent.com/krabblab/krabb/main/install.sh | sh -s -- tui
```

## Table Of Contents

- [Start And Chat](#start-and-chat)
- [Commands](#commands)
- [Tools And Safety](#tools-and-safety)
- [Preferences And History](#preferences-and-history)

## Start And Chat

Start the daemon with `krabb service start` or run `krabb-daemon` in the
foreground. Then open the client:

```sh
krabb tui
krabb tui --once "prompt"
```

`--once` prints one reply without opening the full-screen UI. It uses and
deletes a temporary session unless you pass `--session <id>`. Interactive
sessions resume the last session or start `default`.

Press Enter to send, Ctrl+O to add a line, and Escape or Ctrl+C to quit. Use
Up/Down for input history, Page Up/Page Down to scroll, and Shift-drag to
select text.

## Commands

Use `/help` for commands available in the current session. Common commands:

- `/status`, `/approval`, `/approvals`, `/approve <id>`, `/deny <id>` manage
  daemon status and pending approvals.
- `/session`, `/new`, `/model`, and `/workspace` manage conversation context.
- `/plugin`, `/deliveries`, `/retry`, and `/drop` inspect plugin and delivery
  state.
- `/schedule`, `/memory`, and `/data` are available when their plugins are
  installed. Use `/data value` for simple session values; `/store` remains a
  compatibility alias.
- `/compact` summarizes older turns; `/clear` clears an idle session.

The TUI stores its command exchanges and completed model turns in the shared
session history. See [Commands](../../krabb-docs/commands.md) for CLI
operations and plugin guides for detailed schedule and memory behavior.

## Tools And Safety

TUI model tools are disabled by default. Enable them in `config.toml`:

```toml
[clients.tui]
tools = true
```

The global approval mode still controls mutating tools. To enable direct shell
commands entered with `!`, also set `shell = true`; restart the daemon after
changing that setting. A typed `!` command is an explicit user action, while
shell commands requested by the model remain subject to approval policy.
Workspace tools stay confined to the active session workspace.

## Preferences And History

Use `/history list`, `/history clear`, or `/history help` to manage input
history. `/compact` asks the selected model to summarize older turns; the TUI
also compacts automatically when saved conversation history reaches its limit.

Set `theme = false` under `[clients.tui]` to use the terminal's default colors.
Use `/statusline` to choose which details appear in the footer. Preferences are
saved under `KRABB_HOME/data/plugins/tui/` and apply to the next launch where
noted.
