# Memory

The memory plugin stores scoped records for later conversations. Install it
with:

```sh
curl -fsSL https://raw.githubusercontent.com/krabblab/krabb/main/install.sh | sh -s -- memory
```

## Table Of Contents

- [Storage](#storage)
- [Learning Modes](#learning-modes)
- [Commands](#commands)
- [Scopes And Privacy](#scopes-and-privacy)

## Storage

The plugin creates `KRABB_HOME/data/memories/index.json` when a record or
setting is first saved. Set `KRABB_MEMORY` to change the index path. Records
are stored as private Markdown files beside the index; older flat key/value
files remain readable as global records. Processes coordinate access through a
private lock file beside the index and reload it before every operation, so CLI
and daemon changes remain visible to one another. A busy lock is retried for up
to five seconds before the operation reports an error. Access, size-limit, and I/O
errors leave the index and active memories intact and fail the operation. Only
invalid file contents are quarantined for corruption recovery.
Default path selection also takes the writer locks and recovers interrupted saves
before checking either the current index or the legacy `KRABB_HOME/memory/index.json`.
An interrupted replacement therefore keeps using the existing store.

## Learning Modes

Learning defaults to `guided`. It saves only after an explicit user request or
an operator-entered `memory remember` command. Set
`krabb memory learning autonomous` to allow stable, useful facts to be saved.
Secrets and sensitive inferences must not be stored. Return to the default with
`krabb memory learning guided`.

## Commands

Run `krabb memory help` for syntax. Commands include `status`, `search`,
`list`, `show`, `remember`, `edit`, `forget`, `audit`, `converge`, and
`learning`.

`list [page]` shows up to five records. Filter with `--type global|session`
and `--scope <id>`. `converge` reports likely contradictions in a scope using
text matching; it changes nothing and favors the newer saved preference.

## Scopes And Privacy

Global records are available in every conversation. Channel and TUI records
are scoped to their conversation. Model turns receive a bounded index of global
and current-conversation records. Memory tools still follow the channel or TUI
tool policy.
