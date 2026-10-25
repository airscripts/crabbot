# Memory

Install the released plugin with:

```sh
curl -fsSL https://raw.githubusercontent.com/krabblab/krabb/main/install.sh | sh -s -- memory
```

The plugin persists records to `KRABB_HOME/data/memories/index.json` by
default. The directory is created only when a setting or record is first saved.
Set `KRABB_MEMORY` to override the JSON index path. Each record is also
stored as a private Markdown file under `records/` beside the index, and the
index links to those records. Older flat key/value JSON files remain readable
as global records.

Learning defaults to `guided`: a save requires an explicit user request or an
operator-entered `memory remember` command. Set `krabb memory learning
autonomous` to let the agent retain stable, useful facts; it must not save
secrets or sensitive inferences. Return to the safer default with
`krabb memory learning guided`.

Use `krabb memory help` or `krabb memory --help` for command syntax. The
registered `krabb memory` command supports `status`, `search`, `list`,
`show`, `remember`, `edit`, `forget`, `audit`, `converge`, and `learning`. The
`list [page] [--type global|session] [--scope ID]` command displays up to five
records in a paginated tree; each row identifies its `type: global` or
`type: session`. `--type` filters the listed records. The
`converge` command reports likely contradictions within a scope and gives
precedence to the newer saved preference without deleting either record. It
uses text matching, not a model, and makes no changes. Each conflict includes a
`krabb memory forget` command for the older record if you choose to remove it.
Empty searches and lists report that no memories were found. Global records are
available in every conversation; channel and TUI records are scoped to their
conversation. Each model turn receives a bounded index of global and
conversation memories. Memory tools remain subject to the channel or TUI tool
policy.
