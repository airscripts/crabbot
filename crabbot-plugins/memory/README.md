# Memory

Install the released plugin with:

```sh
curl -fsSL https://raw.githubusercontent.com/airscripts/crabbot/main/install.sh | sh -s -- memory
```

The plugin persists records to `CRABBOT_HOME/data/memories/index.json` by
default. The directory is created only when a setting or record is first saved.
Set `CRABBOT_MEMORY` to override the JSON index path. Each record is also
stored as a private Markdown file under `records/` beside the index, and the
index links to those records. Older flat key/value JSON files remain readable
as global records.

Learning defaults to `guided`: a save requires an explicit user request or an
operator-entered `memory remember` command. Set `crabbot memory learning
autonomous` to let the agent retain stable, useful facts; it must not save
secrets or sensitive inferences. Return to the safer default with
`crabbot memory learning guided`.

Use `crab memory help` or `crab memory --help` for command syntax. The
registered `crab memory` command supports `status`, `search`, `list`,
`show`, `remember`, `edit`, `forget`, `audit`, `converge`, and `learning`. The
`converge` command reports likely contradictions within a scope and gives
precedence to the newer saved preference without deleting either record. It
uses text matching, not a model, and makes no changes. Each conflict includes a
`crab memory forget` command for the older record if you choose to remove it.
Empty searches and lists report that no memories were found. Global records are
available in every conversation; channel and TUI records are scoped to their
conversation. Each model turn receives a bounded index of global and
conversation memories. Memory tools remain subject to the channel or TUI tool
policy.
