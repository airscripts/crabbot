# Memory

Install the released plugin with:

```sh
curl -fsSL https://raw.githubusercontent.com/airscripts/crabbot/main/install.sh | sh -s -- memory
```

The plugin persists records to `CRABBOT_HOME/memory/index.json` by default.
The directory is created only when a setting or record is first saved. Set
`CRABBOT_MEMORY` to override the JSON index path. Each record is also stored as
a private Markdown file beside the index, and the index links to those records.
Older flat key/value JSON files remain readable as global records.

Learning defaults to `guided`: a save requires an explicit user request or an
operator-entered `memory remember` command. Set `crabbot memory learning
autonomous` to let the agent retain stable, useful facts; it must not save
secrets or sensitive inferences. Return to the safer default with
`crabbot memory learning guided`.

Use the registered `crabbot memory` command for `status`, `search`, `list`,
`show`, `remember`, `edit`, `forget`, and `audit`. Records written by a channel
conversation are scoped to that provider and conversation; the model receives
only the bounded index for the current scope. Tool access remains subject to
the channel's existing tool policy.
