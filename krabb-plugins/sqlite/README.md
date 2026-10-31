# SQLite

Install the released plugin with:

```sh
curl -fsSL https://raw.githubusercontent.com/krabblab/krabb/main/install.sh | sh -s -- sqlite
```

Set `KRABB_DB` to choose the database file. Otherwise, it is stored as
`krabb.db` under `KRABB_HOME`. The plugin uses WAL mode, idempotent migrations,
private database files, and a 32 MiB database limit.

## Agent Data

When tools are enabled and SQLite is installed, the agent gets a `data` tool
for creating and exploring structured data. It can create named databases and
tables, declare typed columns, insert records, query by exact field values,
add optional columns or remove columns, update a record by ID, and delete records
or tables. Database and table names use letters, numbers, and underscores.
Column types are `text`, `integer`, `real`, `boolean`, and `json`. A database
can contain up to 100 tables; each table can contain 10,000 records and 64
columns. Queries return at most 20 records and 256 KiB of JSON. Records are limited
to 255 KiB, reserving room for page metadata within the host's tool-result and
model-message limits. Older records that exceed a query page produce an explicit
error; reduce them with `data update` before querying again.

Databases are logical namespaces inside the configured SQLite file, not
separate SQLite files. Data is scoped to the active conversation identity so
one channel conversation cannot read another conversation's data. Mutations
use Krabb's normal tool approval policy. The API accepts structured arguments
only; it does not expose raw SQL.

Agent tool access is disabled by default. Enable it for the clients or channels
that should use saved data:

```toml
[clients.tui]
tools = true

[channels.telegram]
tools = true
```

## Commands

The `krabb data` CLI command operates in a separate `cli` scope by default. Add
`--scope NAME` to inspect or manage a conversation scope, such as
`krabb data --scope tui:default database list`. In the TUI, `/data` operates on
the active TUI conversation's data.

```sh
krabb data database list
krabb data database create contacts
krabb data table create contacts people
krabb data column add contacts people name text --required
krabb data column add contacts people age integer
krabb data insert contacts people '{"name":"Ada","age":36}'
krabb data query contacts people '{"age":36}'
krabb data update contacts people 1 '{"age":37}'
krabb data delete contacts people 1
krabb data table describe contacts people
krabb data column add contacts people active boolean
krabb data column remove contacts people age
krabb data table drop contacts people
krabb data database drop contacts
```

Run `krabb data help` or `/data help` for the full command syntax. TUI commands
share the agent's `tui:<session>` data scope; the CLI scope is intentionally
separate by default. CLI output is human-readable by default; pass `--json` for
structured JSON output. Create an empty table and add columns individually, or
pass JSON schemas for bulk creation and compatibility.

## Key/Value And Reliability Methods

The plugin also keeps the existing key/value methods (`put`, `get`, `delete`),
owner-scoped leases (`lease`, `release`), a durable queue (`enqueue`, `outbox`,
`retry`, `ack`), and expiring event deduplication (`seen`). The daemon's
messaging delivery retry queue is separate and remains in runtime state.
