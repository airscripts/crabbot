# Crabfile Specification

`Crabfile` is the versioned, portable description used by `crabbot export`,
`crabbot validate`, and `crabbot import`. The current format is version `0.1`.
The version is required at the top of the document so future formats can be
validated without guessing which rules apply.

## Schema

```toml
version = "0.1"

[config]
update = "prompt"          # off, check, prompt, or auto
shell = false               # allow shell tools only when separately sandboxed
approval = "off"            # off, prompt, or auto

[config.channels.telegram]
allow = ["-1001234567890"]
mention = "@crabbot"
owner = "42"
admin = ["7"]
member = ["9"]
topic = ["12"]
thread = ["topic-12"]
worktree = true
tools = false

[[plugins]]
id = "telegram"
source = "crabbot-plugins/messaging/telegram"
revision = "local"
version = "0.1.0"
capabilities = ["channel"]
```

The root keys are:

| Key | Type | Meaning |
| --- | --- | --- |
| `version` | string | Crabfile format version. Version `0.1` is currently supported. |
| `config` | table | The configuration that will become `config.toml`. |
| `plugins` | array of tables | Plugin sources and locked metadata to import. |

Each `plugins` entry contains:

| Key | Type | Meaning |
| --- | --- | --- |
| `id` | string | Lowercase plugin identifier using letters, digits, and hyphens. |
| `source` | string | Local path, Git URL, or archive source. It must not be empty. |
| `revision` | string | Source revision; use `local` or an empty value for the local/default revision. |
| `version` | string | Expected plugin version from its manifest. |
| `capabilities` | array of strings | Expected normalized capability names from the manifest. |

The `config` table uses the same keys as `config.toml`: `update`, `shell`,
`approval`, and `channels`. Update and approval modes are documented in the
[configuration guide](configuration.md). Each channel policy supports `allow`,
`mention`, `owner`, `admin`, `member`, `topic`, `thread`, `worktree`, and
`tools`.

Unknown keys are invalid. Plugin IDs must be unique, and the first invalid
root, configuration, or plugin entry is reported so it can be corrected before
the next check.

## Validation And Import

Use the read-only validator before importing a file:

```sh
crabbot validate
crabbot validate --path ./Crabfile --json
```

The default path is `./Crabfile`. Validation checks TOML syntax, the schema,
the supported format version, configuration modes, plugin identifiers, empty
sources, and duplicate plugin IDs. A valid file prints its path and plugin
count; an invalid file reports the first error with a line and column when the
parser provides one.

Import performs the same validation before changing local state. It additionally
resolves each plugin source and verifies the imported plugin metadata against
the installed manifest. Import requires `--yes`; use `--force` when replacing
an existing local configuration is intentional.
