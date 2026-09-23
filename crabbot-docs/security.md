# Security

Crabbot is local-first and collects no telemetry. Provider credentials use
environment variables, the operating-system keyring when `CRABBOT_KEYRING=1`,
or a protected JSON file named by `CRABBOT_CREDENTIALS`. Codex sign-in and
token refresh are delegated to the Codex CLI app-server; Crabbot never reads
its token files or forwards OAuth tokens. Plugin IDs do not grant
home-directory access.
Never paste tokens into an issue, log,
`PLAN.md`, or plugin manifest.

## Threat Model

The daemon and installed plugins are trusted local processes. Channel senders,
model output, plugin archives, and workspace contents are untrusted inputs.
The host therefore treats every plugin as executable code, validates manifests
before launch, and does not treat a model request as an authorization grant.
The prototype does not defend against a local user who can replace the daemon,
its plugins, or the configured workspace.

Shell access is disabled until enabled and each command requires approval. File
operations use descriptor-relative no-follow access on Unix and canonical
boundary checks with reparse-point-safe handles on Windows. An
optional Docker or Podman sandbox runs approved shell commands without network
access, with a read-only container root, dropped Linux capabilities, bounded
resources, and only the active workspace mounted writable. The local
container engine remains part of the trusted computing base. File tools
canonicalize paths and reject traversal, symlink escapes, and symlinked
write targets; global `CRAB.md` and `CLAW.md` context files must be regular,
bounded files and never symbolic links. They guide the model but do not grant
tool capabilities or replace host authorization. Channel
allowlists can be narrowed by owner, admin, member, topic, and thread IDs. Tool
schemas are exposed only when daemon approvals are enabled and the sender is
trusted by the channel policy; private messages are not trusted implicitly.
With `approval = "prompt"`, mutating calls use signed, single-use callbacks
that expire after five minutes and are bound to their channel route, session,
tool, and arguments. `approval = "auto"` intentionally skips that interactive
confirmation and should be limited to explicitly trusted channel policies.
Group turns use an isolated Git worktree by default when the configured root is
a repository; this can be disabled per channel with an explicit
`worktree = false` setting.
Accepted turns are leased durably while model and tool work runs, so a process
restart restores only replay-safe user messages for retry. The host records an
unsafe lease phase before mutating tools; those turns are marked interrupted
without replay, including after a mutating-tool failure. Cancellation clears
the active lease without retrying it. A working or leased session cannot be
deleted, and deleting an idle session
attempts immediate worktree reclamation while reporting any pending cleanup for
the next daemon startup.
Remote plugin archives require HTTPS and a SHA-256 fragment; extraction rejects
absolute paths, parent traversal, symbolic links, and hard links. Archive
downloads are capped at 64 MiB during transfer, and remote Git clone, fetch,
and checkout staging is capped at 256 MiB. Declared archive entry counts and
expanded sizes are checked before extraction, searches have bounded workload,
cooperative cancellation, a single-worker cap, and a ten-second response
deadline, and approved workspace commands have bounded output and a two-minute
deadline. Model requests and global `CRAB.md`/`CLAW.md` context are bounded by
serialized bytes before dispatch. `init --force --yes` deletes all user state
under `CRABBOT_HOME`; stop the daemon and verify that path before confirming
the reset.
Plugin restarts terminate their process trees so canceled shell descendants do
not continue after the host has stopped a turn. Provider and channel response
bodies reserve protocol-frame headroom, plugin metadata and persisted values
are bounded before storage, and SQLite enforces its database cap using the
configured file's actual page size.

State, memory, timer, credential, plugin lock metadata, and gateway cursor files use a shared
crash-safe transaction writer. Unix files are mode 0600; Windows files use an
owner-only protected DACL. Startup recovery completes interrupted replacements
before a file is read. Channel sends are at-most-once: an ambiguous transport
failure is quarantined as uncertain and requires an explicit operator decision.
Daemon and offline state access share a cross-platform advisory file lock;
lock-file contents are diagnostic only and never determine ownership.

## Operator Checklist

- Review plugin IDs, sources, revisions, permissions, and declared secrets
  before installation.
- Keep `CRABBOT_HOME`, `CRABBOT_CREDENTIALS`, and workspace directories owned by
  the daemon user with restrictive permissions.
- Keep `approval = "off"` and `shell = false` until a channel policy is tested.
- Use explicit group `allow` entries and enable `tools` only for trusted rooms.
- Prefer isolated worktrees for group sessions and inspect changes before
  merging them.
- Run `crabbot doctor` after changing configuration or plugin versions.
- Treat interrupted turns and pending worktree cleanup as items for review,
  not as successful completion.
- Review uncertain channel deliveries with `crabbot delivery list`; retry only
  when a duplicate is acceptable, using `crabbot delivery retry <id> --yes`.

Report vulnerabilities privately to
[francesco@airscript.it](mailto:francesco@airscript.it), including the affected
version, platform, reproduction, impact, and a safe mitigation if known.
