# Safety

Krabb is designed for least privilege and bounded failure. Safe defaults keep
shell execution off, keep tools disabled for channels, and require explicit
workspace configuration.

## Table Of Contents

- [Trust Boundaries](#trust-boundaries)
- [Tools](#tools)
- [Recovery](#recovery)
- [Limits](#limits)

## Trust Boundaries

The daemon trusts the local operator and treats model output, channel senders,
plugin archives, and workspace files as untrusted. Plugins run as separate
processes and cannot call one another directly. Review every plugin manifest
before installation.

On Windows, the service runs as the dedicated `NT SERVICE\Krabb` virtual account,
with its token privileges limited to directory traversal. Private state grants full
access to its owning account, the Krabb service account, and LocalSystem. Service
installation applies this policy to existing configuration, credentials, and state.
For service-created files, the user's account is resolved from the enclosing
user-owned data directory, preserving client access to replaced state and IPC files.
Service state must not contain symbolic links.

## Tools

File operations stay below the active session workspace after canonicalization,
or below `KRABB_ROOT` when a session has not selected a workspace. Symlinked
instructions, traversal, dangling write links, and worktree paths outside the
active workspace are rejected. Shell tools in TUI turns require
`clients.tui.shell = true` and approval. Typed `!` commands need the setting and
count as explicit user actions. Output and execution time are bounded.

Confined Unix file writes preserve existing regular files’ ordinary permission
bits, including executable bits. New files use mode `0600`; replacement files
do not inherit set-user-ID, set-group-ID, or sticky bits.

### Container Sandbox

When shell tools are enabled, a Docker or Podman sandbox can keep approved
commands away from the host filesystem and network. Set
`KRABB_SANDBOX_RUNTIME` to `docker` or `podman` and
`KRABB_SANDBOX_IMAGE` to an image already available locally. Krabb mounts
only the active workspace writable, uses a read-only container root, drops
Linux capabilities, disables networking, and applies bounded resource limits.
The sandbox is required for shell execution on macOS and other Unix platforms
without Linux's process-group restriction. An uncontained host shell is rejected
before execution on those platforms.
The tools plugin creates each container before starting its command and records the
ID in the host's private tracking directory. The host force-removes tracked containers
after stopping the plugin, before completing restart or acknowledging cancellation.
Cleanup failures are reported and retain tracking records for retry.
Runtime errors never fall back to host execution. This reduces the impact of
routine command mistakes; it is not a complete security boundary, so the
container engine, image, kernel, and host still need to be trusted and
maintained. See the [configuration](configuration.md) and
[security](security.md) guides for setup and threat-model details.

Automatic room worktree creation disables Git hooks, filesystem monitors, recursive
submodule checkout, automatic maintenance, and lazy object fetching. Repositories with
checkout filters or partial-clone configuration require an explicitly approved shell
operation to prepare the worktree before it can be used automatically.

## Recovery

Accepted events, leases, transcripts, and outbox entries are durable. A crash
can retry only a lease proven safe; a mutating phase is never replayed. A
canceled turn is discarded. Ambiguous deliveries are not retried automatically;
an explicit delivery retry does not rerun model or tool work. Unix plugins run in
private process sessions; host cancellation and restart cleanup terminate shell
commands in those sessions even when they use separate process groups.
Linux host shells inherit a syscall filter that rejects `setsid` and `setpgid`,
so descendants cannot detach from cleanup. Other Unix platforms use the tracked
container sandbox for shell containment.
Windows tool commands start suspended and enter a kill-on-close Job Object before
execution. Completion, failure, cancellation, and plugin termination stop their
descendants, including background commands whose parent has already exited.

## Limits

Protocol frames, model context, session history, queues, archive extraction,
search work, plugin metadata, provider bodies, and persisted values are bounded.
When a limit is reached, Krabb reports an error or dead-letters the item
instead of silently expanding resource use.
