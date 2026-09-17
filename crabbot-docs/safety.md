# Safety

Crabbot is designed for least privilege and bounded failure. Safe defaults keep
shell execution off, keep tools disabled for channels, and require explicit
workspace configuration.

## Trust Boundaries

The daemon trusts the local operator and treats model output, channel senders,
plugin archives, and workspace files as untrusted. Plugins run as separate
processes and cannot call one another directly. Review every plugin manifest
before installation.

## Tools

File operations stay below `CRABBOT_ROOT` after canonicalization. Symlinked
instructions, traversal, dangling write links, and worktree paths outside the
active workspace are rejected. Shell commands require both `shell = true` and
an approval decision, and output and execution time are bounded.

### Optional Container Sandbox

When shell tools are enabled, a Docker or Podman sandbox can keep approved
commands away from the host filesystem and network. Set
`CRABBOT_SANDBOX_RUNTIME` to `docker` or `podman` and
`CRABBOT_SANDBOX_IMAGE` to an image already available locally. Crabbot mounts
only the active workspace writable, uses a read-only container root, drops
Linux capabilities, disables networking, and applies bounded resource limits.
Runtime errors never fall back to host execution. This reduces the impact of
routine command mistakes; it is not a complete security boundary, so the
container engine, image, kernel, and host still need to be trusted and
maintained. See the [configuration](configuration.md) and
[security](security.md) guides for setup and threat-model details.

## Recovery

Accepted events, leases, transcripts, and outbox entries are durable. A crash
can retry only a lease proven safe; a mutating phase is never replayed. A
canceled turn is discarded. Ambiguous deliveries are not retried automatically;
an explicit delivery retry does not rerun model or tool work.

## Limits

Protocol frames, model context, session history, queues, archive extraction,
search work, plugin metadata, provider bodies, and persisted values are bounded.
When a limit is reached, Crabbot reports an error or dead-letters the item
instead of silently expanding resource use.
