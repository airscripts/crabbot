# Tools

Install the released plugin with:

```sh
curl -fsSL https://raw.githubusercontent.com/airscripts/crabbot/main/install.sh | sh -s -- tools
```

The plugin confines reads, writes, searches, patches, and Git operations to
`CRABBOT_ROOT`, or to the active isolated workspace when the host supplies one.
Unix operations use descriptor-relative no-follow access; Windows operations
use canonical boundary checks and reparse-point-safe file handles. Writes reject
symbolic-link targets, and writes, patches, shell commands, and
worktree mutations need explicit approval.
Git inspection allows status, diff, worktree listing, and approved worktree
add/remove operations within that active workspace.

The read-only `fetch` tool retrieves bounded text from public HTTPS URLs. It
blocks local and private network addresses, pins requests to validated DNS
addresses, rechecks redirects, caps redirects at three and response bodies at
2 MiB, and returns at most 48 KiB of text. Fetch requests have a 15-second
deadline and a four-request concurrency limit. The plugin declares its network
permission in the manifest.

Approved shell commands run on the host only when no container sandbox is
configured. To isolate them, set `CRABBOT_SANDBOX_RUNTIME` to `docker` or
`podman` and `CRABBOT_SANDBOX_IMAGE` to an image already available locally.
The tools plugin never pulls images. The container has no network, a read-only
root filesystem, dropped Linux capabilities, bounded CPU, memory, process and
temporary-file resources, and a writable mount limited to the active workspace.
The selected image must contain `sh` and allow its configured user to write the
workspace. Runtime errors fail the command; they never fall back to host shell
execution. Keep `shell = true` in the main configuration and retain an enabled
channel approval mode before shell tools can run.

Use a local Docker or Podman engine. Remote container contexts are unsupported
because the active workspace mount must refer to this host's filesystem. The
sandbox narrows command access but does not replace kernel, daemon, or host
security updates.
