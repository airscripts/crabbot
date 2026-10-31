# Tools

Install the released plugin with:

```sh
curl -fsSL https://raw.githubusercontent.com/krabblab/krabb/main/install.sh | sh -s -- tools
```

The plugin confines reads, writes, searches, and declared patch targets to
`KRABB_ROOT`, or to the active isolated workspace when the host supplies one.
Unix operations use descriptor-relative no-follow access; Windows operations
hold parent directory handles that prohibit directory replacement and use
reparse-point-safe file handles. Confined file operations fail closed on
platforms other than Unix and Windows. Writes reject
symbolic-link targets, and writes, patches, shell commands, and
worktree mutations need explicit approval.
Git inspection allows status, diff, worktree listing, and worktree add/remove
operations within that active workspace. All Git operations and Git-backed
patches require shell access and the configured tool approval policy because
Git can execute repository-configured filters, even during `diff`. They are
unavailable when shell access is disabled; approval authorizes that execution. When a container sandbox is configured,
use the sandboxed shell tool for Git operations and patches; the direct Git
and patch tools reject execution on the host.
Git runs in the active workspace, but repository-configured programs have
shell permissions and may access paths outside that workspace.

The read-only `fetch` tool retrieves bounded text from public HTTPS URLs. It
blocks local and private network addresses, pins requests to validated DNS
addresses, rechecks redirects, caps redirects at three and response bodies at
2 MiB, and returns at most 48 KiB of text. Fetch requests have a 15-second
deadline and a four-request concurrency limit. The plugin declares its network
permission in the manifest.

On Linux and Windows, approved shell commands run on the host when no container sandbox is
configured. Linux shells reject process-group and session changes so descendants cannot
escape cancellation. On macOS and other Unix hosts, shell execution requires the container
sandbox; host execution fails before running the command.
To configure the sandbox, set `KRABB_SANDBOX_RUNTIME` to `docker` or
`podman` and `KRABB_SANDBOX_IMAGE` to an image already available locally.
The tools plugin never pulls images. The container has no network, a read-only
root filesystem, dropped Linux capabilities, bounded CPU, memory, process and
temporary-file resources, and a writable mount limited to the active workspace.
The selected image must contain `sh` and allow its configured user to write the
workspace. Runtime errors fail the command; they never fall back to host shell
execution. Set `[clients.tui] shell = true` to enable TUI shell access. Shell
tools requested by an agent also require an enabled approval policy.

Use a local Docker or Podman engine. Remote container contexts are unsupported
because the active workspace mount must refer to this host's filesystem. The
sandbox narrows command access but does not replace kernel, daemon, or host
security updates.
