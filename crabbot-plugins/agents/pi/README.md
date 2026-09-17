# Pi

The Pi plugin provides `crabbot code` through Pi's headless RPC mode. Crabbot
owns the command, session identity, workspace, output, and process lifecycle;
Pi supplies the coding-agent harness. Install Pi separately and ensure its
executable is available as `pi`, or set `CRABBOT_PI_COMMAND`.

Use `crabbot code "inspect the repository and explain the next fix"` for a
one-shot session. Add `--session <id>` to keep and resume a persistent session.
