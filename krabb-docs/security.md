# Security

Krabb keeps policy in the host and treats external input as untrusted. It
collects no telemetry. This page summarizes the security model; see
[Safety](safety.md) and [Configuration](configuration.md) for operating details.

## Table Of Contents

- [Security Model](#security-model)
- [Operator Checklist](#operator-checklist)
- [Report A Vulnerability](#report-a-vulnerability)

## Security Model

- **Credentials:** Plugins receive declared secrets from environment variables,
  the protected `KRABB_CREDENTIALS` file, or the optional OS keyring. Codex
  authentication is delegated to Codex CLI; Krabb does not read its token files.
- **Plugins:** Installed plugins are trusted executable code. Review their
  source, revision, permissions, and declared secrets before installation.
  Remote archives require HTTPS and a SHA-256 digest.
- **Tools:** File access is confined to the active workspace. Shell is off by
  default; channel tools also require an authorized sender and enabled approval
  policy. A container sandbox can further limit approved shell commands.
- **Browser and desktop:** Browser network access requires an allowlist and
  controlled egress proxy. Linux computer control uses an isolated Xvfb
  display; macOS and Windows control the signed-in desktop only after explicit
  opt-in. See the [Browser](../krabb-plugins/browser/README.md) and
  [Computer](../krabb-plugins/computer/README.md) guides.
- **State and recovery:** Krabb uses crash-safe state writes. Turns that may
  have run a mutating tool are interrupted rather than replayed, and ambiguous
  channel sends require an explicit retry or drop decision.
- **Limits:** Plugin requests, stored values, searches, and commands have
  bounded sizes or runtimes. `krabb init --force --yes` deletes all data under
  `KRABB_HOME`; verify the path and stop the daemon before using it.

## Operator Checklist

- Keep Krabb state, credentials, and workspace files private to the daemon user.
- Leave shell off until its policy and isolation are configured.
- Allow group messages only from explicit channel IDs; enable tools only for
  trusted senders.
- Review interrupted turns, pending worktree cleanup, and uncertain deliveries.
- Run `krabb doctor` after configuration or plugin changes.

## Report A Vulnerability

Report vulnerabilities privately to
[francesco@airscript.it](mailto:francesco@airscript.it). Include the affected
version, platform, reproduction steps, impact, and a safe mitigation if known.
