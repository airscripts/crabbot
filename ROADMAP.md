# Roadmap

## v0.2 | Reliability

Strengthen Crabbot’s engineering foundation while keeping the core small and
the public behavior stable.

- Resolve all known high and medium reliability findings.
- Improve recovery, cancellation, queue handling, persistence, and plugin updates.
- Preserve stable JSON-RPC 0.x compatibility.
- Keep core coverage above 80% and runtime host coverage above 50%.
- Require clean dependency vulnerability audits.
- Keep formatting, Clippy, tests, rustdoc, security, and Agentskill checks passing.

## v0.3 | Experience

Make installation, first-run setup, daily CLI use, and project understanding
clearer for adopters and contributors.

- Improve `init`, `doctor`, `status`, configuration, and plugin installation.
- Improve errors, recovery guidance, and shell completion.
- Clarify architecture, plugin development, configuration, security, and troubleshooting documentation.
- Add deterministic onboarding and command-output tests.
- Preserve protocol and plugin compatibility.

## v0.4 | Ecosystem

Make it easy to create, review, discover, and securely install community
plugins without adding a hosted runtime to Crabbot.

- Add the `crabbot-plugins/template` Hello Crabworld Rust starter.
- Document the plugin authoring and catalog submission workflow.
- Create a separate Astro site deployed on Vercel.
- Add a curated public plugin catalog with reviewed pull requests.
- Include pinned sources, project details, capabilities, permissions, secrets, targets, maintainers, and security status.
- Generate copyable install commands for immutable GitHub revisions.
- Keep private plugins directly installable but outside the public catalog.

## v0.5 | Deployments

Make Crabbot straightforward to operate in containerized environments without
coupling users to one container engine or bundling optional plugins into the
core image.

- Publish reproducible OCI-compatible deployment images for the CLI and daemon.
- Keep Docker, Podman, and other OCI runtimes supported through the same image
  and documented entrypoints.
- Document environment and file-based secret injection, persistent volumes for
  `CRABBOT_HOME` and `CRABBOT_ROOT`, health checks, graceful shutdown, and
  restart behavior.
- Keep plugins independently installable and hot-loadable, with explicit IPC,
  network, and volume boundaries in the container deployment guide.
- Avoid privileged defaults and add CI smoke coverage for an OCI runtime,
  image metadata, persistence, and daemon lifecycle.
