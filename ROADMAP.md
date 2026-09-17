# Roadmap

## v0.2 | Reliability

Strengthen Crabbot’s engineering foundation while keeping the core small and
the public behavior stable.

- Resolve all known high and medium reliability findings.
- Improve recovery, cancellation, queue handling, persistence, and plugin updates.
- Preserve stable JSON-RPC 0.x compatibility.
- Keep workspace coverage above 80%.
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
