# Krabb

[![Main](https://github.com/krabblab/krabb/actions/workflows/main.yml/badge.svg)](https://github.com/krabblab/krabb/actions/workflows/main.yml)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-2024-orange.svg)](https://www.rust-lang.org/)

Krabb is a self-hostable, open-source agent operating system for building,
running, and orchestrating native and external AI agents.

Krabb keeps its core provider-neutral and adds capabilities through isolated
plugins. Native agents use the Krabb runtime. External agents connect through
ACP, agent collaboration uses A2A, and user interfaces connect through AG-UI.

> Krabb is pre-release software. Commands and configuration may change
> incompatibly before version 1.0.

## Get Started

Install the CLI, daemon, and the plugins you plan to use. The
[first-run guide](krabb-docs/workflows.md) covers installation, provider setup,
and starting the daemon.

```sh
krabb init
krabb plugin list
krabb doctor
krabb status
krabb-daemon
```

Configure at least one intelligence provider and one messaging channel before
starting the daemon. The daemon stays in the foreground until you stop it with
Ctrl-C; use the [service guide](krabb-docs/workflows.md#background-service) to
run it in the background.

## Capabilities

| Area | Examples |
| --- | --- |
| Intelligence | Codex, Anthropic, OpenAI, Gemini, Ollama, OpenRouter |
| Messaging | Telegram, Discord, WhatsApp, Signal, Slack |
| Agent protocols | ACP, A2A, AG-UI |
| Other plugins | Tools, memory, scheduling, SQLite, MCP, speech, TUI |

All plugins are optional. Intelligence plugins live in
`krabb-plugins/intelligence/`; messaging plugins live in
`krabb-plugins/messaging/`. Plugin manifests declare capabilities and
permissions.

## Documentation

Start at the [documentation home](krabb-docs/home.md). It links to the
[first-run workflows](krabb-docs/workflows.md),
[configuration](krabb-docs/configuration.md),
[provider setup](krabb-docs/providers.md),
[commands](krabb-docs/commands.md),
[architecture](krabb-docs/architecture.md),
[plugin development](krabb-docs/plugins.md), and
[security guidance](krabb-docs/security.md).

## Development

Use Rust 1.89 or newer and Cargo. Run the complete local checks before opening
a pull request:

```sh
lefthook install
make verify
```

See [contributing](CONTRIBUTING.md) for repository conventions. Tests use local
fixtures and do not call paid provider APIs.

## Support And License

Run `krabb doctor` for local diagnostics. Read [support](SUPPORT.md) before
reporting an issue and [security](SECURITY.md) before reporting a vulnerability.
Krabb is licensed under [Apache-2.0](LICENSE).
