# Checklist

Use this checklist to track whether each Crabbot crate has received a refinement
pass and has been tested. Automated test runs and hands-on product checks are
tracked separately so one is not mistaken for the other.

## Workspace Crates

| Crate | Refined | Automated Tests | Hands-on Check |
| --- | --- | --- | --- |
| `crabbot-core` | [x] | [x] | [ ] |
| `crabbot-file` | [ ] | [x] | [ ] |
| `crabbot-runtime` | [x] | [x] | [ ] |
| `crabbot` | [ ] | [x] | [x] |
| `crabbot-daemon` | [ ] | [x] | [x] |
| `crabbot-plugin-claude` | [ ] | [x] | [ ] |
| `crabbot-plugin-codex` | [ ] | [x] | [x] |
| `crabbot-plugin-gemini` | [ ] | [x] | [ ] |
| `crabbot-plugin-ollama` | [ ] | [x] | [ ] |
| `crabbot-plugin-openrouter` | [ ] | [x] | [ ] |
| `crabbot-plugin-pi` | [ ] | [x] | [ ] |
| `crabbot-plugin-mcp` | [ ] | [x] | [ ] |
| `crabbot-plugin-memory` | [ ] | [x] | [ ] |
| `crabbot-plugin-discord` | [ ] | [x] | [ ] |
| `crabbot-plugin-signal` | [ ] | [x] | [ ] |
| `crabbot-plugin-slack` | [ ] | [x] | [ ] |
| `crabbot-plugin-telegram` | [ ] | [x] | [ ] |
| `crabbot-plugin-whatsapp` | [ ] | [x] | [ ] |
| `crabbot-plugin-sqlite` | [ ] | [x] | [ ] |
| `crabbot-plugin-scheduler` | [x] | [x] | [ ] |
| `crabbot-plugin-tools` | [ ] | [x] | [x] |
| `crabbot-plugin-tui` | [x] | [x] | [x] |
| `crabbot-plugin-whisper` | [ ] | [x] | [ ] |

The workspace test suite completed successfully on 2026-10-17. The CLI, daemon,
and TUI hands-on checks are marked complete based on the maintainer's report.
An earlier `make verify` run on 2026-10-18 passed all package coverage gates;
the runtime reached 83.41% line coverage and the scheduler plugin reached
90.14%. The latest `make verify` run on 2026-10-18 passed formatting, spacing,
Clippy, workspace checks, and all workspace tests. Runtime, scheduler, and
memory coverage passed; the coverage phase still fails the 80% gate for
OpenRouter, Discord, Slack, Telegram, and WhatsApp.
