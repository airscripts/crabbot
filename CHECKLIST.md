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
| `crabbot-plugin-codex` | [ ] | [x] | [ ] |
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
| `crabbot-plugin-timer` | [ ] | [x] | [ ] |
| `crabbot-plugin-tools` | [ ] | [x] | [ ] |
| `crabbot-plugin-tui` | [x] | [x] | [x] |
| `crabbot-plugin-whisper` | [ ] | [x] | [ ] |

The workspace test suite completed successfully on 2026-09-28. The CLI, daemon,
and TUI hands-on checks are marked complete based on the maintainer's report.
Coverage thresholds are tracked separately: the full profile still needs work
in `crabbot-plugin-openrouter`, `crabbot-plugin-discord`,
`crabbot-plugin-slack`, `crabbot-plugin-telegram`, and
`crabbot-plugin-whatsapp`.
