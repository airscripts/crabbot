# Checklist

Use this checklist to track whether each Krabb crate has received a refinement
pass and has been tested. Automated test runs and hands-on product checks are
tracked separately so one is not mistaken for the other.

## Workspace Crates

| Crate | Refined | Automated Tests | Hands-on Check |
| --- | --- | --- | --- |
| `krabb-core` | [x] | [x] | [ ] |
| `krabb-date` | [ ] | [x] | [ ] |
| `krabb-file` | [ ] | [x] | [ ] |
| `krabb-process` | [ ] | [x] | [ ] |
| `krabb-runtime` | [x] | [x] | [ ] |
| `krabb` | [ ] | [x] | [x] |
| `krabb-daemon` | [ ] | [x] | [x] |
| `krabb-plugin-anthropic` | [ ] | [x] | [ ] |
| `krabb-plugin-acp` | [ ] | [x] | [ ] |
| `krabb-plugin-a2a` | [ ] | [x] | [ ] |
| `krabb-plugin-ag-ui` | [ ] | [x] | [ ] |
| `krabb-plugin-openai` | [ ] | [x] | [ ] |
| `krabb-plugin-codex` | [ ] | [x] | [x] |
| `krabb-plugin-gemini` | [ ] | [x] | [ ] |
| `krabb-plugin-ollama` | [ ] | [x] | [ ] |
| `krabb-plugin-openrouter` | [ ] | [x] | [ ] |
| `krabb-plugin-mcp` | [ ] | [x] | [ ] |
| `krabb-plugin-memory` | [ ] | [x] | [ ] |
| `krabb-plugin-discord` | [ ] | [x] | [ ] |
| `krabb-plugin-signal` | [ ] | [x] | [ ] |
| `krabb-plugin-slack` | [ ] | [x] | [ ] |
| `krabb-plugin-telegram` | [ ] | [x] | [ ] |
| `krabb-plugin-whatsapp` | [ ] | [x] | [ ] |
| `krabb-plugin-sqlite` | [ ] | [x] | [ ] |
| `krabb-plugin-scheduler` | [x] | [x] | [ ] |
| `krabb-plugin-tools` | [ ] | [x] | [x] |
| `krabb-plugin-tui` | [x] | [x] | [x] |
| `krabb-plugin-whisper` | [ ] | [x] | [ ] |

`make verify` passed on 2026-10-25, including formatting, spacing, Clippy,
workspace checks, all package tests, coverage gates, builds, and metrics. The
CLI, daemon, Codex, tools, and TUI hands-on checks are marked complete based on
the maintainer's report. Hands-on checks for the new interoperability and
OpenAI plugins remain unmarked.
