# Contributing

Thanks for helping make Crabbot useful, safe, and easy to extend. Discuss
substantial changes through an issue before opening a pull request. Use
[SECURITY.md](SECURITY.md) for private vulnerability reports.

## Local Setup

```sh
git clone https://github.com/airscripts/crabbot.git
cd crabbot
rustup toolchain install 1.89.0
lefthook install
cargo build --workspace --locked
make verify
```

Lefthook runs fast checks before commits. `make verify` runs formatting,
Clippy, locked compilation, all tests, coverage, builds, and metrics. Provider
tests use local fixtures and never call paid APIs.

## Change Rules

- Keep the core capability-free and provider-neutral.
- Add provider or channel behavior only in its plugin.
- Keep plugin stdout as protocol-only JSONL and logs on stderr.
- Use one precise word for names when possible; do not invent vague utility
  modules or comments that merely restate code.
- Add deterministic tests and documentation for public behavior, permissions,
  and protocol changes.
- Keep filesystem access confined, shell disabled by default, and secrets out
  of logs, manifests, issues, and planning files.
- Never add telemetry or commit credentials.

## Commit Messages

Use lowercase Conventional Commits. When a scope applies, use the
corresponding monorepo module without the `crabbot-` prefix. The CLI is the
exception and uses `cli`; plugin scopes use the plugin ID. Do not use `repo` as
a scope. For genuinely cross-cutting or repository-wide changes, omit the
scope entirely.

```text
feat(core): add turn contracts
feat(runtime): add session state
feat(telegram): add telegram channel
fix(tools): reject unsafe paths
docs(cli): document commands
ci: add verification workflow
```

Keep each commit atomic: one user-visible capability, module, fix, or support
change per commit. Commit messages must be one short, entirely lowercase line
with no body, footer, or extended description. Keep subjects imperative and
under 50 characters. Use `refactor` for structure-only changes, `test` for
test-only changes, `docs` for documentation, `ci` for automation, and `chore`
for repository maintenance.

## Pull Requests

Keep one logical change per pull request. Describe the behavior, security and
platform impact, test commands, documentation updates, and release-note entry.
Explain the fix or feature, not only the symptom that motivated it. Keep
review instructions in the pull request template and write the submission
under its comments.
