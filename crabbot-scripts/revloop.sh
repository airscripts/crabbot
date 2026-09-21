#!/usr/bin/env bash
set -euo pipefail

readonly DEFAULT_MAX_CYCLES=50
readonly DEFAULT_CLEAN_PASSES=3
readonly DEFAULT_CODEX_TIMEOUT='60m'
readonly DEFAULT_VERIFICATION_TIMEOUT='30m'
readonly EXIT_USAGE=2
readonly EXIT_CODEX=70
readonly EXIT_VERIFICATION=71
readonly EXIT_ORCHESTRATOR_OUTPUT=72
readonly EXIT_EXHAUSTED=73
readonly EXIT_REPOSITORY=74
readonly EXIT_VERIFICATION_TIMEOUT=75
readonly REVIEW_CLEAR='Review is clean, so no findings will be listed.'
readonly REVIEW_FINDINGS='Review is not clean, findings are:'

MAX_CYCLES="${CRABBOT_REVLOOP_MAX_CYCLES:-$DEFAULT_MAX_CYCLES}"
CLEAN_PASSES="${CRABBOT_REVLOOP_CLEAN_PASSES:-$DEFAULT_CLEAN_PASSES}"
CODEX_TIMEOUT="${CRABBOT_REVLOOP_CODEX_TIMEOUT:-$DEFAULT_CODEX_TIMEOUT}"
VERIFICATION_TIMEOUT="${CRABBOT_REVLOOP_VERIFICATION_TIMEOUT:-$DEFAULT_VERIFICATION_TIMEOUT}"
OUTPUT_MODE="${CRABBOT_REVLOOP_OUTPUT:-clean}"
MODEL="${CRABBOT_REVLOOP_MODEL:-gpt-5.6-luna}"
REASONING="${CRABBOT_REVLOOP_REASONING:-high}"
GLOBAL_REVIEW=false

print_error() {
    printf '[ERROR] %s\n' "$*" >&2
}

print_info() {
    printf '[INFO] %s\n' "$*"
}

print_warn() {
    printf '[WARN] %s\n' "$*" >&2
}

usage() {
    printf '%s\n' \
        'Usage: crabbot-scripts/revloop.sh [--global] [--clean|--verbose]'
    printf '%s\n' 'Default output mode: clean.'
    printf '%s\n' \
        'Default scope: uncommitted changes, the latest commit, or the feature branch.'
    printf '%s\n' 'Use --global for a repository-wide audit.'
    printf '%s\n' 'Environment:'
    printf '%s\n' '  CRABBOT_CODEX_HOME=~/.codex'
    printf '%s\n' '  CRABBOT_REVLOOP_MODEL=gpt-5.6-luna'
    printf '%s\n' '  CRABBOT_REVLOOP_REASONING=high'
    printf '%s\n' '  CRABBOT_REVLOOP_MAX_CYCLES=50 CRABBOT_REVLOOP_OUTPUT=clean|verbose'
    printf '%s\n' '  CRABBOT_REVLOOP_CLEAN_PASSES=3.'
    printf '%s\n' '  CRABBOT_REVLOOP_CODEX_TIMEOUT=60m.'
    printf '%s\n' '  CRABBOT_REVLOOP_VERIFICATION_TIMEOUT=30m.'
}

die() {
    local status=$1
    shift
    print_error "$*"
    exit "$status"
}

while (($# > 0)); do
    case "$1" in
        --global)
            GLOBAL_REVIEW=true
            ;;
        --clean)
            OUTPUT_MODE='clean'
            ;;
        --verbose)
            OUTPUT_MODE='verbose'
            ;;
        --help|-h)
            usage
            exit 0
            ;;
        *)
            die "$EXIT_USAGE" "Unknown option '$1'. Use --help for usage."
            ;;
    esac

    shift
done

case "$OUTPUT_MODE" in
    clean|verbose)
        ;;
    *)
        die "$EXIT_USAGE" \
            "CRABBOT_REVLOOP_OUTPUT must be 'clean' or 'verbose'; got '$OUTPUT_MODE'."
        ;;
esac

[[ -n "$MODEL" ]] || die "$EXIT_USAGE" 'CRABBOT_REVLOOP_MODEL must not be empty.'

case "$REASONING" in
    low|medium|high|xhigh)
        ;;
    *)
        die "$EXIT_USAGE" \
            "CRABBOT_REVLOOP_REASONING must be low, medium, high, or xhigh; got '$REASONING'."
        ;;
esac

if ! [[ "$MAX_CYCLES" =~ ^[1-9][0-9]*$ ]]; then
    die "$EXIT_USAGE" "CRABBOT_REVLOOP_MAX_CYCLES must be a positive integer; got '$MAX_CYCLES'."
fi

if ! [[ "$CLEAN_PASSES" =~ ^[1-9][0-9]*$ ]]; then
    die "$EXIT_USAGE" \
        "CRABBOT_REVLOOP_CLEAN_PASSES must be a positive integer; got '$CLEAN_PASSES'."
fi

if ! SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"; then
    die "$EXIT_REPOSITORY" 'Unable to determine the script directory.'
fi

if REPO_ROOT="$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel 2>/dev/null)"; then
    if ! REPO_ROOT="$(cd "$REPO_ROOT" && pwd -P)"; then
        die "$EXIT_REPOSITORY" 'Unable to resolve the Git repository root.'
    fi
else
    if ! REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd -P)"; then
        die "$EXIT_REPOSITORY" 'Unable to resolve the repository root.'
    fi
fi

[[ -f "$REPO_ROOT/Cargo.toml" ]] || die "$EXIT_REPOSITORY" \
    "Repository root '$REPO_ROOT' has no Cargo.toml."

[[ -f "$REPO_ROOT/Makefile" ]] || die "$EXIT_REPOSITORY" \
    "Repository root '$REPO_ROOT' has no Makefile."

if [[ "$GLOBAL_REVIEW" == true ]]; then
    REVIEW_SCOPE='global repository-wide audit'
elif [[ -n "$(git -C "$REPO_ROOT" status --porcelain --untracked-files=all)" ]]; then
    REVIEW_SCOPE='uncommitted working-tree changes (staged, unstaged, and
untracked files)'
else
    CURRENT_BRANCH="$(git -C "$REPO_ROOT" branch --show-current)"
    if [[ -n "$CURRENT_BRANCH" && "$CURRENT_BRANCH" != 'main' ]]; then
        if git -C "$REPO_ROOT" show-ref --verify --quiet refs/heads/main; then
            REVIEW_SCOPE="feature branch '$CURRENT_BRANCH' against main"
            REVIEW_SCOPE+=' (main...HEAD)'
        elif git -C "$REPO_ROOT" show-ref --verify --quiet refs/remotes/origin/main; then
            REVIEW_SCOPE="feature branch '$CURRENT_BRANCH' against origin/main"
            REVIEW_SCOPE+=' (origin/main...HEAD)'
        else
            REVIEW_SCOPE='latest commit (HEAD^..HEAD) because main is not available locally'
        fi
    elif [[ "$CURRENT_BRANCH" == 'main' ]]; then
        REVIEW_SCOPE='latest commit on main (HEAD^..HEAD)'
    else
        REVIEW_SCOPE='latest commit on the current revision (HEAD^..HEAD)'
    fi
fi

if [[ -z "${CRABBOT_CODEX_HOME:-}" ]]; then
    if [[ -z "${HOME:-}" ]]; then
        die "$EXIT_CODEX" \
            'HOME is not set; configure CRABBOT_CODEX_HOME before starting revloop.'
    fi

    CRABBOT_CODEX_HOME="$HOME/.codex"
fi

export CRABBOT_CODEX_HOME

for required_tool in bash cargo cksum make timeout; do
    command -v "$required_tool" >/dev/null 2>&1 || die "$EXIT_VERIFICATION" \
        "Required verification tool '$required_tool' is not available on PATH."
done

if ! timeout "$CODEX_TIMEOUT" true >/dev/null 2>&1; then
    die "$EXIT_USAGE" \
        "CRABBOT_REVLOOP_CODEX_TIMEOUT must be accepted by timeout; got '$CODEX_TIMEOUT'."
fi

if ! timeout "$VERIFICATION_TIMEOUT" true >/dev/null 2>&1; then
    die "$EXIT_USAGE" \
        "CRABBOT_REVLOOP_VERIFICATION_TIMEOUT must be accepted by timeout; got '$VERIFICATION_TIMEOUT'."
fi

if [[ "$OUTPUT_MODE" == 'verbose' ]] && \
    ! command -v tee >/dev/null 2>&1
then
    die "$EXIT_VERIFICATION" \
        "Required verification tool 'tee' is not available on PATH."
fi

command -v codex >/dev/null 2>&1 || die "$EXIT_CODEX" \
    'Codex is not available on PATH.'

STATE_ROOT="$REPO_ROOT/.revloop"
mkdir -p "$STATE_ROOT"

if ! RUN_ID="$(date -u '+%Y%m%dT%H%M%SZ')-$$"; then
    die "$EXIT_REPOSITORY" 'Unable to create a loop run identifier.'
fi

RUN_DIR="$STATE_ROOT/$RUN_ID"
mkdir -p "$RUN_DIR"

LATEST_ORCHESTRATOR="$RUN_DIR/latest-orchestrator.txt"
LATEST_ORCHESTRATOR_LOG="$RUN_DIR/latest-orchestrator.log"
LATEST_WORKER="$RUN_DIR/latest-worker.txt"
LATEST_WORKER_LOG="$RUN_DIR/latest-worker.log"

write_log() {
    local log_file=$1

    if [[ "$OUTPUT_MODE" == 'verbose' ]]; then
        tee "$log_file"
        return
    fi

    : >"$log_file"

    while IFS= read -r line || [[ -n "$line" ]]; do
        printf '%s\n' "$line" >>"$log_file" || return 1

        case "$line" in
            '[INFO]'*|'[WARN]'*|'[ERROR]'*)
                printf '%s\n' "$line" || return 1
                ;;
        esac
    done
}

run_codex() {
    local role=$1
    local sandbox=$2
    local prompt_file=$3
    local output_file=$4
    local log_file=$5
    local status
    local role_name
    local pipeline_status
    local sink_status

    case "$role" in
        orchestrator)
            role_name='Orchestrator'
            ;;
        worker)
            role_name='Worker'
            ;;
        *)
            role_name="$role"
            ;;
    esac

    print_info "Starting $role_name invocation."

    set +e
    (
        set -e
        cd "$REPO_ROOT"

        # Codex CLI has no flag for its home, so translate the canonical setting at this boundary.
        timeout --foreground --kill-after=30s "$CODEX_TIMEOUT" \
            env CODEX_HOME="$CRABBOT_CODEX_HOME" codex exec \
                --cd "$REPO_ROOT" \
                --sandbox "$sandbox" \
                --model "$MODEL" \
                --config 'approval_policy="never"' \
                --config "model_reasoning_effort=\"$REASONING\"" \
                --ephemeral \
                --color never \
                --output-last-message "$output_file" \
                - < "$prompt_file"
    ) </dev/null 2>&1 | write_log "$log_file"
    pipeline_status=("${PIPESTATUS[@]}")
    status=${pipeline_status[0]}
    sink_status=${pipeline_status[1]}
    set -e

    if (( status == 0 && sink_status != 0 )); then
        status=$sink_status
    fi

    if (( status == 124 )); then
        print_error "$role_name invocation timed out after $CODEX_TIMEOUT."
        print_error "Codex log: $log_file"
        return "$EXIT_CODEX"
    fi

    if (( status != 0 )); then
        print_error "$role_name invocation failed with exit status $status."
        print_error "Codex log: $log_file"
        return "$EXIT_CODEX"
    fi

    if [[ ! -s "$output_file" ]]; then
        print_error "$role_name invocation produced no final message."
        print_error "Codex log: $log_file"
        return "$EXIT_CODEX"
    fi
}

write_orchestrator_prompt() {
    local prompt_file=$1
    local verification_file=${2:-}
    local verification_note
    local review_scope
    local scope_guard

    if [[ -n "$verification_file" ]]; then
        verification_note="$verification_file"
    else
        verification_note='None; no verification failure is currently pending.'
    fi

    case "$REVIEW_SCOPE" in
        global*)
            review_scope='Global mode is enabled. Audit the actual repository-wide
product code, scripts, documentation, manifests, release files, and CI. Include
unchanged code and the current working tree when assessing reachable material
defects.'
            scope_guard='In global mode, report relevant pre-existing defects in
untouched areas as well as defects introduced by current changes. Keep the audit
bounded to repository content and exclude generated artifacts listed above.'
            ;;
        uncommitted*)
            review_scope='Review the uncommitted working-tree change set as the
primary scope. Include staged and unstaged changes, untracked source files, and
behavior directly affected by them.'
            scope_guard='Do not turn this review into an unrestricted audit of the
entire repository. Do not report unrelated pre-existing defects in untouched
areas unless the current changes introduce them, expose them, materially worsen
them, depend on them, or make them directly relevant to the changed behavior.'
            ;;
        feature*)
            review_scope="""No uncommitted files are present. Review the complete
feature branch represented by $REVIEW_SCOPE, including its commits relative to
main and any behavior directly affected by those changes."""
            scope_guard='Do not turn this review into an unrestricted audit of the
entire repository. Do not report unrelated pre-existing defects outside the
feature branch unless the branch changes introduce them, expose them, materially
worsen them, depend on them, or make them directly relevant.'
            ;;
        *)
            review_scope="""No uncommitted files are present. Review the latest
commit represented by $REVIEW_SCOPE and behavior directly affected by it. If no
parent commit is available, inspect the current repository baseline as needed to
understand that commit."""
            scope_guard='Do not turn this review into an unrestricted audit of the
entire repository. Do not report unrelated pre-existing defects in untouched
areas unless the latest commit introduces them, exposes them, materially worsens
them, depends on them, or makes them directly relevant.'
            ;;
    esac

    cat >"$prompt_file" <<EOF
# Role

You are the fresh, independent material-defect orchestrator for the Crabbot
repository.

Repository: $REPO_ROOT

Read the repository-root AGENTS.md and every applicable nested AGENTS.md before
reviewing.

The repository is a Rust 2024 workspace containing crabbot-core, crabbot,
crabbot-daemon, and
the crabbot-plugins/* process plugins.

The authoritative local verification gate is:

make verify

The loop artifacts under:

$STATE_ROOT

are diagnostics, not product code. Do not review or modify them unless the
review specifically concerns crabbot-scripts/revloop.sh itself.

Never enumerate or read target/, .revloop/, .git/, or generated build
artifacts. They are not review scope and can contain very large, redundant
outputs. Use focused source and configuration paths instead.

# Review Scope

$review_scope

Inspect:

- Current Git status.
- Current tracked modifications.
- Current staged modifications.
- Current untracked source files.
- Surrounding implementation required to understand the changes.
- Relevant callers and callees.
- Relevant tests.
- Relevant manifests and configuration.
- Relevant CI and verification configuration.
- Applicable repository instructions.

You may inspect unchanged surrounding code whenever necessary to establish
correctness, understand contracts, or identify consequences of the current
changes.

$scope_guard

# Verification Context

A previous relevant verification log is located here:

$verification_note

If a verification log is present, inspect it.

Distinguish carefully between:

- Real repository defects.
- Real test failures.
- Real configuration defects.
- Environmental failures.
- Missing tools.
- Network failures.
- Sandbox limitations.
- External service failures.

Do not propose product-code changes merely to conceal an environmental or
tooling failure.

# Material Findings

Review only realistically reachable and materially important defects.

This is a deep review, not a first-finding triage. Perform an exhaustive pass
over the in-scope changes before writing the report. Continue looking after
each finding and report every distinct actionable finding identified in this
invocation. Do not stop after the first finding, select only the highest
severity finding, or defer an identifiable finding to a later cycle. Combine
only findings that have the same root cause and failure mode; keep independent
defects as separate blocks.

Before emitting the final report, perform a second completeness pass over the
changed files, their callers and callees, tests, manifests, release packaging,
and verification configuration. Include valid non-blocking findings as well
as blocking findings. The blocker-priority loop is applied after this complete
report is produced: Blocking: Yes findings gate worker cycles, while
Blocking: No findings remain recorded without forcing extra cycles.

Classify merge impact separately from severity. Mark a finding Blocking: Yes
when the defect prevents a released feature from installing, activating, or
performing its advertised behavior, or when it would make a release artifact
unusable. This remains true when the workspace builds and unit tests pass but
the failure appears only in packaging, installation, discovery, or runtime
integration. Mark Blocking: No only for a valid defect that does not affect a
released workflow and can safely ship without user-visible loss of function.
For example, a release archive that contains an executable name the runtime
cannot discover is Blocking: Yes, even when source tests pass.
If a concern is not a concrete bug or problem, do not report it as a finding.

Valid review categories include:

- Correctness bugs.
- Regressions.
- Security vulnerabilities.
- Race conditions.
- Concurrency errors.
- Broken error handling.
- Incorrect assumptions.
- Important missing edge cases.
- API contract violations.
- Protocol contract violations.
- Schema violations.
- Data-loss risks.
- Data-corruption risks.
- Resource leaks.
- Meaningful type-safety problems.
- Material performance problems.
- Missing regression coverage for behavior whose failure would be material and
  is not already adequately protected by existing tests.

Lack of direct test coverage alone is not a finding.

A test-gap finding requires a concrete material behavior or regression risk
that is insufficiently protected.

# Simplicity and Maintainability

The code should be correct, well engineered, well tested, simple, and
economical to maintain.

Treat unnecessary code as a maintenance liability.

All else being equal:

- Fewer lines of code are preferable to more lines of code.
- Fewer abstractions are preferable to more abstractions.
- Fewer moving parts are preferable to more moving parts.
- Fewer dependencies are preferable to more dependencies.
- Less duplicated logic is preferable to more duplicated logic.
- Smaller public APIs are preferable to larger ones.
- Smaller focused changes are preferable to broad rewrites.

This does not mean minimizing line count at the expense of readability,
correctness, explicit behavior, safety, or testing.

Do not report simplicity or maintainability findings merely because an
alternative implementation would be shorter or stylistically preferable.

A simplicity or maintainability finding is actionable only when it identifies
a specific and localized source of material defect risk, reliability risk,
maintenance risk, or materially unnecessary complexity in the changed area.

Examples may include:

- Duplicate logic that can realistically diverge.
- Unnecessary state that creates additional failure modes.
- Dead code that materially obscures behavior.
- Redundant wrappers that materially obscure control flow.
- Over-generalized abstractions that create concrete maintenance risk.
- Unnecessary dependencies for trivial functionality.
- Multiple paths that can safely and materially be reduced to one.

A merely simpler alternative is not a finding.

# Non-Findings

Do not continue the loop for:

- Stylistic preferences.
- Naming preferences.
- Formatting differences.
- Optional refactors.
- Cleanup-only changes.
- Documentation polish.
- Speculative improvements without a concrete failure mode.
- Theoretical cases that are not realistically reachable.
- Duplicate findings.
- "Could be cleaner" observations.
- Alternative implementations that are not materially more correct.
- Hypothetical future requirements.
- Generalization for possible future use.
- Test additions that provide no meaningful additional confidence.

Finding nothing is valid.

A clear review is a desirable outcome when appropriate.

Do not invent work merely to continue the loop.

# Finding Validation

Before reporting any finding, actively attempt to disprove it.

Where relevant, inspect:

- Current implementation.
- Surrounding implementation.
- Call sites.
- Type definitions.
- Existing tests.
- Error paths.
- State transitions.
- Concurrency behavior.
- Configuration.
- Repository conventions.
- Existing validation logic.

Do not report a finding if reasonable inspection disproves it.

Do not report duplicate manifestations of the same root cause as separate
findings unless they genuinely require distinct fixes.

Every finding must identify:

- Concrete evidence.
- A realistic failure mode.
- A materially incorrect behavior or material risk.
- A focused root-cause fix.

# Severity

Use severity conservatively.

Severity definitions:

- Critical: A realistically reachable defect likely to cause catastrophic
  security, integrity, data-loss, or availability impact.
- High: A realistically reachable defect likely to cause major correctness,
  security, data-loss, integrity, or availability impact.
- Medium: A concrete, realistically reachable user-visible or operational
  defect with meaningful impact.
- Low: A minor defect, difficult-to-reach issue, non-material behavior,
  maintainability observation, or preference.

Do not report Low observations.

When uncertain between two severities, choose the lower severity.

Do not inflate severity to make a finding actionable.

# Repository Safety

Do not modify repository state.

Do not:

- Edit files.
- Commit.
- Reset.
- Stash.
- Restore.
- Rebase.
- Push.
- Amend.
- Rewrite history.
- Call paid provider APIs.

# Output Contract

The output contract is strict.

If there are no concrete findings, output exactly:

$REVIEW_CLEAR

Output nothing else.

Do not include:

- Markdown.
- Explanation.
- Praise.
- Summary.
- Commentary.

If actionable findings remain, the first line must be exactly:

$REVIEW_FINDINGS

Then emit one or more distinct finding blocks.

Each block must contain exactly these seven labelled lines:

Severity: Critical|High|Medium
Blocking: Yes|No
File: repository-relative path and line or location
Problem: concrete defect
Evidence: concrete code, control-flow, state, test, or contract evidence
Failure Mode: realistic consequence or reproduction path
Required Fix: smallest correct root-cause fix

Separate finding blocks with one blank line.

Do not add headings, bullets, markdown, commentary, Low findings, or optional
suggestions.
EOF
}

write_worker_prompt() {
    local prompt_file=$1
    local review_file=$2
    local verification_file=${3:-}
    local verification_note

    if [[ -n "$verification_file" ]]; then
        verification_note="$verification_file"
    else
        verification_note='None; no verification failure is currently pending.'
    fi

    cat >"$prompt_file" <<EOF
# Role

You are the focused worker executor for the current Crabbot repository working
tree.

Repository: $REPO_ROOT

Read the repository-root AGENTS.md and every applicable nested AGENTS.md before
making changes.

The orchestrator selected this review scope:

$REVIEW_SCOPE

Keep worker changes within that scope. In global mode, valid blocking findings
may be fixed anywhere in the actual repository, subject to the repository
instructions and the generated-artifact exclusions.

The latest independent review is stored at:

$review_file

A relevant verification failure, if any, is stored at:

$verification_note

# Inputs

Read the review report before changing anything.

The review file may contain either:

- $REVIEW_FINDINGS followed by actionable findings.
- $REVIEW_CLEAR when the independent orchestrator found no material defect.

Every finding includes a Blocking: Yes or Blocking: No classification.
Only blocking findings require worker changes. Non-blocking findings remain
visible in the report, but do not prevent successful completion.

A clear review does not override a failing verification log.

If a verification failure is provided, treat it as an independent input that
requires investigation even when the review file contains no findings.

Determine whether each verification failure represents:

- A real code defect.
- A real test defect.
- A real repository configuration defect.
- An environmental failure.
- A missing tool.
- A network failure.
- A sandbox limitation.
- An external service failure.

Do not modify product code merely to conceal an environmental, tooling,
network, or sandbox failure.

# Finding Validation

Verify every orchestrator finding against the current repository state before
changing code.

For every reported finding:

- Confirm that it still applies.
- Inspect the cited code and surrounding implementation.
- Inspect relevant callers and tests.
- Confirm the stated failure mode.
- Reject false positives.
- Reject findings already resolved by the current working tree.

Do not change correct code merely to satisfy an orchestrator.

# Fixing Requirements

For each valid blocking finding or real verification defect:

- Fix the root cause.
- Use the smallest clear change that solves the problem correctly.
- Follow existing repository architecture and conventions.
- Preserve existing correct behavior.
- Add or update a focused regression test when it provides meaningful
  protection.
- Reuse existing test patterns and helpers where practical.
- Run the narrowest relevant verification after the change.
- Keep provider-related tests deterministic.

Non-blocking findings must remain visible in the report, but do not require a
worker change unless the change is straightforward and safe.
- Never call paid provider APIs.

Prefer deleting unnecessary code over adding compensating code when both
approaches are equally correct and clear.

Prefer:

- Existing patterns over new patterns.
- Direct solutions over generalized solutions.
- Focused functions over unnecessary frameworks.
- Clear control flow over cleverness.
- Existing utilities over near-duplicates.
- Smaller diffs over broader rewrites.
- Fewer moving parts when correctness and clarity are preserved.
- Fewer lines of code when readability, correctness, safety, and explicitness
  are preserved.

Do not optimize for line count itself.

The objective is the smallest clear, correct, robust, well-tested, and
maintainable implementation.

# Scope Control

Keep all changes focused on the validated finding or verification defect.

Do not:

- Perform unrelated refactoring.
- Perform unrelated style cleanup.
- Introduce optional abstractions.
- Generalize for hypothetical future requirements.
- Add speculative compatibility mechanisms.
- Expand public APIs unnecessarily.
- Add unnecessary dependencies.
- Rewrite stable code merely because another implementation is shorter.
- Add tests solely to increase coverage numbers.
- Introduce excessive test scaffolding for trivial behavior.

If an existing test already protects the behavior adequately, do not duplicate
it.

# Repository Safety

Preserve unrelated local modifications.

Do not:

- Edit AGENTS.md files.
- Edit loop artifacts under $STATE_ROOT unless the finding explicitly concerns
  crabbot-scripts/revloop.sh.
- Commit.
- Reset.
- Stash.
- Restore unrelated files.
- Rebase.
- Push.
- Amend.
- Rewrite Git history.

# Completion

After handling every applicable finding and any real verification defect:

- Run relevant focused verification.
- Inspect the resulting diff for accidental breakage.
- Stop.

Do not declare the repository clear.

Do not claim that no further findings exist.

A completely fresh orchestrator invocation must determine cleanliness.

In the final message, briefly state:

- What you changed.
- Which findings you rejected, if any, and why.
- What verification you ran.
- Whether any verification could not run because of environment or tooling.

Do not claim that the repository is clean.
EOF
}

validate_review_output() {
    local review_file=$1
    local content

    content="$(<"$review_file")"

    if [[ "$content" == "$REVIEW_CLEAR" ]]; then
        return 0
    fi

    if [[ "$content" != "$REVIEW_FINDINGS"$'\n'* ]]; then
        print_error \
            "Invalid orchestrator output in $review_file: expected a clear review or"
        print_error 'a findings report with the required finding fields.'
        return "$EXIT_ORCHESTRATOR_OUTPUT"
    fi

    if ! awk -v header="$REVIEW_FINDINGS" '
        function finish() {
            if (!in_block) {
                return
            }

            if (!(severity && blocking && file && problem && evidence && failure && fix)) {
                invalid = 1
            }

            count++
            in_block = 0
            severity = 0
            blocking = 0
            file = 0
            problem = 0
            evidence = 0
            failure = 0
            fix = 0
        }

        NR == 1 {
            if ($0 != header) {
                invalid = 1
            }
            next
        }

        $0 == "" {
            finish()
            next
        }

        /^Severity: (Critical|High|Medium)$/ {
            if (severity) {
                finish()
            }

            in_block = 1
            severity = 1
            next
        }

        /^Blocking: (Yes|No)$/ {
            if (!in_block || blocking) {
                invalid = 1
            }

            blocking = 1
            next
        }

        /^File:[[:space:]]*[^[:space:]]/ {
            if (!in_block || file) {
                invalid = 1
            }

            file = 1
            next
        }

        /^Problem:[[:space:]]*[^[:space:]]/ {
            if (!in_block || problem) {
                invalid = 1
            }

            problem = 1
            next
        }

        /^Evidence:[[:space:]]*[^[:space:]]/ {
            if (!in_block || evidence) {
                invalid = 1
            }

            evidence = 1
            next
        }

        /^Failure Mode:[[:space:]]*[^[:space:]]/ {
            if (!in_block || failure) {
                invalid = 1
            }

            failure = 1
            next
        }

        /^Required Fix:[[:space:]]*[^[:space:]]/ {
            if (!in_block || fix) {
                invalid = 1
            }

            fix = 1
            next
        }

        {
            invalid = 1
        }

        END {
            finish()

            if (invalid || count == 0) {
                exit 1
            }
        }
    ' "$review_file"; then
        print_error \
            "Invalid orchestrator output in $review_file: malformed finding block."
        return "$EXIT_ORCHESTRATOR_OUTPUT"
    fi
}

has_blocking_findings() {
    local review_file=$1

    awk '/^Blocking: Yes$/ { found = 1 } END { exit found ? 0 : 1 }' "$review_file"
}

finding_count() {
    local review_file=$1

    awk '/^Severity: (Critical|High|Medium)$/ { count++ } END { print count + 0 }' \
        "$review_file"
}

run_verification_command() {
    local label=$1
    shift
    local status

    if timeout --foreground --kill-after=30s "$VERIFICATION_TIMEOUT" "$@"; then
        status=0
    else
        status=$?
    fi

    if (( status == 124 )); then
        print_warn "$label timed out after $VERIFICATION_TIMEOUT."
        return "$EXIT_VERIFICATION_TIMEOUT"
    fi

    return "$status"
}

run_fast_verification() {
    local log_file=$1
    local status
    local pipeline_status
    local sink_status

    print_info 'Running focused verification.'

    set +e
    (
        set -e
        cd "$REPO_ROOT"

        print_info 'Applying Rust spacing formatter...'
        run_verification_command 'Rust spacing formatter' make spacing || exit $?

        print_info 'Running format check (1/4)...'
        run_verification_command 'Format check' make fmt || exit $?

        print_info 'Running workspace check (2/4)...'
        run_verification_command 'Workspace check' make check || exit $?

        print_info 'Running workspace tests (3/4)...'
        run_verification_command 'Workspace tests' make test || exit $?

        print_info 'Running shell syntax check (4/4)...'
        run_verification_command 'Shell syntax check' bash -n crabbot-scripts/*.sh install.sh || exit $?
    ) </dev/null 2>&1 | write_log "$log_file"
    pipeline_status=("${PIPESTATUS[@]}")
    status=${pipeline_status[0]}
    sink_status=${pipeline_status[1]}
    set -e

    if (( status == 0 && sink_status != 0 )); then
        status=$sink_status
    fi

    if (( status != 0 )); then
        if (( status == EXIT_VERIFICATION_TIMEOUT )); then
            print_warn 'Focused verification stopped because a phase timed out.'
            print_warn "Verification log: $log_file"
        else
            print_error "Focused verification failed with exit status $status."
            print_error "Verification log: $log_file"
        fi
        return "$status"
    fi
}

run_fast_verification_with_retry() {
    local log_file=$1
    local status

    if run_fast_verification "$log_file"; then
        return 0
    else
        status=$?
    fi

    if (( status != EXIT_VERIFICATION_TIMEOUT )); then
        return "$status"
    fi

    print_warn 'Focused verification timed out; retrying once without consuming a cycle.'

    if run_fast_verification "$log_file"; then
        return 0
    else
        status=$?
    fi

    if (( status == EXIT_VERIFICATION_TIMEOUT )); then
        print_warn 'Focused verification timed out twice.'
    fi

    return "$status"
}

verification_failure_signature() {
    local log_file=$1
    local failure_lines

    failure_lines="$(grep -E \
        '^(---- .* stdout ----|test result: FAILED|error: test failed|[[:space:]]*called .+ panicked)' \
        "$log_file" || true)"

    [[ -n "$failure_lines" ]] || return 0

    printf '%s' "$failure_lines" | cksum | awk '{ print $1 ":" $2 }'
}

run_final_verification() {
    local log_file=$1
    local status
    local environment_failure_pattern
    local coverage_environment_pattern
    local pipeline_status
    local sink_status

    environment_failure_pattern='failed to acquire advisory database'
    environment_failure_pattern+='|couldn.t fetch advisory database'
    environment_failure_pattern+='|network is unreachable'
    environment_failure_pattern+='|request could not be completed in the allotted timeframe'
    environment_failure_pattern+='|couldn.t check if the package is yanked'
    environment_failure_pattern+='|read-only path'
    coverage_environment_pattern='target/llvm-cov-target'
    coverage_environment_pattern+='|failed to remove file.*permission denied'

    print_info 'Running final CI-equivalent verification.'

    set +e
    (
        set -e
        cd "$REPO_ROOT"

        for required_tool in \
            cargo-llvm-cov \
            agentskill \
            cargo-deny \
            cargo-audit
        do
            if ! command -v "$required_tool" >/dev/null 2>&1; then
                print_error "Required final verification tool is missing: $required_tool."
                exit "$EXIT_VERIFICATION"
            fi
        done

        if ! cargo +1.89 --version >/dev/null 2>&1; then
            print_error 'Required Rust 1.89 toolchain is not available.'
            exit "$EXIT_VERIFICATION"
        fi

        print_info 'Applying Rust spacing formatter before complete verification...'
        run_verification_command \
            'Rust spacing formatter' \
            make spacing || exit $?

        print_info 'Running complete verification (1/10)...'
        run_verification_command 'Complete verification' make verify || exit $?

        print_info 'Running Rustdoc check (2/10)...'
        run_verification_command \
            'Rustdoc check' \
            env RUSTDOCFLAGS='-D warnings' \
            cargo doc \
            --workspace \
            --no-deps \
            --locked || exit $?

        print_info 'Running shell syntax check (3/10)...'
        run_verification_command \
            'Shell syntax check' \
            bash -n crabbot-scripts/*.sh install.sh || exit $?

        print_info 'Running all-target workspace check (4/10)...'
        run_verification_command \
            'All-target workspace check' \
            env CARGO_BUILD_JOBS=4 \
            cargo check \
            --workspace \
            --all-targets \
            --locked || exit $?

        print_info 'Running MSRV workspace check (5/10)...'
        run_verification_command \
            'MSRV workspace check' \
            env CARGO_BUILD_JOBS=4 \
            cargo +1.89 check \
            --workspace \
            --all-targets \
            --locked || exit $?

        print_info 'Running release build (6/10)...'
        run_verification_command \
            'Release build' \
            cargo build \
            --workspace \
            --release \
            --locked || exit $?

        print_info 'Running release smoke test (7/10)...'
        run_verification_command \
            'Release smoke test' \
            cargo run \
            -p crabbot \
            --bin crabbot \
            --release \
            -- \
            version || exit $?

        print_info 'Running Agentskill document check (8/10)...'
        run_verification_command \
            'Agentskill document check' \
            agentskill validate . --signature auto || exit $?

        print_info 'Running dependency policy check (9/10)...'
        run_verification_command 'Dependency policy check' cargo deny check || exit $?

        print_info 'Running RustSec audit (10/10)...'
        run_verification_command 'RustSec audit' cargo audit || exit $?
    ) </dev/null 2>&1 | write_log "$log_file"
    pipeline_status=("${PIPESTATUS[@]}")
    status=${pipeline_status[0]}
    sink_status=${pipeline_status[1]}
    set -e

    if (( status == 0 && sink_status != 0 )); then
        status=$sink_status
    fi

    if (( status != 0 )); then
        if (( status == EXIT_VERIFICATION_TIMEOUT )); then
            print_warn 'Final verification stopped because a phase timed out.'
            print_warn "Verification log: $log_file"
            return "$status"
        fi

        if grep -Eiq "$coverage_environment_pattern" "$log_file"; then
            print_warn \
                'Coverage verification was unavailable because existing build artifacts could not be removed.'
            print_warn "Verification log: $log_file"
            return "$EXIT_VERIFICATION"
        fi

        if grep -Eiq "$environment_failure_pattern" "$log_file" && \
            grep -Eiq \
                'Running dependency policy check|Running RustSec audit|advisory database' \
                "$log_file"
        then
            print_warn \
                'Dependency audit was unavailable because the local advisory database could not be accessed.'
            print_warn "Verification log: $log_file"
            return "$EXIT_VERIFICATION"
        fi

        if (( status == EXIT_VERIFICATION )); then
            print_error \
                'Final verification could not run in the current environment.'
            print_error "Verification log: $log_file"
            return "$EXIT_VERIFICATION"
        fi

        print_error "Final verification failed with exit status $status."
        print_error "Verification log: $log_file"
        return "$status"
    fi
}

run_final_verification_with_retry() {
    local log_file=$1
    local status

    if run_final_verification "$log_file"; then
        return 0
    else
        status=$?
    fi

    if (( status != EXIT_VERIFICATION_TIMEOUT )); then
        return "$status"
    fi

    print_warn 'Final verification timed out; retrying once without consuming a cycle.'

    if run_final_verification "$log_file"; then
        return 0
    else
        status=$?
    fi

    if (( status == EXIT_VERIFICATION_TIMEOUT )); then
        print_warn 'Final verification timed out twice.'
    fi

    return "$status"
}

copy_latest_verification() {
    local log_file=$1

    cp "$log_file" "$RUN_DIR/latest-verification.log"
}

run_worker_pass() {
    local cycle=$1
    local review_file=$2
    local verification_file=${3:-}
    local artifact_name=${4:-worker}
    local cycle_tag
    local prompt_file
    local worker_file
    local worker_log

    printf -v cycle_tag '%02d' "$cycle"

    prompt_file="$RUN_DIR/cycle-${cycle_tag}-${artifact_name}-prompt.txt"
    worker_file="$RUN_DIR/cycle-${cycle_tag}-${artifact_name}.txt"
    worker_log="$RUN_DIR/cycle-${cycle_tag}-${artifact_name}.log"

    write_worker_prompt \
        "$prompt_file" \
        "$review_file" \
        "$verification_file"

    if ! run_codex \
        'worker' \
        'workspace-write' \
        "$prompt_file" \
        "$worker_file" \
        "$worker_log"
    then
        print_error "Worker output is preserved at $worker_file."
        return "$EXIT_CODEX"
    fi

    cp "$worker_file" "$LATEST_WORKER"
    cp "$worker_log" "$LATEST_WORKER_LOG"
}

report_exhausted() {
    local review_file=$1

    print_error "Did not converge within CRABBOT_REVLOOP_MAX_CYCLES=$MAX_CYCLES."
    print_error "Latest orchestrator output: $review_file"
    print_error 'Current working tree changes were preserved.'
    exit "$EXIT_EXHAUSTED"
}

pending_verification=''
final_verification_pending=0
clean_passes=0
repeated_verification_failures=0
last_verification_signature=''
cycle=1

print_info "Repository: $REPO_ROOT."
print_info "State: $RUN_DIR."
print_info "Codex home: $CRABBOT_CODEX_HOME."
print_info "Model: $MODEL."
print_info "Reasoning: $REASONING."
print_info "Output mode: $OUTPUT_MODE."
print_info "Review scope: $REVIEW_SCOPE."
print_info "Maximum cycles: $MAX_CYCLES."
print_info "Required consecutive clean reviews: $CLEAN_PASSES."
print_info "Verification timeout per phase: $VERIFICATION_TIMEOUT."

while (( cycle <= MAX_CYCLES )); do
    printf '\n'
    print_info "Cycle $cycle/$MAX_CYCLES: Starting fresh orchestration."

    printf -v cycle_tag '%02d' "$cycle"

    review_prompt="$RUN_DIR/cycle-${cycle_tag}-orchestrator-prompt.txt"
    review_file="$RUN_DIR/cycle-${cycle_tag}-orchestrator.txt"
    review_log="$RUN_DIR/cycle-${cycle_tag}-orchestrator.log"

    write_orchestrator_prompt \
        "$review_prompt" \
        "$pending_verification"

    if ! run_codex \
        'orchestrator' \
        'read-only' \
        "$review_prompt" \
        "$review_file" \
        "$review_log"
    then
        if [[ -f "$review_log" ]]; then
            cp "$review_log" "$LATEST_ORCHESTRATOR_LOG"
        fi
        print_error \
            "Orchestrator output is preserved at $review_file and $review_log."
        exit "$EXIT_CODEX"
    fi

    cp "$review_file" "$LATEST_ORCHESTRATOR"
    cp "$review_log" "$LATEST_ORCHESTRATOR_LOG"

    if ! validate_review_output "$review_file"; then
        print_error "Orchestrator output is preserved at $review_file."
        exit "$EXIT_ORCHESTRATOR_OUTPUT"
    fi

    report_content="$(<"$review_file")"

    if [[ "$report_content" != "$REVIEW_CLEAR" ]]; then
        clean_passes=0
    fi

    if [[ "$report_content" != "$REVIEW_CLEAR" ]]; then
        print_info "Orchestrator reported $(finding_count "$review_file") finding(s)."
    fi

    if [[ "$report_content" == "$REVIEW_CLEAR" ]] || \
        ! has_blocking_findings "$review_file"
    then
        if [[ "$report_content" == "$REVIEW_CLEAR" ]]; then
            print_info "$REVIEW_CLEAR"
        else
            print_info \
                "No blocking findings remain; non-blocking findings are recorded in $review_file."
        fi

        if [[ "$report_content" == "$REVIEW_CLEAR" && clean_passes -gt 0 && \
            final_verification_pending -eq 0 ]]
        then
            clean_passes=$((clean_passes + 1))
            print_info \
                "Review remained clean ($clean_passes/$CLEAN_PASSES)."

            if (( clean_passes >= CLEAN_PASSES )); then
                print_info 'Review reached its clean stability threshold.'
                exit 0
            fi

            if (( cycle == MAX_CYCLES )); then
                report_exhausted "$review_file"
            fi

            cycle=$((cycle + 1))
            continue
        fi

        print_info 'Running final verification.'

        final_log="$RUN_DIR/cycle-${cycle_tag}-final-verification.log"

        if run_final_verification_with_retry "$final_log"; then
            cp "$final_log" "$RUN_DIR/latest-verification.log"
            pending_verification=''
            final_verification_pending=0

            if [[ "$report_content" == "$REVIEW_CLEAR" ]]; then
                clean_passes=1

                if (( clean_passes < CLEAN_PASSES )); then
                    print_info \
                        "Review is clean (1/$CLEAN_PASSES); starting a stability recheck."

                    if (( cycle == MAX_CYCLES )); then
                        report_exhausted "$review_file"
                    fi

                    cycle=$((cycle + 1))
                    continue
                fi

                print_info 'Review reached its clean stability threshold.'
            else
                print_info 'No blocking findings remain and final verification passed.'
            fi
            exit 0
        else
            final_status=$?
        fi

        if (( final_status == EXIT_VERIFICATION_TIMEOUT )); then
            print_warn \
                'Final verification timed out twice; starting a recovery worker without consuming a cycle.'
            print_warn "Verification log: $final_log"
        elif (( final_status == EXIT_VERIFICATION )); then
            print_warn \
                'Final verification was unavailable in the current environment; continuing with worker assessment.'
            print_warn "Verification log: $final_log"
        fi

        copy_latest_verification "$final_log"
        pending_verification="$final_log"
        final_verification_pending=1

        print_info 'Final verification failure is being sent to a worker.'

        worker_artifact='worker'
        if (( final_status == EXIT_VERIFICATION_TIMEOUT )); then
            worker_artifact='timeout-recovery-worker'
        fi

        if ! run_worker_pass \
            "$cycle" \
            "$review_file" \
            "$pending_verification" \
            "$worker_artifact"
        then
            exit "$EXIT_CODEX"
        fi
    else
        print_info "Review is not clean; findings are recorded in $review_file."
        print_info 'Running worker.'

        if ! run_worker_pass \
            "$cycle" \
            "$review_file" \
            "$pending_verification"
        then
            exit "$EXIT_CODEX"
        fi
    fi

    verification_log="$RUN_DIR/cycle-${cycle_tag}-focused-verification.log"

    focused_status=0
    if run_fast_verification_with_retry "$verification_log"; then
        focused_status=0
    else
        focused_status=$?
    fi

    if (( focused_status == EXIT_VERIFICATION_TIMEOUT )); then
        copy_latest_verification "$verification_log"
        pending_verification="$verification_log"
        print_warn \
            'Focused verification timed out twice; starting a recovery worker without consuming a cycle.'
        print_warn "Verification log: $verification_log"

        if ! run_worker_pass \
            "$cycle" \
            "$review_file" \
            "$pending_verification" \
            'timeout-recovery-worker'
        then
            exit "$EXIT_CODEX"
        fi

        print_info 'Re-running focused verification after the timeout recovery worker.'
        if run_fast_verification_with_retry "$verification_log"; then
            focused_status=0
        else
            focused_status=$?
        fi
    fi

    if (( focused_status == 0 )); then
        cp "$verification_log" "$RUN_DIR/latest-verification.log"
        if (( final_verification_pending == 0 )); then
            pending_verification=''
        fi
        repeated_verification_failures=0
        last_verification_signature=''

        print_info 'Focused verification passed.'
    else
        copy_latest_verification "$verification_log"
        if (( final_verification_pending == 0 )); then
            pending_verification="$verification_log"
        fi

        verification_signature="$(verification_failure_signature "$verification_log")"
        if [[ -n "$verification_signature" && \
            "$verification_signature" == "$last_verification_signature" ]]
        then
            repeated_verification_failures=$((repeated_verification_failures + 1))
        elif [[ -n "$verification_signature" ]]; then
            repeated_verification_failures=1
            last_verification_signature="$verification_signature"
        else
            repeated_verification_failures=0
            last_verification_signature=''
        fi

        if (( repeated_verification_failures >= 2 )); then
            print_warn \
                'Focused verification repeated the same failure after two worker passes; continuing autonomously.'
            print_warn "Verification log: $verification_log"
            repeated_verification_failures=0
            last_verification_signature=''
        fi

        print_info \
            'Continuing so a fresh orchestrator and worker can assess the verification failure.'
    fi

    if (( cycle == MAX_CYCLES )); then
        report_exhausted "$review_file"
    fi

    cycle=$((cycle + 1))
done

report_exhausted "$LATEST_ORCHESTRATOR"
