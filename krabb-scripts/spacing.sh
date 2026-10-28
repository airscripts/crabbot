#!/usr/bin/env bash
set -euo pipefail

usage() {
    printf '%s\n' 'Usage: krabb-scripts/spacing.sh [--check]'
    printf '%s\n' 'Add or check code spacing in Rust, JavaScript, and Python files.'
}

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
check_only=false

if (($# > 1)); then
    usage >&2
    exit 2
elif (($# == 1)); then
    case "$1" in
        --check)
            check_only=true
            ;;

        --help|-h)
            usage
            exit 0
            ;;

        *)
            usage >&2
            exit 2
            ;;
    esac
fi

trim_line() {
    local value=$1
    value=${value#"${value%%[![:space:]]*}"}
    printf '%s' "$value"
}

line_indent() {
    local value=$1
    local trimmed=${value#"${value%%[![:space:]]*}"}
    printf '%d' "$(( ${#value} - ${#trimmed} ))"
}

is_continuation() {
    local value=$1
    [[ -z "$value" || "$value" == else* || "$value" == \;* || "$value" == ,* \
        || "$value" == \)* || "$value" == \]* || "$value" == .* \
        || "$value" == \?* || "$value" == \}* || "$value" == \{* \
        || "$value" == '=>'* ]]
}

is_closing_block() {
    local value=$1
    [[ "$value" == \}* ]] || return 1
    [[ "$value" == *"}" || "$value" == *"};" || "$value" == *"});" ]]
}

is_inline_block() {
    local value=$1
    [[ "$value" == *"};" || "$value" == *"});" ]] || return 1
    [[ "$value" =~ (else|unsafe|async[[:space:]]+move|move[[:space:]]*\|)[[:space:]]*\{ ]]
}

is_control() {
    [[ "$1" =~ ^(if|for|while|loop|match)([[:space:]]|$) ]]
}

is_match_block() {
    local pattern='=>[[:space:]]*\{'
    [[ "$1" =~ $pattern ]]
}

is_assertion() {
    [[ "$1" =~ ^(debug_)?assert(_(eq|ne))?! ]]
}

is_multiline_start() {
    local value=$1
    local function_signature='^\)[[:space:]]*->[^{]*\{[[:space:]]*$'
    [[ "$value" == *\; ]] && return 1
    [[ "$value" == //* || "$value" == \#* || "$value" == \}* ]] && return 1
    [[ "$value" =~ ^(if|for|while|loop|match|else)([[:space:]]|$) ]] && return 1
    [[ "$value" =~ ^(pub[[:space:]]+)?(impl|struct|enum|trait|mod)([[:space:]]|$) ]] && return 1
    is_match_block "$value" && return 1
    [[ "$value" =~ ^(pub[[:space:]]+)?(async[[:space:]]+)?fn[[:space:]] ]] && return 1
    [[ "$value" =~ $function_signature ]] && return 1
    if is_assertion "$value" || [[ "$value" =~ ^(let|const|static|return|break|continue)([[:space:]]|$) ]]; then
        [[ "$value" != *\; && "$value" == *"("* ]] && return 0
    fi
    [[ "$value" == *'(' || "$value" == *'{' || "$value" == *'=' \
        || "$value" == *'.' || "$value" =~ ^(let|const|static|return|break|continue)([[:space:]]|$) ]]
}

append_blank() {
    local -n output=$1
    ((${#output[@]} == 0)) && return
    if [[ -n "${output[${#output[@]} - 1]}" ]]; then
        output+=("")
    fi
}

scan_delimiters() {
    local value=$1
    local -n state_ref=$2
    local character
    local index
    local next_index
    local expected_hashes=''
    local actual_hashes=''
    local escaped=false
    local quote=
    paren_delta=0
    bracket_delta=0
    brace_delta=0

    for ((index = 0; index < ${#value}; index++)); do
        character=${value:index:1}

        if ((state_ref >= 0)); then
            if [[ "$character" == '"' ]]; then
                expected_hashes=''
                for ((next_index = 0; next_index < state_ref; next_index++)); do
                    expected_hashes+='#'
                done
                actual_hashes=${value:index+1:state_ref}
                if [[ "$actual_hashes" == "$expected_hashes" ]]; then
                    state_ref=-1
                    break
                fi
            fi
            continue
        fi

        if [[ -n "$quote" ]]; then
            if $escaped; then
                escaped=false
            elif [[ "$character" == '\\' ]]; then
                escaped=true
            elif [[ "$character" == "$quote" ]]; then
                quote=
            fi
            continue
        fi

        case "$character" in
            '"')
                quote=$character
                ;;

            r)
                next_index=$((index + 1))
                while [[ ${value:next_index:1} == '#' ]]; do
                    next_index=$((next_index + 1))
                done
                if [[ ${value:next_index:1} == '"' ]]; then
                    state_ref=$((next_index - index - 1))
                    index=$next_index
                fi
                ;;

            '(')
                paren_delta=$((paren_delta + 1))
                ;;

            ')')
                paren_delta=$((paren_delta - 1))
                ;;

            '[')
                bracket_delta=$((bracket_delta + 1))
                ;;

            ']')
                bracket_delta=$((bracket_delta - 1))
                ;;

            '{')
                brace_delta=$((brace_delta + 1))
                ;;

            '}')
                brace_delta=$((brace_delta - 1))
                ;;

            '/')
                if [[ ${value:index+1:1} == '/' ]]; then
                    break
                fi
                ;;
        esac
    done
}

format_file() {
    local path=$1
    local -a lines formatted
    local line trimmed previous previous_trimmed
    local leading
    local current_leading previous_leading
    local previous_index current_indent previous_indent
    local needs_blank pending_blank=false
    local multiline=false multiline_lines=0
    local multiline_indent=0
    local chain=false chain_lines=0
    local paren_depth=0 bracket_depth=0 brace_depth=0
    local raw_state=-1
    local raw_line=false
    local temporary

    formatted=()
    mapfile -t lines < "$path"

    for line in "${lines[@]}"; do
        trimmed=${line#"${line%%[![:space:]]*}"}
        leading=${line%%[![:space:]]*}
        current_indent=${#leading}
        raw_line=false
        if ((raw_state >= 0)); then
            raw_line=true
        fi

        if ! $raw_line && $pending_blank; then
            if [[ -n "$trimmed" ]] && ! is_continuation "$trimmed"; then
                append_blank formatted
            fi
            pending_blank=false
        fi

        if ! $raw_line && [[ -n "$trimmed" ]]; then
            previous_index=$((${#formatted[@]} - 1))
            while ((previous_index >= 0)) && [[ -z "${formatted[previous_index]}" ]]; do
                previous_index=$((previous_index - 1))
            done

            if ((previous_index >= 0)); then
                previous=${formatted[previous_index]}
                previous_trimmed=${previous#"${previous%%[![:space:]]*}"}
                current_leading=${line%%[![:space:]]*}
                previous_leading=${previous%%[![:space:]]*}
                current_indent=${#current_leading}
                previous_indent=${#previous_leading}
                needs_blank=false

                if is_control "$trimmed" \
                    && [[ "$previous_trimmed" != \#* && "$previous_trimmed" != //* ]] \
                    && [[ "$previous_trimmed" != *\{ && "$previous_trimmed" != *"=>" \
                        && "$previous_trimmed" != *, && "$previous_trimmed" != *\( \
                        && "$previous_trimmed" != *\[ && "$previous_trimmed" != *. \
                        && "$previous_trimmed" != *= ]] \
                    && ((current_indent <= previous_indent)); then
                    needs_blank=true
                fi

                if is_match_block "$trimmed" \
                    && [[ "$previous_trimmed" != //* && "$previous_trimmed" != *\{ ]]; then
                    needs_blank=true
                fi

                if is_closing_block "$previous_trimmed" \
                    && ((current_indent <= previous_indent)) \
                    && { ! is_continuation "$trimmed" || [[ "$trimmed" == //* ]] \
                        || [[ "$trimmed" == \#* && current_indent -gt 0 ]]; }; then
                    needs_blank=true
                fi

                if is_inline_block "$previous_trimmed" && ((current_indent <= previous_indent)); then
                    needs_blank=true
                fi

                if is_assertion "$trimmed" && ! is_assertion "$previous_trimmed" \
                    && ((current_indent == previous_indent)); then
                    needs_blank=true
                fi

                if $needs_blank; then
                    append_blank formatted
                fi
            fi
        fi

        formatted+=("$line")

        if $multiline; then
            multiline_lines=$((multiline_lines + 1))
            scan_delimiters "$line" raw_state
            paren_depth=$((paren_depth + paren_delta))
            bracket_depth=$((bracket_depth + bracket_delta))
            brace_depth=$((brace_depth + brace_delta))

            if [[ "$trimmed" == *\; ]] \
                && ((paren_depth <= 0 && bracket_depth <= 0)) \
                && ((brace_depth <= 0 || current_indent <= multiline_indent)); then
                multiline=false
                if ((multiline_lines > 1)); then
                    pending_blank=true
                fi
            fi
        elif $chain; then
            chain_lines=$((chain_lines + 1))
            if [[ "$trimmed" == *\; ]]; then
                chain=false
                if ((chain_lines > 1)); then
                    pending_blank=true
                fi
            fi
        elif [[ "$trimmed" == .* ]]; then
            chain=true
            chain_lines=1
        elif is_multiline_start "$trimmed"; then
            multiline=true
            multiline_lines=1
            multiline_indent=$current_indent
            paren_depth=0
            bracket_depth=0
            brace_depth=0
            scan_delimiters "$line" raw_state
            paren_depth=$paren_delta
            bracket_depth=$bracket_delta
            brace_depth=$brace_delta
        fi
    done

    temporary=$(mktemp "${path}.spacing.XXXXXX")
    cp -p "$path" "$temporary"
    printf '%s\n' "${formatted[@]}" > "$temporary"

    if cmp -s "$path" "$temporary"; then
        rm -f "$temporary"
        return 0
    fi

    if $check_only; then
        printf '[WARN] Rust spacing needs formatting: %s\n' "${path#"$repo_root/"}" >&2
        rm -f "$temporary"
        return 1
    fi

    mv "$temporary" "$path"
}

status=0
while IFS= read -r -d '' path; do
    if ! format_file "$path"; then
        status=1
    fi
done < <(
    find "$repo_root" -type f -name '*.rs' \
        -not -path "$repo_root/target/*" \
        -not -path "$repo_root/krabb-ci/.cache/*" \
        -not -path "$repo_root/krabb-ci/.target/*" \
        -print0
)

python_status=0
python3 - "$repo_root" "$check_only" <<'PY' || python_status=$?
import argparse
import ast
import re
import sys
import tokenize
from io import StringIO
from pathlib import Path


ROOT = Path(sys.argv[1])
CHECK_ONLY = sys.argv[2] == "true"
IGNORED = {"node_modules", "target", ".git"}
LINE_LIMIT = 100
BLOCK_END = re.compile(r"^\}(?:\)|;|\);|\s*;)?$")
CONTINUATION = re.compile(r"^(?:\}|else\b|catch\b|finally\b|[.,;\)\]])")


def javascript_masks(lines):
    masks = []
    template_lines = []
    quote = None
    in_template = False
    in_block_comment = False

    for line in lines:
        mask = []
        started_in_template = in_template
        index = 0

        while index < len(line):
            char = line[index]
            following = line[index + 1] if index + 1 < len(line) else ""

            if in_template:
                if char == "\\":
                    index += 2
                    continue

                if char == "`":
                    in_template = False

                index += 1
                continue

            if quote:
                if char == "\\":
                    index += 2
                    continue

                if char == quote:
                    quote = None

                index += 1
                continue

            if in_block_comment:
                if char == "*" and following == "/":
                    in_block_comment = False
                    index += 2
                else:
                    index += 1

                continue

            if char == "/" and following == "/":
                break

            if char == "/" and following == "*":
                in_block_comment = True
                index += 2
                continue

            if char in "'\"":
                quote = char
                index += 1
                continue

            if char == "`":
                in_template = True
                index += 1
                continue

            mask.append(char)
            index += 1

        masks.append("".join(mask))
        template_lines.append(started_in_template)

        if quote:
            quote = None

    return masks, template_lines


def format_javascript(source):
    lines = source.splitlines()
    masks, template_lines = javascript_masks(lines)

    for number, line in enumerate(lines, 1):
        if len(line) > LINE_LIMIT:
            raise ValueError(f"line {number} exceeds the {LINE_LIMIT}-column limit.")

    for line, in_template in zip(lines, template_lines, strict=True):
        if line.strip() and not in_template:
            leading = len(line) - len(line.lstrip(" "))

            if "\t" in line[:leading]:
                raise ValueError("JavaScript indentation must use spaces.")

            if leading % 4:
                raise ValueError("JavaScript indentation must use four spaces per level.")

    output = []

    for index, line in enumerate(lines):
        output.append(line)

        if line.strip() and masks[index].strip() and BLOCK_END.fullmatch(masks[index].strip()):
            next_index = index + 1

            if next_index < len(lines):
                next_code = masks[next_index].strip()
                next_line = lines[next_index]
                continued = bool(CONTINUATION.match(next_code))

                if next_line.strip() and next_code and not continued:
                    if output and output[-1].strip():
                        output.append("")

    return "\n".join(output) + "\n"


def is_block_statement(statement):
    return isinstance(
        statement,
        (
            ast.AsyncFor,
            ast.AsyncFunctionDef,
            ast.AsyncWith,
            ast.ClassDef,
            ast.For,
            ast.FunctionDef,
            ast.If,
            ast.Match,
            ast.Try,
            ast.While,
            ast.With,
        ),
    )


def format_python(source):
    tree = ast.parse(source)
    lines = source.splitlines()
    boundaries = {}

    for number, line in enumerate(lines, 1):
        if len(line) > LINE_LIMIT:
            raise ValueError(f"line {number} exceeds the {LINE_LIMIT}-column limit.")

    def visit(node):
        for value in vars(node).values():
            if isinstance(value, list):
                statements = [item for item in value if isinstance(item, ast.stmt)]
                top_level = isinstance(node, ast.Module)

                for previous, current in zip(statements, statements[1:]):
                    if not is_block_statement(previous) and not is_block_statement(current):
                        continue

                    definitions = (ast.AsyncFunctionDef, ast.ClassDef, ast.FunctionDef)
                    blank_lines = 2 if top_level and any(
                        isinstance(statement, definitions) for statement in (previous, current)
                    ) else 1

                    after_line = previous.end_lineno or previous.lineno
                    boundaries[after_line] = max(boundaries.get(after_line, 0), blank_lines)

                for item in value:
                    if isinstance(item, ast.AST):
                        visit(item)

            elif isinstance(value, ast.AST):
                visit(value)

    visit(tree)

    for token in tokenize.generate_tokens(StringIO(source).readline):
        if token.type == tokenize.INDENT:
            if "\t" in token.string or len(token.string) % 4:
                raise ValueError("Python indentation must use four spaces per level.")

    for after_line, blank_lines in sorted(boundaries.items(), reverse=True):
        index = after_line
        existing = 0

        while index + existing < len(lines) and not lines[index + existing].strip():
            existing += 1

        lines[index : index + existing] = [""] * blank_lines

    return "\n".join(lines).rstrip() + "\n"


status = 0
paths = sorted(
    path
    for suffix in ("*.mjs", "*.py")
    for path in ROOT.rglob(suffix)
    if not any(part in IGNORED for part in path.relative_to(ROOT).parts)
)

for path in paths:
    original = path.read_text()

    try:
        formatted = format_javascript(original) if path.suffix == ".mjs" else format_python(original)
    except (SyntaxError, tokenize.TokenError, ValueError) as error:
        print(f"[ERROR] {path.relative_to(ROOT)}: {error}", file=sys.stderr)
        status = 1
        continue

    if formatted == original:
        continue

    if CHECK_ONLY:
        print(f"[WARN] Code spacing needs formatting: {path.relative_to(ROOT)}", file=sys.stderr)
        status = 1
    else:
        path.write_text(formatted)
        print(f"[FIXED] {path.relative_to(ROOT)}")

raise SystemExit(status)
PY

if ((python_status != 0)); then
    status=1
fi

exit "$status"
