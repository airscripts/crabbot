#!/usr/bin/env python3
"""Format or check indentation and spacing around JavaScript and Python blocks."""

from __future__ import annotations

import argparse
import ast
import re
import sys
import tokenize
from io import StringIO
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
IGNORED = {"node_modules", "target", ".git"}
LINE_LIMIT = 100
BLOCK_END = re.compile(r"^\}(?:\)|;|\);|\s*;)?$")
CONTINUATION = re.compile(r"^(?:\}|else\b|catch\b|finally\b|[.,;\)\]])")


def javascript_masks(lines: list[str]) -> tuple[list[str], list[bool]]:
    masks: list[str] = []
    template_lines: list[bool] = []
    quote: str | None = None
    in_template = False
    in_block_comment = False

    for line in lines:
        mask: list[str] = []
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


def block_ends(line: str) -> bool:
    value = line.strip()

    return bool(BLOCK_END.fullmatch(value))


def continuation(line: str) -> bool:
    return bool(CONTINUATION.match(line.strip()))


def format_lines(source: str) -> str:
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

    output: list[str] = []

    for index, line in enumerate(lines):
        output.append(line)

        if line.strip() and masks[index].strip() and block_ends(masks[index]):
            next_index = index + 1

            if next_index < len(lines):
                next_code = masks[next_index].strip()
                next_line = lines[next_index]

                if next_line.strip() and next_code and not continuation(next_code):
                    if output and output[-1].strip():
                        output.append("")

    return "\n".join(output) + "\n"


def is_block_statement(statement: ast.stmt) -> bool:
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


def format_python(source: str) -> str:
    tree = ast.parse(source)
    lines = source.splitlines()
    boundaries: dict[int, int] = {}

    for number, line in enumerate(lines, 1):
        if len(line) > LINE_LIMIT:
            raise ValueError(f"line {number} exceeds the {LINE_LIMIT}-column limit.")

    def visit(node: ast.AST) -> None:
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


def files() -> list[Path]:
    return sorted(
        path
        for suffix in ("*.mjs", "*.py")
        for path in ROOT.rglob(suffix)
        if not any(part in IGNORED for part in path.relative_to(ROOT).parts)
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--check", action="store_true", help="report files that need spacing")
    mode.add_argument("--fix", action="store_true", help="apply indentation and spacing")
    args = parser.parse_args()

    failed = False

    for path in files():
        original = path.read_text()

        try:
            if path.suffix == ".mjs":
                formatted = format_lines(original)
            else:
                formatted = format_python(original)
        except ValueError as error:
            print(f"[ERROR] {path.relative_to(ROOT)}: {error}", file=sys.stderr)
            failed = True
            continue

        if formatted == original:
            continue

        if args.fix:
            path.write_text(formatted)
            print(f"[FIXED] {path.relative_to(ROOT)}")
        else:
            print(
                f"[WARN] Code spacing needs formatting: {path.relative_to(ROOT)}",
                file=sys.stderr,
            )
            failed = True

    return int(failed)


if __name__ == "__main__":
    raise SystemExit(main())
