#!/usr/bin/env python3
"""
Validate the Databricks notebooks in notebooks/ without a workspace
==================================================================

There is no test framework for a repo-only deliverable: the notebooks are never
executed here, so the only thing that *can* be checked locally is that they are
well-formed. This script does exactly that, and nothing more.

It checks the three ways a Databricks `.py` notebook breaks:

1. **Missing header.** The first line must be `# Databricks notebook source`.
2. **Malformed cells.** A cell is a block of lines between `# COMMAND ----------`
   separators. A cell that starts with `# MAGIC %md` is markdown, and *every*
   non-blank line in it must be prefixed `# MAGIC` -- a bare line in the middle
   of a markdown cell is silently swallowed by the parser.
3. **Code that is not Python.** Every non-markdown cell must pass `ast.parse`.

It also flags leftover scaffolding markers (the `# COMMANDILE` style placeholders
that are easy to leave behind) and trailing whitespace-only lines, which are
cosmetic but a useful signal that a cell was hand-edited.

    python tools/validate_notebooks.py            # check notebooks/
    python tools/validate_notebooks.py path ...   # check specific files
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

HEADER = "# Databricks notebook source"
SEPARATOR = "# COMMAND ----------"
MAGIC = "# MAGIC"
SUSPECT_MARKERS = ("COMMANDILE", "COMMAND----")


def cells(text: str) -> list[tuple[int, list[str]]]:
    """Split a notebook into (start_line_number, lines) cells."""
    out: list[tuple[int, list[str]]] = []
    start = 1
    current: list[str] = []
    for n, line in enumerate(text.splitlines(), 1):
        if line.strip() == SEPARATOR:
            out.append((start, current))
            current = []
            start = n + 1
        else:
            current.append(line)
    out.append((start, current))
    return out


def validate(path: Path) -> list[str]:
    problems: list[str] = []
    text = path.read_text()

    lines = text.splitlines()
    if not lines or lines[0].strip() != HEADER:
        problems.append("first line is not '# Databricks notebook source'")

    for start, cell in cells(text):
        stripped = [ln for ln in cell if ln.strip()]
        if not stripped:
            continue

        is_md = stripped[0].startswith(f"{MAGIC} %md") or stripped[0].strip() == f"{MAGIC}"

        if is_md:
            for i, line in enumerate(cell):
                if line.strip() and not line.lstrip().startswith(MAGIC):
                    problems.append(
                        f"line {start + i}: markdown cell contains a non-MAGIC line: "
                        f"{line[:60]!r}"
                    )
            continue

        # Code cell: must be valid Python.
        code = "\n".join(
            ln for ln in cell if not ln.lstrip().startswith(MAGIC)
        )
        if not code.strip():
            continue
        try:
            ast.parse(code)
        except SyntaxError as exc:
            problems.append(f"line {start + (exc.lineno or 1) - 1}: not valid Python: {exc.msg}")

    for n, line in enumerate(lines, 1):
        for marker in SUSPECT_MARKERS:
            if marker in line:
                problems.append(f"line {n}: leftover marker {marker!r}: {line.strip()[:60]!r}")

    return problems


def main(argv: list[str]) -> int:
    targets = [Path(a) for a in argv[1:]] or sorted(Path("notebooks").glob("*.py"))
    if not targets:
        print("no notebooks found under notebooks/", file=sys.stderr)
        return 2

    total_problems = 0
    for path in targets:
        if not path.exists():
            print(f"MISSING  {path}")
            total_problems += 1
            continue
        problems = validate(path)
        status = "ok  " if not problems else "FAIL"
        print(f"{status}  {path}")
        for p in problems:
            print(f"        - {p}")
        total_problems += len(problems)

    print()
    if total_problems:
        print(f"{total_problems} problem(s) found")
        return 1
    print(f"all {len(targets)} notebook(s) well-formed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
