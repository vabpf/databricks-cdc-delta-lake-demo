#!/usr/bin/env python3
"""
Validate the Databricks notebooks in notebooks/ without a workspace
==================================================================

There is no test framework for a repo-only deliverable: the notebooks are never
executed here, so the only thing that *can* be checked locally is that they are
well-formed. This script does exactly that, and nothing more.

It checks the ways a Databricks notebook breaks, in either source format: a
`.py` notebook prefixes every line with `#`, a `.sql` notebook prefixes with
`--`. The header, the cell separator and the `MAGIC` prefix are all chosen from
the file extension, so both formats go through the same structural checks.

1. **Missing header.** The first line must be `# Databricks notebook source`
   (`.py`) or `-- Databricks notebook source` (`.sql`).
2. **Malformed cells.** A cell is a block of lines between `COMMAND ----------`
   separators. A cell that starts with `MAGIC %md` is markdown, and *every*
   non-blank line in it must be prefixed `MAGIC` -- a bare line in the middle
   of a markdown cell is silently swallowed by the parser.
3. **Code that is not Python.** Every non-markdown `.py` cell must pass
   `ast.parse`. SQL cannot be parsed without a warehouse, so a `.sql` notebook
   is checked instead for **`${...}` references that no parameter declares** --
   by `CREATE WIDGET TEXT name DEFAULT 'value'` or `SET name=value`. A typo in a
   parameter name is the one SQL mistake that stays invisible in the source and
   only fails when a reader runs the notebook.

   That check is a guard, not a pattern to copy. On serverless notebook compute
   `SET` of a custom key fails outright (`[CONFIG_NOT_AVAILABLE]`) and `${name}`
   is deprecated, so this repo writes names out in full rather than
   substituting. The check exists so a future `${typo}` cannot slip past review.

It also flags leftover scaffolding markers (the `# COMMANDILE` style placeholders
that are easy to leave behind) and trailing whitespace, which is cosmetic but a
useful signal that a cell was hand-edited.

    python tools/validate_notebooks.py            # check notebooks/
    python tools/validate_notebooks.py path ...   # check specific files
"""

from __future__ import annotations

import ast
import re
import sys
from pathlib import Path

HEADER_PY = "# Databricks notebook source"
HEADER_SQL = "-- Databricks notebook source"
SEPARATOR_PY = "# COMMAND ----------"
SEPARATOR_SQL = "-- COMMAND ----------"
MAGIC_PY = "# MAGIC"
MAGIC_SQL = "-- MAGIC"
SUSPECT_MARKERS = ("COMMANDILE", "COMMAND----")
WIDGET_REF = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")
# A parameter is declared either the SQL-widget way or the session-conf way.
# Neither is recommended inside a notebook -- see the docstring -- so this repo
# spells the names out in full. The check is here so a future ${typo} cannot
# slip past review unnoticed.
DECLARED_PARAM = re.compile(
    r"(?:CREATE\s+WIDGET\s+(?:TEXT|NUMBER|SQL|NUMERIC)\s+([A-Za-z_][A-Za-z0-9_]*)"
    r"|^\s*SET\s+([A-Za-z_][A-Za-z0-9_]*)\s*=)",
    re.IGNORECASE | re.MULTILINE,
)


def style(path: Path) -> tuple[str, str, str, str]:
    """(header, separator, magic, language) for a notebook file."""
    if path.suffix == ".sql":
        return HEADER_SQL, SEPARATOR_SQL, MAGIC_SQL, "sql"
    return HEADER_PY, SEPARATOR_PY, MAGIC_PY, "python"


def cells(text: str, separator: str) -> list[tuple[int, list[str]]]:
    """Split a notebook into (start_line_number, lines) cells."""
    out: list[tuple[int, list[str]]] = []
    start = 1
    current: list[str] = []
    for n, line in enumerate(text.splitlines(), 1):
        if line.strip() == separator:
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
    header, separator, magic, language = style(path)

    lines = text.splitlines()
    if not lines or lines[0].strip() != header:
        problems.append(f"first line is not {header!r}")

    declared: set[str] = set()
    referenced: list[tuple[int, str]] = []

    for start, cell in cells(text, separator):
        stripped = [ln for ln in cell if ln.strip()]
        if not stripped:
            continue

        is_md = stripped[0].startswith(f"{magic} %md") or stripped[0].strip() == magic

        if is_md:
            for i, line in enumerate(cell):
                if line.strip() and not line.lstrip().startswith(magic):
                    problems.append(
                        f"line {start + i}: markdown cell contains a non-MAGIC line: "
                        f"{line[:60]!r}"
                    )
            continue

        # Code cell: SQL lines are kept intact (including their own comments),
        # MAGIC lines dropped, so declarations below are found in place.
        code = "\n".join(
            ln for ln in cell if not ln.lstrip().startswith(magic)
        )
        if not code.strip():
            continue

        if language == "sql":
            declared |= {n for pair in DECLARED_PARAM.findall(code) for n in pair if n}
            for i, line in enumerate(cell):
                if line.lstrip().startswith(magic):
                    continue
                for name in WIDGET_REF.findall(line):
                    referenced.append((start + i, name))
            continue

        try:
            ast.parse(code)
        except SyntaxError as exc:
            problems.append(f"line {start + (exc.lineno or 1) - 1}: not valid Python: {exc.msg}")

    for n, name in referenced:
        if name not in declared:
            problems.append(
                f"line {n}: ${{{name}}} is referenced but never declared "
                f"by a CREATE WIDGET or SET statement"
            )

    for n, line in enumerate(lines, 1):
        for marker in SUSPECT_MARKERS:
            if marker in line:
                problems.append(f"line {n}: leftover marker {marker!r}: {line.strip()[:60]!r}")
        if line.rstrip() != line:
            problems.append(f"line {n}: trailing whitespace")

    return problems


def main(argv: list[str]) -> int:
    targets = [Path(a) for a in argv[1:]] or sorted(
        Path("notebooks").glob("*.py")
    ) + sorted(Path("notebooks").glob("*.sql"))
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
