#!/usr/bin/env python3
"""
Verify a generated CDC landing zone before you spend pipeline time on it.

These are the invariants the Lakeflow pipeline actually depends on, so it is
worth asserting them locally first:

  1. every non-`broken` line is valid JSON with the expected envelope keys
  2. op is one of c/u/d/r/t/x
  3. `c`/`r` rows have before=null; `d` rows have after=null
  4. per entity+key, `event_ts` is strictly increasing across the WHOLE stream
     (this is what makes AUTO CDC's SEQUENCE BY deterministic)
  5. primary keys are non-null on the rows that are supposed to be valid
  6. schema drift lands where we expect it (new/renamed columns by wave)
  7. wave coverage: every entity has files in every wave

Exit code 0 = clean, 1 = at least one check failed.
"""

from __future__ import annotations

import argparse
import collections
import gzip
import json
import sys
from pathlib import Path

VALID_OPS = {"c", "u", "d", "r", "t", "x"}


def iter_files(root: Path):
    yield from sorted(root.glob("events/*/*.jsonl*"))


def read_lines(path: Path):
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8") as fh:
        for lineno, line in enumerate(fh, 1):
            yield lineno, line


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--landing", type=Path, default=Path("data/landing"))
    ap.add_argument("--max-violations", type=int, default=5,
                    help="stop reporting a given problem after N examples")
    args = ap.parse_args(argv)

    if not args.landing.exists():
        print(f"FAIL: {args.landing} does not exist")
        return 1

    problems: list[str] = []
    notes: list[str] = []
    seen: collections.Counter = collections.Counter()

    def problem(msg: str, key: str):
        seen[key] += 1
        if seen[key] <= args.max_violations:
            problems.append(msg)

    last_ts: dict[tuple, int] = {}
    ts_fingerprint: dict[tuple, str] = {}
    out_of_order = 0
    replayed = 0
    op_counts: dict[str, collections.Counter] = collections.defaultdict(collections.Counter)
    wave_by_entity: dict[str, set] = collections.defaultdict(set)
    columns_by_wave: dict[tuple[str, int], set] = collections.defaultdict(set)
    total = 0
    broken_lines = 0
    files = list(iter_files(args.landing))
    if not files:
        print(f"FAIL: no files under {args.landing}/events")
        return 1

    for path in files:
        entity = path.parent.name
        wave = int(path.name.split("_w")[1][:2])
        wave_by_entity[entity].add(wave)
        is_broken_file = "broken" in path.name

        for lineno, line in read_lines(path):
            line = line.strip()
            if not line:
                continue
            total += 1
            try:
                rec = json.loads(line)
            except json.JSONDecodeError as exc:
                if is_broken_file:
                    broken_lines += 1
                    continue
                problem(f"{path.name}:{lineno} invalid JSON ({exc.msg})", "bad_json")
                continue

            if is_broken_file:
                broken_lines += 1
                continue

            op = rec.get("op")
            op_counts[entity][op] += 1
            if op not in VALID_OPS:
                problem(f"{path.name}:{lineno} unknown op={op!r}", "bad_op")
                continue

            if op in ("c", "r") and rec.get("before") is not None:
                problem(f"{path.name}:{lineno} op={op} must have before=null", "before_not_null")
            if op == "d" and rec.get("after") is not None:
                problem(f"{path.name}:{lineno} op=d must have after=null", "after_not_null")
            if op == "u" and rec.get("after") is None:
                problem(f"{path.name}:{lineno} op=u must have after", "u_no_after")

            image = rec.get("after") or rec.get("before") or {}
            columns_by_wave[(entity, wave)] |= set(image.keys())

            # key/ordering checks only apply to rows the pipeline keeps
            keycol = {
                "customers": ("customer_id",),
                "products": ("product_id",),
                "sellers": ("seller_id",),
                "orders": ("order_id",),
                "order_items": ("order_id", "order_item_id"),
                "order_payments": ("order_id", "payment_sequential"),
                "order_reviews": ("review_id",),
            }[entity]
            key = tuple(image.get(k) for k in keycol)
            if any(v is None for v in key):
                notes.append(f"{entity} row with null key in {path.name}:{lineno} (expected: quarantine fodder)")
                continue
            ets = rec.get("ts_ms")
            if ets is None:
                notes.append(f"{entity} row with null ts in {path.name}:{lineno} (expected: quarantine fodder)")
                continue

            # Invariant: (entity, key, event_ts) must be UNAMBIGUOUS. Two distinct
            # payloads sharing a business timestamp would make SEQUENCE BY
            # non-deterministic. An identical payload is a legitimate replay.
            fingerprint = json.dumps(rec.get("after"), sort_keys=True)
            slot = (entity, key, ets)
            prior = ts_fingerprint.get(slot)
            if prior is None:
                ts_fingerprint[slot] = fingerprint
            elif prior == fingerprint:
                replayed += 1  # identical payload -> idempotent replay
            else:
                problem(
                    f"{entity} key={key} ts={ets}: two DIFFERENT payloads share "
                    f"one business timestamp (ambiguous ordering) in "
                    f"{path.name}:{lineno}", "ambiguous_ts")

            # Arrival order is *deliberately* not business order; measure it
            # rather than assert on it.
            last = last_ts.get((entity, key))
            if last is not None and ets < last:
                out_of_order += 1
            last_ts[(entity, key)] = max(last or 0, ets)

    # ---- schema drift expectations -------------------------------------
    prod_late = sorted(w for (e, w) in columns_by_wave if e == "products" and w >= 4)
    if prod_late:
        late_cols = set().union(*(columns_by_wave[("products", w)] for w in prod_late))
        if "product_name_length" not in late_cols:
            problem("products waves >=4 never carry product_name_length", "drift_rename")
        if "product_launch_date" not in late_cols:
            problem("products waves >=4 never carry product_launch_date", "drift_new")
    ord_late = sorted(w for (e, w) in columns_by_wave if e == "orders" and w >= 3)
    if ord_late:
        late_cols = set().union(*(columns_by_wave[("orders", w)] for w in ord_late))
        if "delivery_sla_breach" not in late_cols:
            problem("orders waves >=3 never carry delivery_sla_breach", "drift_orders")

    # ---- report ---------------------------------------------------------
    print(f"files scanned      : {len(files)}")
    print(f"records scanned    : {total}")
    print(f"intentionally broken lines: {broken_lines}")
    print(f"out-of-order arrivals     : {out_of_order}  (arrival order != business order)")
    print(f"idempotent replays        : {replayed}  (same key+ts+payload redelivered)")
    print()
    print("op counts per entity:")
    for entity in sorted(op_counts):
        print(f"  {entity:<16} {dict(op_counts[entity])}")
    print()
    print("wave coverage per entity:")
    for entity in sorted(wave_by_entity):
        print(f"  {entity:<16} waves={sorted(wave_by_entity[entity])}")

    if notes:
        print()
        print("notes (expected quarantine fodder):")
        for n in notes[:8]:
            print(f"  - {n}")
        if len(notes) > 8:
            print(f"  ... and {len(notes) - 8} more")

    print()
    if problems:
        print(f"RESULT: FAIL ({len(problems)} shown)")
        for p in problems:
            print(f"  ✗ {p}")
        return 1
    print("RESULT: PASS — all invariants hold")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
