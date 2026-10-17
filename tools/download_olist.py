#!/usr/bin/env python3
"""
Download the Olist "Brazilian E-Commerce" dataset from Kaggle
============================================================

The pipeline needs the *contents* of Kaggle dataset
`olistbr/brazilian-ecommerce`: nine CSV files. This script fetches them into a
directory that tools/generate_cdc.py can read.

Why this is a local step and not something the notebook does
------------------------------------------------------------
Databricks Free Edition restricts outbound network egress to an allowlist that
does not include kaggle.com, so a notebook literally cannot download the data.
That is fine -- and arguably better: the landing zone becomes a versioned,
reviewable artifact instead of a network dependency, and the demo runs on a
volume you control.

Prerequisites
-------------
Either:

  * `pip install kagglehub` and a Kaggle API token at ~/.kaggle/kaggle.json
    (Kaggle account -> Settings -> Create New API Token), or
  * the classic Kaggle CLI: `pip install kaggle`, or
  * no tooling at all -- download the dataset from
    https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
    and unzip it yourself, then point --out at the folder.

Usage
-----
    python tools/download_olist.py                     # -> data/olist
    python tools/download_olist.py --out data/olist
"""

from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

# The nine files the generator expects. Everything else in the Kaggle zip is
# ignored; these are the whole of the source system we model.
EXPECTED = (
    "olist_customers_dataset.csv",
    "olist_geolocation_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_order_payments_dataset.csv",
    "olist_order_reviews_dataset.csv",
    "olist_orders_dataset.csv",
    "olist_products_dataset.csv",
    "olist_sellers_dataset.csv",
    "product_category_name_translation.csv",
)

KAGGLE_SLUG = "olistbr/brazilian-ecommerce"


def report(out: Path) -> bool:
    """Print what is present. Returns True when the directory is complete."""
    have = {p.name for p in out.glob("*.csv")}
    missing = [f for f in EXPECTED if f not in have]
    for f in EXPECTED:
        mark = "ok " if f in have else "MISSING"
        print(f"  [{mark}] {f}")
        _ = mark
    if missing:
        print(f"\n{len(missing)} file(s) missing from {out}")
        return False
    print(f"\nAll {len(EXPECTED)} source files present in {out}")
    return True


def download_with_kagglehub(out: Path) -> bool:
    try:
        import kagglehub  # noqa: PLC0415
    except ImportError:
        return False

    print(f"Downloading {KAGGLE_SLUG} with kagglehub ...")
    try:
        cached = Path(kagglehub.dataset_download(KAGGLE_SLUG))
    except Exception as exc:  # noqa: BLE001 - surface whatever Kaggle said
        print(f"kagglehub could not download the dataset:\n  {exc}", file=sys.stderr)
        print(
            "\nThis is almost always missing credentials. Create an API token at\n"
            "  https://www.kaggle.com/settings  (Create New API Token)\n"
            "and place it at ~/.kaggle/kaggle.json, or fall back to the manual\n"
            "download described in this file's docstring.",
            file=sys.stderr,
        )
        return False

    print(f"kagglehub cache: {cached}")
    out.mkdir(parents=True, exist_ok=True)
    copied = 0
    # The cache directory nests the files under versions/<n>/; copy every CSV
    # we find rather than assuming a layout.
    for src in cached.rglob("*.csv"):
        shutil.copy2(src, out / src.name)
        copied += 1
    print(f"copied {copied} CSV file(s) into {out}")
    return copied > 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--out", type=Path, default=Path("data/olist"),
                    help="directory to place the Olist CSVs in")
    ap.add_argument("--check", action="store_true",
                    help="only report what is already present; do not download")
    args = ap.parse_args(argv)

    if args.check:
        return 0 if report(args.out) else 1

    if not args.out.exists() or not report(args.out):
        if download_with_kagglehub(args.out):
            if not report(args.out):
                return 1
        elif args.out.exists() and any(args.out.glob("*.csv")):
            # Some files are already there and kagglehub is unavailable: the
            # user is probably doing this by hand. Say what is still missing.
            return 1
        else:
            print(
                f"\nNothing downloaded. Place the Olist CSVs in {args.out} manually:\n"
                f"  1. open https://www.kaggle.com/datasets/{KAGGLE_SLUG}\n"
                "  2. download and unzip the archive\n"
                f"  3. move the CSVs into {args.out}\n"
                f"  4. re-run: python tools/download_olist.py --check",
                file=sys.stderr,
            )
            return 1

    print("\nNext: generate the CDC stream\n"
          f"  python tools/generate_cdc.py --olist-dir {args.out} --out data/landing")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
