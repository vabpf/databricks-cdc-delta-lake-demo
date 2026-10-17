#!/usr/bin/env python3
"""
Upload the generated CDC landing zone into a Unity Catalog volume, wave by wave
==============================================================================

The point of this script is the *sequence*. Upload wave 1, run the pipeline,
then upload wave 2 and run it again. Each run ingests only the new files,
because Auto Loader tracks what it has already processed transactionally. That
is what makes the demo a real incremental-ingestion story rather than a one-shot
load: late arrivals land in later waves, duplicates are redelivered, and the
schema only drifts once you get to wave 3.

    python tools/upload_landing.py --wave 1      # then run the pipeline
    python tools/upload_landing.py --wave 2      # then run it again
    ...
    python tools/upload_landing.py --all         # or just do the lot

Unity Catalog volumes are the only supported landing zone on Free Edition --
there is no DBFS root to write to.

Layout written (matches what the pipeline's `retail_landing` config points at):

    <volume>/events/<entity>/<entity>_w<NN>_p<PPP>.jsonl.gz
    <volume>/reference/geolocation_by_zip.csv
    <volume>/_manifest.json
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

# Defaults match databricks.yml. Override with --volume if your layout differs.
DEFAULT_VOLUME = "/Volumes/workspace/retail_landing/cdc"
DEFAULT_LOCAL = Path("data/landing")


def load_manifest(local: Path) -> dict:
    path = local / "_manifest.json"
    if not path.exists():
        raise SystemExit(
            f"{path} not found.\n"
            "Generate the landing zone first:\n"
            "  python tools/generate_cdc.py --olist-dir data/olist --out data/landing"
        )
    return json.loads(path.read_text())


def available_waves(manifest: dict) -> list[int]:
    waves: set[int] = set()
    for stats in manifest.get("entities", {}).values():
        for w in stats.get("by_wave", {}):
            waves.add(int(w))
    return sorted(waves)


def event_files_for_wave(local: Path, wave: int) -> list[Path]:
    """Every landing file belonging to one delivery wave.

    Wave is carried in the filename (`..._w03_p001.jsonl.gz`) rather than in a
    directory, so that a wave is a property of the *file*: appending one file
    is one delivery, and Auto Loader needs no extra bookkeeping to see it.
    """
    token = f"_w{wave:02d}_"
    return sorted(
        p for p in local.glob("events/*/*.jsonl*") if token in p.name
    )


def upload(client, local_path: Path, remote_path: str, retries: int = 3) -> None:
    """Upload one file, retrying transient failures."""
    last: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            with open(local_path, "rb") as fh:
                client.files.upload(
                    file_path=remote_path, contents=fh, overwrite=True
                )
            return
        except Exception as exc:  # noqa: BLE001 - SDK raises many shapes
            last = exc
            if attempt < retries:
                time.sleep(2 * attempt)
    raise SystemExit(f"failed to upload {local_path} -> {remote_path}: {last}")


def ensure_dir(client, remote_dir: str) -> None:
    try:
        client.files.create_directory(remote_dir)
    except Exception:  # noqa: BLE001 - "already exists" is the normal case
        pass


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--local", type=Path, default=DEFAULT_LOCAL,
                    help="local landing zone produced by tools/generate_cdc.py")
    ap.add_argument("--volume", default=DEFAULT_VOLUME,
                    help="target Unity Catalog volume path")
    ap.add_argument("--wave", type=int, action="append", default=None,
                    help="wave to upload; repeat the flag for several (e.g. --wave 1 --wave 2)")
    ap.add_argument("--all", action="store_true", help="upload every wave")
    ap.add_argument("--profile", default=None,
                    help="Databricks CLI profile to authenticate with")
    ap.add_argument("--dry-run", action="store_true",
                    help="print what would be uploaded and stop")
    args = ap.parse_args(argv)

    manifest = load_manifest(args.local)
    waves = available_waves(manifest)

    if args.all:
        selected = waves
    elif args.wave:
        unknown = [w for w in args.wave if w not in waves]
        if unknown:
            raise SystemExit(
                f"wave(s) {unknown} not in the landing zone; available: {waves}"
            )
        selected = sorted(set(args.wave))
    else:
        raise SystemExit(
            "choose what to upload: --wave N (repeatable) or --all\n"
            f"waves available in {args.local}: {waves}"
        )

    # Per-entity counts for the selected waves, so the operator can sanity-check
    # that a wave really did carry changes before pushing 100+ files to a volume.
    planned: list[tuple[Path, str]] = []
    for wave in selected:
        for f in event_files_for_wave(args.local, wave):
            entity = f.parent.name
            planned.append((f, f"{args.volume}/events/{entity}/{f.name}"))

    reference = args.local / "reference" / "geolocation_by_zip.csv"
    extras: list[tuple[Path, str]] = []
    if reference.exists():
        extras.append((reference, f"{args.volume}/reference/{reference.name}"))
    extras.append((args.local / "_manifest.json", f"{args.volume}/_manifest.json"))

    print(f"volume   : {args.volume}")
    print(f"waves    : {selected}")
    for wave in selected:
        per_entity: dict[str, int] = {}
        for f in event_files_for_wave(args.local, wave):
            per_entity[f.parent.name] = per_entity.get(f.parent.name, 0) + 1
        total = sum(per_entity.values())
        print(f"  wave {wave:02d}  {total:3d} file(s)  {per_entity}")
    print(f"reference: {reference.name if reference.exists() else '(not found)'}")
    print(f"events to upload: {len(planned)}")

    if args.dry_run:
        for local_path, remote_path in planned:
            print(f"  would upload {local_path} -> {remote_path}")
        return 0

    try:
        from databricks.sdk import WorkspaceClient  # noqa: PLC0415
    except ImportError:
        raise SystemExit(
            "databricks-sdk is not installed.\n"
            "  pip install -r requirements.txt\n"
            "or upload with the CLI instead:\n"
            f"  databricks fs cp -r {args.local}/events "
            f"dbfs:{args.volume}/events"
        )

    client = WorkspaceClient(profile=args.profile) if args.profile else WorkspaceClient()

    # Directories first: one per entity, plus reference/.
    ensure_dir(client, f"{args.volume}/events")
    for entity in sorted({f.parent.name for f, _ in planned}):
        ensure_dir(client, f"{args.volume}/events/{entity}")
    ensure_dir(client, f"{args.volume}/reference")

    # The reference file and the manifest go up with every invocation. They are
    # tiny, and it means wave 1 always produces a volume the gold layer can read
    # (the geolocation mart is built from this CSV).
    for local_path, remote_path in extras:
        if local_path.exists():
            upload(client, local_path, remote_path)
            print(f"uploaded {remote_path}")

    for i, (local_path, remote_path) in enumerate(planned, 1):
        upload(client, local_path, remote_path)
        if i % 20 == 0 or i == len(planned):
            print(f"  ... {i}/{len(planned)} event files")

    print(
        f"\nDone. {len(planned)} event file(s) for wave(s) {selected} are in "
        f"{args.volume}.\n"
        "Now run the pipeline:\n"
        "  databricks bundle run retail_medallion_cdc"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
