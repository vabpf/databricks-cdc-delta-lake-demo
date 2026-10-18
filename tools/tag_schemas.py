#!/usr/bin/env python3
"""
Label the demo's Unity Catalog objects so they can be found again
=================================================================

Free Edition gives you exactly one writable catalog -- `workspace` -- and it
will not let you create a second one. Unity Catalog has no folders either: its
only hierarchy is catalog -> schema -> object. So the five medallion schemas
have to share `workspace` with `default` and `information_schema`, and the only
way to "separate" them is to label them.

Two labels do the work, and this script applies the second:

* The **name prefix**. Every schema is `retail_*`, which comes from the bundle
  variable defaults (`var.landing_schema` and friends). In Catalog Explorer's
  filter box -- which matches *names* -- typing `retail` narrows the schema list
  to the demo, and so does

      SHOW SCHEMAS IN workspace LIKE 'retail_*'

* **Unity Catalog tags**, applied here. These carry the vocabulary the pipeline
  and job already use: a `project` tag (`retail-cdc-demo`) and a `layer` tag
  (`landing`, `bronze`, `silver`, `gold`, `ops`).

Read this before you go looking for the tags in the UI
------------------------------------------------------
**Schemas cannot be tag-searched.** Databricks documents the tag search syntax
as covering tables, views, volumes, models, functions, dashboards and
notebooks, and states plainly that catalogs, schemas and columns are excluded.
Catalog Explorer's filter box is a name filter and does not search tags either.

So for a schema, the tag is metadata: you read it on the schema's Overview page
in Catalog Explorer, or in SQL:

    SELECT * FROM workspace.information_schema.schema_tags;

The searchable payoff is the objects *inside* the schemas, which is why this
script tags tables and volumes as well. In the workspace search bar (Cmd-P):

    tag:project:retail-cdc-demo        # every tagged table and volume
    tag:layer:silver                   # just the silver layer

That returns the demo's objects and nothing else -- which is the "separate them
from everything else" the name prefix cannot do, because it searches across the
whole workspace rather than inside one schema list.

Usage
-----
    databricks bundle deploy
    python tools/tag_schemas.py          # schemas, volumes, and any tables

Run it again after the pipeline has built tables, to pick up the new ones. It
is idempotent: a tag that is already correct is left alone, and a tag whose
value has drifted is updated in place.
"""

from __future__ import annotations

import argparse
import sys

# Defaults match databricks.yml and resources/pipeline.yml. Override them if you
# changed the bundle variables.
DEFAULT_CATALOG = "workspace"
DEFAULT_PREFIX = "retail"
DEFAULT_PROJECT = "retail-cdc-demo"

ENTITY_SCHEMAS = "schemas"
ENTITY_VOLUMES = "volumes"
ENTITY_TABLES = "tables"


def layer_of(name: str, prefix: str) -> str:
    """Map a schema name onto its layer tag: `retail_bronze` -> `bronze`."""
    rest = name[len(prefix):] if name.startswith(prefix) else name
    return rest.lstrip("_") or name


def apply_tags(client, entity_type: str, entity_name: str, tags: dict[str, str],
               dry_run: bool) -> list[tuple[str, str, str]]:
    """Make sure `tags` are set on one entity. Returns (marker, key, value)."""
    from databricks.sdk.service.catalog import EntityTagAssignment  # noqa: PLC0415

    existing = {
        a.tag_key: a.tag_value
        for a in client.entity_tag_assignments.list(entity_type, entity_name)
    }

    results: list[tuple[str, str, str]] = []
    for key, value in sorted(tags.items()):
        if existing.get(key) == value:
            results.append(("=", key, value))
            continue

        if key in existing:
            results.append(("~", key, value))
            if not dry_run:
                client.entity_tag_assignments.update(
                    entity_type, entity_name, key,
                    EntityTagAssignment(entity_type=entity_type,
                                        entity_name=entity_name,
                                        tag_key=key, tag_value=value),
                    update_mask="tag_value",
                )
        else:
            results.append(("+", key, value))
            if not dry_run:
                client.entity_tag_assignments.create(
                    EntityTagAssignment(entity_type=entity_type,
                                        entity_name=entity_name,
                                        tag_key=key, tag_value=value)
                )
    return results


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--catalog", default=DEFAULT_CATALOG,
                    help="catalog holding the demo schemas")
    ap.add_argument("--prefix", default=DEFAULT_PREFIX,
                    help="only schemas whose name starts with this are tagged")
    ap.add_argument("--project", default=DEFAULT_PROJECT,
                    help="value for the `project` tag")
    ap.add_argument("--dry-run", action="store_true",
                    help="show what would change and stop")
    ap.add_argument("--profile", default=None,
                    help="Databricks CLI profile to authenticate with")
    args = ap.parse_args(argv)

    try:
        from databricks.sdk import WorkspaceClient  # noqa: PLC0415
    except ImportError:
        raise SystemExit(
            "databricks-sdk is not installed.\n"
            "  pip install -r requirements.txt\n"
            "or tag by hand with the CLI:\n"
            f"  databricks entity-tag-assignments create "
            f"{args.catalog}.retail_bronze project schemas --tag-value {args.project}"
        )

    client = WorkspaceClient(profile=args.profile) if args.profile else WorkspaceClient()

    schemas = sorted(
        s.name for s in client.schemas.list(catalog_name=args.catalog)
        if s.name and s.name.startswith(args.prefix)
    )
    if not schemas:
        raise SystemExit(
            f"no schemas starting with '{args.prefix}' in catalog "
            f"'{args.catalog}'.\n"
            "Deploy the bundle to create them:\n"
            "  databricks bundle deploy"
        )

    # Each entry is (entity_type, fully-qualified name, tags). Schemas and
    # volumes exist straight after `bundle deploy`; tables only appear once the
    # pipeline has written them, so the table loop is empty on a fresh deploy.
    plan: list[tuple[str, str, dict[str, str]]] = []
    for schema in schemas:
        layer = layer_of(schema, args.prefix)
        tags = {"project": args.project, "layer": layer}
        plan.append((ENTITY_SCHEMAS, f"{args.catalog}.{schema}", tags))
        for volume in client.volumes.list(catalog_name=args.catalog, schema_name=schema):
            plan.append((ENTITY_VOLUMES, volume.full_name, dict(tags)))
        for table in client.tables.list(catalog_name=args.catalog, schema_name=schema):
            plan.append((ENTITY_TABLES, table.full_name, dict(tags)))

    def count(entity_type: str) -> int:
        return sum(1 for e, _, _ in plan if e == entity_type)

    print(f"catalog : {args.catalog}")
    print(f"prefix  : {args.prefix}")
    print(f"tags    : project={args.project}, layer=<layer>")
    print(f"entities: {len(plan)} ({count(ENTITY_SCHEMAS)} schemas, "
          f"{count(ENTITY_VOLUMES)} volumes, {count(ENTITY_TABLES)} tables)")
    print()

    counts = {"+": 0, "~": 0, "=": 0}
    for entity_type, entity_name, tags in plan:
        for marker, key, value in apply_tags(client, entity_type, entity_name,
                                             tags, args.dry_run):
            counts[marker] += 1
            print(f"  {marker} {entity_name:<45} {key}={value}")

    verb = "would be" if args.dry_run else "were"
    print(f"\n{counts['+']} tag(s) created, {counts['~']} updated, "
          f"{counts['=']} already correct ({verb}).")

    if not args.dry_run:
        if count(ENTITY_TABLES) == 0:
            print("\nNo tables yet, so only schemas and volumes were tagged. Re-run "
                  "this after\nthe pipeline has built the tables to make them "
                  "tag-searchable.")
        print("\nTag search -- in the workspace search bar (Cmd-P), not in Catalog\n"
              "Explorer's filter box, which only matches names:\n"
              f"  tag:project:{args.project}")
        print("\nSchemas are not tag-searchable (a documented limitation); read "
              "their tags with:\n"
              "  SELECT * FROM %s.information_schema.schema_tags;" % args.catalog)
    else:
        print("Dry run: nothing was changed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
