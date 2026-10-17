# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Set up and verify the workspace
# MAGIC
# MAGIC Run this first. It checks that everything the medallion pipeline needs
# MAGIC actually exists, and prints the exact commands to drive the demo.
# MAGIC
# MAGIC This notebook **changes nothing**. The schemas and the landing volume are
# MAGIC declared as code in `resources/schemas.yml` and `resources/volumes.yml`, so
# MAGIC they are created by:
# MAGIC
# MAGIC ```bash
# MAGIC databricks bundle deploy
# MAGIC ```
# MAGIC
# MAGIC Declaring them in the bundle rather than creating them here is deliberate:
# MAGIC the environment becomes reproducible, and this notebook can stay a pure
# MAGIC check that is always safe to run.
# MAGIC
# MAGIC What it verifies:
# MAGIC
# MAGIC 1. the five schemas exist
# MAGIC 2. the landing volume exists and what is in it, per wave
# MAGIC 3. the geolocation reference file is present
# MAGIC 4. the pipeline exists and what state it is in

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parameters
# MAGIC
# MAGIC Defaults match `databricks.yml`. If you changed the bundle variables,
# MAGIC change them here too.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("landing_schema", "retail_landing", "Landing schema")
dbutils.widgets.text("bronze_schema", "retail_bronze", "Bronze schema")
dbutils.widgets.text("silver_schema", "retail_silver", "Silver schema")
dbutils.widgets.text("gold_schema", "retail_gold", "Gold schema")
dbutils.widgets.text("ops_schema", "retail_ops", "Ops schema")
dbutils.widgets.text("volume_name", "cdc", "Landing volume name")
dbutils.widgets.text("pipeline_name", "retail_medallion_cdc", "Pipeline name")

CATALOG = dbutils.widgets.get("catalog")
LANDING_SCHEMA = dbutils.widgets.get("landing_schema")
BRONZE_SCHEMA = dbutils.widgets.get("bronze_schema")
SILVER_SCHEMA = dbutils.widgets.get("silver_schema")
GOLD_SCHEMA = dbutils.widgets.get("gold_schema")
OPS_SCHEMA = dbutils.widgets.get("ops_schema")
VOLUME = f"/Volumes/{CATALOG}/{LANDING_SCHEMA}/{dbutils.widgets.get('volume_name')}"
PIPELINE_NAME = dbutils.widgets.get("pipeline_name")

LAYER_SCHEMAS = {
    "landing": LANDING_SCHEMA,
    "bronze": BRONZE_SCHEMA,
    "silver": SILVER_SCHEMA,
    "gold": GOLD_SCHEMA,
    "ops": OPS_SCHEMA,
}

print(f"catalog      : {CATALOG}")
for layer, schema in LAYER_SCHEMAS.items():
    print(f"{layer:<13}: {CATALOG}.{schema}")
print(f"volume       : {VOLUME}")
print(f"pipeline     : {PIPELINE_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Do the schemas exist?

# COMMAND ----------

present = {r["databaseName"].lower() for r in spark.sql(f"SHOW SCHEMAS IN {CATALOG}").collect()}

problems = []
for layer, schema in LAYER_SCHEMAS.items():
    ok = schema.lower() in present
    print(f"  [{'ok' if ok else 'MISSING'}] {CATALOG}.{schema}")
    if not ok:
        problems.append(f"{CATALOG}.{schema}")

if problems:
    print("\nMissing schemas:", problems)
    print("Deploy the bundle to create them:  databricks bundle deploy")
else:
    print("\nAll five schemas present.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. What is in the landing zone?
# MAGIC
# MAGIC The generated stream is delivered in **waves**. Each wave is a set of
# MAGIC files; uploading wave N and running the pipeline, then uploading wave
# MAGIC N+1, is what makes the demo an incremental-ingestion story rather than a
# MAGIC bulk load. Late arrivals and duplicate redeliveries are *inside* those
# MAGIC later waves.
# MAGIC
# MAGIC So the most important thing to know before running anything is: how many
# MAGIC waves have arrived, and how big are they?

# COMMAND ----------

import re
from collections import defaultdict

try:
    entity_dirs = dbutils.fs.ls(f"{VOLUME}/events")
except Exception as exc:
    raise SystemExit(
        f"Nothing at {VOLUME}/events ({exc}).\n"
        "Generate and upload the landing zone first:\n"
        "  python tools/generate_cdc.py --olist-dir data/olist --out data/landing\n"
        "  python tools/upload_landing.py --all"
    )

files = []
for entity_dir in entity_dirs:
    for f in dbutils.fs.ls(entity_dir.path):
        m = re.search(r"_w(\d+)_", f.name)
        files.append(
            {
                "entity": entity_dir.name,
                "wave": int(m.group(1)) if m else -1,
                "file": f.name,
                "bytes": f.size,
                "broken_demo": "broken" in f.name,
            }
        )

by_wave = defaultdict(lambda: {"files": 0, "mb": 0.0, "broken": 0})
for r in files:
    b = by_wave[r["wave"]]
    b["files"] += 1
    b["mb"] += r["bytes"] / 1e6
    b["broken"] += int(r["broken_demo"])

print(f"{len(files)} file(s), {sum(r['bytes'] for r in files) / 1e6:.1f} MB total\n")
print(f"{'wave':>4}  {'files':>5}  {'MB':>7}  {'broken-JSON files':>18}")
for wave in sorted(by_wave):
    b = by_wave[wave]
    print(f"{wave:>4}  {b['files']:>5}  {b['mb']:>7.1f}  {b['broken']:>18}")

entities = sorted({r["entity"] for r in files})
waves = sorted({r["wave"] for r in files})
print(f"\nentities : {entities}")
print(f"waves    : {waves}")

if not files:
    print("\nNo event files found -- upload a wave before running the pipeline.")
else:
    print(
        f"\nNext: upload the next wave and refresh the pipeline.\n"
        f"  python tools/upload_landing.py --wave {min(waves)}\n"
        f"  databricks bundle run {PIPELINE_NAME}"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Is the geolocation reference file there?
# MAGIC
# MAGIC `reference/geolocation_by_zip.csv` is not a change stream. It is a slowly
# MAGIC changing (really: static) lookup that gold joins onto customers and
# MAGIC sellers. It is aggregated to one row per zip-code prefix so the join is
# MAGIC cheap.
# MAGIC
# MAGIC Note the zip prefix is a **string** with leading zeros preserved. Reading
# MAGIC this file with schema inference would type `01001` as a number, drop the
# MAGIC zero, and silently break the join -- which is why
# MAGIC `gold.dim_geolocation` reads it with `inferSchema => false` and casts
# MAGIC deliberately.

# COMMAND ----------

reference_path = f"{VOLUME}/reference/geolocation_by_zip.csv"
try:
    dbutils.fs.ls(reference_path)
    display(spark.read.text(reference_path).limit(5))
    print("present:", reference_path)
except Exception:
    print(f"NOT FOUND: {reference_path}")
    print("The gold geolocation dimension will fail until this is uploaded.")
    print("  python tools/upload_landing.py --wave 1")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Pipeline state
# MAGIC
# MAGIC Look the pipeline up in `system.lakeflow.pipelines`. On a workspace where
# MAGIC the `system` catalog is not available this falls back to a name-only
# MAGIC message -- the demo does not depend on it.

# COMMAND ----------

try:
    display(
        spark.sql(
            f"""
            SELECT pipeline_id, name, catalog, schema, state
            FROM system.lakeflow.pipelines
            WHERE name = '{PIPELINE_NAME}'
            """
        )
    )
except Exception as exc:
    print(f"could not query system.lakeflow.pipelines: {exc}")
    print(f"Look for a pipeline named '{PIPELINE_NAME}' in the workspace UI.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next
# MAGIC
# MAGIC Run `01_explore_landing_cdc.py` to look at the raw change stream before any
# MAGIC of it is loaded -- the Debezium envelope, the deliberately broken records,
# MAGIC and the schema drift that does not show up until wave 3.
