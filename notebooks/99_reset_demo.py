# Databricks notebook source
# MAGIC %md
# MAGIC # 99 - Reset the demo
# MAGIC
# MAGIC Two different things get called "resetting", and conflating them is how
# MAGIC people lose an afternoon. This notebook does both, but keeps them separate:
# MAGIC
# MAGIC | reset | what it does | when you want it |
# MAGIC |---|---|---|
# MAGIC | **Full refresh** | recomputes every dataset from the landing zone. Files are kept, checkpoint state is reset. | you changed pipeline logic and need existing data re-derived |
# MAGIC | **Clean slate** | drops every table the pipeline owns, and optionally empties the landing volume | you want the demo to run from wave 1 again, as if for the first time |
# MAGIC
# MAGIC Neither is destructive to the *repo*, only to the workspace objects, and both
# MAGIC are gated behind a confirmation widget.
# MAGIC
# MAGIC Two notes worth reading before pressing anything:
# MAGIC
# MAGIC * **Streaming tables cannot be `RESTORE`d.** The usual Delta recovery move
# MAGIC   does not apply here. Point-in-time recovery is done by full-refreshing
# MAGIC   from the landing zone, which is immutable.
# MAGIC * **Dropping a pipeline's tables does not stop the pipeline.** The pipeline
# MAGIC   will happily recreate them on its next run -- which is usually exactly what
# MAGIC   you want. If you want the tables gone *and* to stay gone, delete the
# MAGIC   pipeline too (`databricks bundle destroy`).

# COMMAND ----------

import time

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("landing_schema", "retail_landing", "Landing schema")
dbutils.widgets.text("bronze_schema", "retail_bronze", "Bronze schema")
dbutils.widgets.text("silver_schema", "retail_silver", "Silver schema")
dbutils.widgets.text("gold_schema", "retail_gold", "Gold schema")
dbutils.widgets.text("ops_schema", "retail_ops", "Ops schema")
dbutils.widgets.text("volume_name", "cdc", "Landing volume name")
dbutils.widgets.text("pipeline_name", "retail_medallion_cdc", "Pipeline name")
dbutils.widgets.text("pipeline_id", "", "Pipeline id (optional; looked up by name if blank)")
dbutils.widgets.dropdown("confirm", "no", ["no", "yes"], "Confirm destructive actions")
dbutils.widgets.dropdown("clear_landing", "no", ["no", "yes"], "Also empty the landing volume?")
dbutils.widgets.dropdown("full_refresh", "no", ["no", "yes"], "Run a full refresh instead?")
dbutils.widgets.dropdown("auto_run", "no", ["no", "yes"], "Trigger the pipeline from this notebook?")

CATALOG = dbutils.widgets.get("catalog")
LAYER_SCHEMAS = {
    "landing": dbutils.widgets.get("landing_schema"),
    "bronze": dbutils.widgets.get("bronze_schema"),
    "silver": dbutils.widgets.get("silver_schema"),
    "gold": dbutils.widgets.get("gold_schema"),
    "ops": dbutils.widgets.get("ops_schema"),
}
VOLUME = (
    f"/Volumes/{CATALOG}/{LAYER_SCHEMAS['landing']}/"
    f"{dbutils.widgets.get('volume_name')}"
)
PIPELINE_NAME = dbutils.widgets.get("pipeline_name")

CONFIRM = dbutils.widgets.get("confirm") == "yes"
CLEAR_LANDING = dbutils.widgets.get("clear_landing") == "yes"
FULL_REFRESH = dbutils.widgets.get("full_refresh") == "yes"
AUTO_RUN = dbutils.widgets.get("auto_run") == "yes"

# The landing schema holds the volume, not pipeline tables, so only these four
# are candidates for "drop every table".
DATA_SCHEMAS = ["bronze", "silver", "gold", "ops"]


def scalar(sql, default=None):
    try:
        return spark.sql(sql).collect()[0][0]
    except Exception:
        return default


print(f"catalog       : {CATALOG}")
for layer, schema in LAYER_SCHEMAS.items():
    print(f"  {layer:<8} {CATALOG}.{schema}")
print(f"volume        : {VOLUME}")
print(f"pipeline      : {PIPELINE_NAME}")
print()
print(f"confirm       : {CONFIRM}")
print(f"clear_landing : {CLEAR_LANDING}")
print(f"full_refresh  : {FULL_REFRESH}")
print(f"auto_run      : {AUTO_RUN}")

if not CONFIRM:
    print(
        "\nNothing will be changed. To proceed, set `confirm = yes` and choose the\n"
        "action you want (`full_refresh = yes` and/or `clear_landing = yes`), then\n"
        "re-run the notebook."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Pipeline id (needed for the refresh path)
# MAGIC
# MAGIC Same lookup as notebooks 30 and 40.

# COMMAND ----------

PIPELINE_ID = dbutils.widgets.get("pipeline_id") or (scalar(
    f"SELECT pipeline_id FROM system.lakeflow.pipelines WHERE name = '{PIPELINE_NAME}' LIMIT 1",
    "",
) or "")
print(f"pipeline_id: {PIPELINE_ID or '(not resolved)'}")


def run_pipeline(full_refresh=False):
    """Trigger a pipeline update and wait. Returns True on success."""
    if not PIPELINE_ID:
        print("No pipeline_id; cannot trigger the pipeline from here.")
        return False
    try:
        from databricks.sdk import WorkspaceClient
    except ImportError:
        print("databricks-sdk not available.")
        return False
    try:
        w = WorkspaceClient()
        resp = w.pipelines.start_update(pipeline_id=PIPELINE_ID, full_refresh=full_refresh)
        update_id = resp.update_id
        print(f"  started update {update_id} (full_refresh={full_refresh})")
        while True:
            update = w.pipelines.get_update(
                pipeline_id=PIPELINE_ID, update_id=update_id
            ).update
            state = getattr(update.state, "value", None) or str(update.state)
            print(f"  state: {state}")
            if state in ("COMPLETED", "FAILED", "CANCELED"):
                return state == "COMPLETED"
            time.sleep(15)
    except Exception as exc:  # noqa: BLE001
        print(f"  could not run the pipeline: {exc}")
        return False

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Full refresh (the safe reset)
# MAGIC
# MAGIC Recompute everything from the landing files. This is the operation to reach
# MAGIC for when you have edited `src/pipeline/*` and want the existing data
# MAGIC re-derived under the new logic. Nothing is deleted; the pipeline just starts
# MAGIC over from the raw files.

# COMMAND ----------

if FULL_REFRESH:
    if not CONFIRM:
        print("full_refresh requested but confirm != yes. Set confirm = yes.")
    elif AUTO_RUN and PIPELINE_ID:
        print("running a full refresh ...")
        ok = run_pipeline(full_refresh=True)
        print("full refresh completed" if ok else "full refresh did NOT complete")
    else:
        print("Run this by hand (auto_run = no or no pipeline id):")
        print("  databricks bundle run retail_medallion_cdc --full-refresh")
        print("  # or: databricks pipelines start-update --pipeline-id <id> --full-refresh")
else:
    print("full_refresh = no -- skipping the refresh path.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Inventoried tables per schema
# MAGIC
# MAGIC Before dropping anything, list it. `information_schema.tables` gives the
# MAGIC table type, which decides the DDL: a materialized view is dropped with
# MAGIC `DROP MATERIALIZED VIEW`, a plain view with `DROP VIEW`, and everything else
# MAGIC with `DROP TABLE` (streaming tables included -- they are tables).

# COMMAND ----------

schema_list = ", ".join(f"'{LAYER_SCHEMAS[s]}'" for s in DATA_SCHEMAS)

inventory = []
try:
    inventory = spark.sql(
        f"""
        SELECT table_schema, table_name, table_type
        FROM {CATALOG}.information_schema.tables
        WHERE table_schema IN ({schema_list})
        ORDER BY table_schema, table_name
        """
    ).collect()
except Exception as exc:  # noqa: BLE001
    print(f"information_schema unavailable ({exc}); falling back to SHOW TABLES.")
    for layer in DATA_SCHEMAS:
        schema = LAYER_SCHEMAS[layer]
        for r in spark.sql(f"SHOW TABLES IN {CATALOG}.{schema}").collect():
            inventory.append({"table_schema": schema, "table_name": r["tableName"],
                              "table_type": "UNKNOWN"})

print(f"{len(inventory)} object(s) across {', '.join(LAYER_SCHEMAS[s] for s in DATA_SCHEMAS)}:\n")
for r in inventory:
    print(f"  {r['table_schema']}.{r['table_name']:<32} {r['table_type']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Clean slate (drops everything, optional volume wipe)
# MAGIC
# MAGIC Only runs when `confirm = yes`. Each drop is independent and failures are
# MAGIC reported rather than fatal -- a single stuck object should not abort the
# MAGIC cleanup of the rest.

# COMMAND ----------

dropped, failed = [], []

if not CONFIRM:
    print("confirm != yes -- not dropping anything.")
else:
    for r in inventory:
        schema, name, kind = r["table_schema"], r["table_name"], str(r["table_type"]).upper()
        if kind == "MATERIALIZED_VIEW":
            ddl = f"DROP MATERIALIZED VIEW IF EXISTS {CATALOG}.{schema}.{name}"
        elif kind == "VIEW":
            ddl = f"DROP VIEW IF EXISTS {CATALOG}.{schema}.{name}"
        else:
            ddl = f"DROP TABLE IF EXISTS {CATALOG}.{schema}.{name}"
        try:
            spark.sql(ddl)
            dropped.append(f"{schema}.{name}")
        except Exception as exc:  # noqa: BLE001
            failed.append((f"{schema}.{name}", str(exc)))

    print(f"dropped {len(dropped)} object(s)")
    if failed:
        print(f"\n{len(failed)} object(s) could not be dropped:")
        for name, err in failed:
            print(f"  {name}: {err}")

    if CLEAR_LANDING:
        try:
            dbutils.fs.rm(f"{VOLUME}/events", True)
            print(f"\nemptied {VOLUME}/events")
            print(
                "The landing zone is now empty. Re-upload a wave to run the demo again:\n"
                "  python tools/upload_landing.py --wave 1"
            )
        except Exception as exc:  # noqa: BLE001
            print(f"\ncould not empty {VOLUME}/events: {exc}")
    else:
        print("\nlanding volume left intact (clear_landing = no).")

    print(
        "\nIMPORTANT: a pipeline will recreate its tables on the next run. Because\n"
        "the tables were dropped, do a full refresh so nothing is skipped:\n"
        "  databricks bundle run retail_medallion_cdc --full-refresh"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. What to do next
# MAGIC
# MAGIC Depending on what you chose above:
# MAGIC
# MAGIC * **nothing happened** -- `confirm = no`. Set it to `yes` and re-run.
# MAGIC * **full refresh only** -- the pipeline is recomputed from the landing zone.
# MAGIC   Re-run `10_medallion_walkthrough.py` to confirm it still passes.
# MAGIC * **clean slate, landing kept** -- run a full refresh; the tables come back
# MAGIC   from the files already in the volume.
# MAGIC * **clean slate, landing emptied** -- re-upload from wave 1 and run the
# MAGIC   pipeline:
# MAGIC
# MAGIC   ```bash
# MAGIC   python tools/upload_landing.py --wave 1
# MAGIC   databricks bundle run retail_medallion_cdc
# MAGIC   ```
# MAGIC
# MAGIC To remove the pipeline and every declared resource as well, use the bundle:
# MAGIC
# MAGIC ```bash
# MAGIC databricks bundle destroy
# MAGIC ```
# MAGIC
# MAGIC That drops the schemas and the landing volume declared in
# MAGIC `resources/*.yml`, so it is the true "start completely over" option.

# COMMAND ----------

print("reset notebook finished -- review the output above for what actually ran.")
