# Databricks notebook source
# MAGIC %md
# MAGIC # 40 - Observability: the event log, expectations and the quarantine
# MAGIC
# MAGIC The pipeline is written to *keep* rejected rows (the `*_quarantine` tables)
# MAGIC and to *account* for them (`CONSTRAINT ... EXPECT ... ON VIOLATION DROP
# MAGIC ROW`). This notebook is where those two halves meet, and where the
# MAGIC pipeline's own operations data -- the **event log** -- is read back.
# MAGIC
# MAGIC The event log is the Lakeflow pipeline's structured run log. It is exposed
# MAGIC as a table-valued function, which means it is queryable with SQL like any
# MAGIC other table:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT * FROM event_log('<pipeline-id>');
# MAGIC ```
# MAGIC
# MAGIC Three things here are worth internalising:
# MAGIC
# MAGIC 1. **Expectations produce metrics, not just drops.** `ON VIOLATION DROP ROW`
# MAGIC    would otherwise be invisible. The event log is where the *count* of
# MAGIC    dropped rows lives; the quarantine tables are where the *rows* live.
# MAGIC    They should agree, and this notebook checks that they do.
# MAGIC 2. **The event log is operationally useful on its own.** Update states,
# MAGIC    per-flow output counts and durations are all in there -- enough to build
# MAGIC    an alerting dashboard over the pipeline without reading any of the data.
# MAGIC 3. **Rescued data has a home.** The deliberately broken JSON does not
# MAGIC    vanish and does not break the run; it lands in `_rescued_data` and then
# MAGIC    in quarantine.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("bronze_schema", "retail_bronze", "Bronze schema")
dbutils.widgets.text("ops_schema", "retail_ops", "Ops schema")
dbutils.widgets.text("pipeline_name", "retail_medallion_cdc", "Pipeline name")
dbutils.widgets.text("pipeline_id", "", "Pipeline id (optional; looked up by name if blank)")

CATALOG = dbutils.widgets.get("catalog")
BRONZE = f"{CATALOG}.{dbutils.widgets.get('bronze_schema')}"
OPS = f"{CATALOG}.{dbutils.widgets.get('ops_schema')}"
PIPELINE_NAME = dbutils.widgets.get("pipeline_name")

ENTITIES = [
    "customers", "products", "sellers",
    "orders", "order_items", "order_payments", "order_reviews",
]

_results = []


def check(label, condition, detail=""):
    ok = bool(condition)
    _results.append((label, ok))
    print(f"  [{'PASS' if ok else 'FAIL'}] {label}{(' -- ' + detail) if detail else ''}")
    return ok


def scalar(sql, default=None):
    try:
        return spark.sql(sql).collect()[0][0]
    except Exception:
        return default


def table_exists(fqn):
    try:
        spark.sql(f"SELECT 1 FROM {fqn} LIMIT 1")
        return True
    except Exception:
        return False


# The `details` column has been a VARIANT since DBR 15 and a STRING before that.
# CAST(details AS STRING) is a no-op on the string form and produces JSON on the
# variant form, so the JSON-path extraction below works either way.
def dpath(expr):
    return f"get_json_object(CAST(details AS STRING), '{expr}')"


print(f"bronze: {BRONZE}\nops   : {OPS}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Find the pipeline
# MAGIC
# MAGIC The event log is keyed by pipeline id. Look it up in
# MAGIC `system.lakeflow.pipelines`; if that is unavailable, set the `pipeline_id`
# MAGIC widget yourself (it is in the pipeline URL). The notebook still does the
# MAGIC table-based data-quality work if no id is available -- only the event-log
# MAGIC sections are skipped.

# COMMAND ----------

PIPELINE_ID = dbutils.widgets.get("pipeline_id") or (scalar(
    f"SELECT pipeline_id FROM system.lakeflow.pipelines WHERE name = '{PIPELINE_NAME}' LIMIT 1",
    "",
) or "")
print(f"pipeline_id: {PIPELINE_ID or '(not resolved)'}")

EVENT_LOG_OK = False
if PIPELINE_ID:
    try:
        n = scalar(f"SELECT COUNT(*) FROM event_log('{PIPELINE_ID}')", 0)
        EVENT_LOG_OK = bool(n)
        print(f"event_log(): {n:,} event(s) visible")
    except Exception as exc:  # noqa: BLE001
        print(f"event_log() not readable: {exc}")
else:
    print("No pipeline id -- skipping the event-log sections.")

if not EVENT_LOG_OK:
    print(
        "\nTo enable the event-log sections, set the `pipeline_id` widget to the\n"
        "pipeline id (visible in the pipeline URL in the workspace)."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Update history -- the run log
# MAGIC
# MAGIC Every `databricks bundle run` is one update, and each emits `update_progress`
# MAGIC events that carry its state. This is what you would alert on: a failed or
# MAGIC long-running update, without reading a single row of data.

# COMMAND ----------

if EVENT_LOG_OK:
    display(
        spark.sql(
            f"""
            SELECT
              timestamp,
              origin.flow_name                     AS flow,
              {dpath('$.update_progress.state')}   AS update_state,
              message
            FROM event_log('{PIPELINE_ID}')
            WHERE event_type = 'update_progress'
            ORDER BY timestamp DESC
            LIMIT 40
            """
        )
    )
else:
    print("(skipped: no event log)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Per-flow throughput -- the last thing each flow did
# MAGIC
# MAGIC `flow_progress` events carry per-flow metrics. Aggregating to the latest
# MAGIC event per flow gives a live "what does the pipeline look like right now"
# MAGIC table: each source, each conform step, each AUTO CDC target, each gold
# MAGIC mart.

# COMMAND ----------

if EVENT_LOG_OK:
    display(
        spark.sql(
            f"""
            WITH events AS (
              SELECT
                timestamp,
                origin.flow_name AS flow,
                {dpath('$.flow_progress.status')} AS status,
                CAST({dpath('$.flow_progress.metrics.num_output_rows')} AS BIGINT) AS output_rows
              FROM event_log('{PIPELINE_ID}')
              WHERE event_type = 'flow_progress'
            ),
            latest AS (
              SELECT *, ROW_NUMBER() OVER (PARTITION BY flow ORDER BY timestamp DESC) AS rn
              FROM events
            )
            SELECT flow, status, output_rows, timestamp
            FROM latest
            WHERE rn = 1
            ORDER BY output_rows DESC NULLS LAST
            """
        )
    )
else:
    print("(skipped: no event log)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Expectation metrics -- the counts behind the drops
# MAGIC
# MAGIC Every expectation declared with `CREATE ... (CONSTRAINT ... EXPECT ...)`
# MAGIC reports `passed_records` and `failed_records` into the event log. This is
# MAGIC the metric half of the story; section 4 is the row half.
# MAGIC
# MAGIC The nested structure is JSON inside a VARIANT, so it is normalised with
# MAGIC `from_json` + `explode`. Doing it that way rather than with more
# MAGIC `get_json_object` nesting is deliberate: it is easier to read, and it
# MAGIC degrades gracefully if the producer adds a field.

# COMMAND ----------

EXPECTATION_ROWS = f"""
WITH dq AS (
  SELECT
    timestamp,
    from_json(
      {dpath('$.flow_progress.data_quality.expectations')},
      'ARRAY<STRUCT<name STRING, dataset STRING, passed_records BIGINT, failed_records BIGINT>>'
    ) AS expectations
  FROM event_log('{PIPELINE_ID}')
  WHERE event_type = 'flow_progress'
)
SELECT
  e.timestamp,
  exp.dataset,
  exp.name        AS expectation,
  exp.passed_records,
  exp.failed_records
FROM dq e
LATERAL VIEW explode(e.expectations) t AS exp
WHERE exp IS NOT NULL AND exp.name IS NOT NULL
"""

# The same extraction, aggregated across all updates. Kept separate from the
# detail query above rather than glued on with a GROUP BY, because the detail
# query selects a non-aggregated timestamp column and cannot be grouped in place.
EXPECTATION_TOTALS = f"""
WITH dq AS (
  SELECT
    from_json(
      {dpath('$.flow_progress.data_quality.expectations')},
      'ARRAY<STRUCT<name STRING, dataset STRING, passed_records BIGINT, failed_records BIGINT>>'
    ) AS expectations
  FROM event_log('{PIPELINE_ID}')
  WHERE event_type = 'flow_progress'
)
SELECT
  exp.dataset,
  exp.name                 AS expectation,
  SUM(exp.passed_records)  AS passed_records,
  SUM(exp.failed_records)  AS failed_records
FROM dq e
LATERAL VIEW explode(e.expectations) t AS exp
WHERE exp IS NOT NULL AND exp.name IS NOT NULL
GROUP BY exp.dataset, exp.name
"""

# COMMAND ----------

if EVENT_LOG_OK:
    display(spark.sql(EXPECTATION_ROWS + "ORDER BY e.timestamp DESC, exp.dataset LIMIT 100"))
else:
    print("(skipped: no event log)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Does the metric agree with the rows?
# MAGIC
# MAGIC This is the reconciliation that makes the design testable:
# MAGIC
# MAGIC * the *metric* is `SUM(failed_records)` across all updates, from the event log;
# MAGIC * the *rows* are the quarantine tables;
# MAGIC * they must be equal per entity, because quarantine is written as the exact
# MAGIC   complement of the conform predicate.
# MAGIC
# MAGIC If they ever diverge, the two predicates have drifted apart -- which is the
# MAGIC single most likely way for this design to rot, and the reason both are
# MAGIC annotated in `02_bronze_conform.sql` with "MUST stay the exact complement".

# COMMAND ----------

metrics = {}
if EVENT_LOG_OK:
    for r in spark.sql(EXPECTATION_TOTALS).collect():
        metrics[r["dataset"] or r["expectation"]] = r["failed_records"]

quarantine = {
    entity: scalar(f"SELECT COUNT(*) FROM {OPS}.{entity}_quarantine", 0)
    for entity in ENTITIES
}

rows = []
for entity in ENTITIES:
    failed = None
    for key, value in metrics.items():
        if entity in key:
            failed = value
            break
    rows.append(
        {
            "entity": entity,
            "quarantined_rows": quarantine[entity],
            "failed_records_metric": failed,
        }
    )

display(
    spark.createDataFrame(
        rows,
        schema="entity string, quarantined_rows long, failed_records_metric long",
    ).orderBy("entity")
)

if EVENT_LOG_OK and metrics:
    diffs = [
        abs(r["quarantined_rows"] - (r["failed_records_metric"] or 0))
        for r in rows
        if r["failed_records_metric"] is not None
    ]
    check(
        "quarantine rows == failed_records reported by the pipeline expectations",
        all(d == 0 for d in diffs),
        f"max difference {max(diffs) if diffs else 'n/a'}",
    )
else:
    print("Expectation metrics unavailable; the quarantine counts above are still authoritative.")

# COMMAND ----------

# MAGIC %md
# MAGIC A look at *why* rows were rejected. Three planted failure modes, plus the
# MAGIC rescued-data path:

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT 'customers' AS entity, _quarantine_reason, COUNT(*) AS records FROM {OPS}.customers_quarantine GROUP BY _quarantine_reason
        UNION ALL
        SELECT 'orders', _quarantine_reason, COUNT(*) FROM {OPS}.orders_quarantine GROUP BY _quarantine_reason
        UNION ALL
        SELECT 'order_reviews', _quarantine_reason, COUNT(*) FROM {OPS}.order_reviews_quarantine GROUP BY _quarantine_reason
        ORDER BY entity, records DESC
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Rescued data -- the garbage that did not get dropped
# MAGIC
# MAGIC One file in `orders/` is literally truncated JSON
# MAGIC (`orders_w03_p999_broken.jsonl.gz`). Auto Loader is configured with
# MAGIC `cloudFiles.rescuedDataColumn = _rescued_data`, so instead of dropping the
# MAGIC unparseable line it keeps the original text in that column.
# MAGIC
# MAGIC Two counts are interesting: how much was rescued, and whether all of it made
# MAGIC it into quarantine (it should -- a rescued row has no `_op`, so it fails the
# MAGIC conform expectation and is dropped *and recorded*).

# COMMAND ----------

rescued_raw = scalar(
    f"SELECT COUNT(*) FROM {BRONZE}.orders_raw WHERE _rescued_data IS NOT NULL", 0
)
rescued_quarantine = scalar(
    f"""
    SELECT COUNT(*) FROM {OPS}.orders_quarantine
    WHERE _quarantine_reason LIKE 'rescued_data%'
    """,
    0,
)

print(f"rescued rows in bronze.orders_raw       : {rescued_raw:,}")
print(f"rescued rows in ops.orders_quarantine    : {rescued_quarantine:,}")
print()

check(
    "rescued (unparseable) records are all accounted for in quarantine",
    rescued_raw == rescued_quarantine and rescued_raw > 0,
    f"{rescued_raw} vs {rescued_quarantine}",
)

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT _source_file, substr(_rescued_data, 1, 120) AS rescued_fragment
        FROM {BRONZE}.orders_raw
        WHERE _rescued_data IS NOT NULL
        LIMIT 5
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. The data-quality scorecard
# MAGIC
# MAGIC Everything above, in one table per entity: how many raw records landed, how
# MAGIC many were accepted, how many were rejected, and what fraction that is. The
# MAGIC invariant in the last column is the one from notebook 10 --
# MAGIC `raw == conformed + quarantined` -- evaluated for every entity at once,
# MAGIC from the tables themselves.

# COMMAND ----------

scorecard = []
for entity in ENTITIES:
    raw = scalar(f"SELECT COUNT(*) FROM {BRONZE}.{entity}_raw", 0)
    cdc = scalar(f"SELECT COUNT(*) FROM {BRONZE}.{entity}_cdc", 0)
    quar = quarantine[entity]
    scorecard.append(
        {
            "entity": entity,
            "raw": raw,
            "conformed": cdc,
            "quarantined": quar,
            "quarantine_pct": round(100.0 * quar / raw, 3) if raw else None,
            "fully_accounted": (cdc + quar == raw),
        }
    )

df = spark.createDataFrame(
    scorecard,
    schema="entity string, raw long, conformed long, quarantined long, "
    "quarantine_pct double, fully_accounted boolean",
).orderBy("entity")
display(df)
print()

check(
    "every entity: raw == conformed + quarantined (nothing silently lost)",
    all(r["fully_accounted"] for r in scorecard),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Result

# COMMAND ----------

passed = sum(1 for _, ok in _results if ok)
total = len(_results)
print(f"{passed}/{total} checks passed")
for label, ok in _results:
    print(f"  [{'PASS' if ok else 'FAIL'}] {label}")

if passed != total:
    raise AssertionError(f"{total - passed} check(s) failed")

print("\n" + "=" * 70)
print("Next:")
print("  99_reset_demo.py  -- tear the demo down / start it over")
print("  docs/demo-script.md  -- the scripted end-to-end run")
print("=" * 70)
