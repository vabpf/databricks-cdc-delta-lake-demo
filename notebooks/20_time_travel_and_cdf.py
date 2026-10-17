# Databricks notebook source
# MAGIC %md
# MAGIC # 20 - Time travel, change data capture and the right to be forgotten
# MAGIC
# MAGIC `10_medallion_walkthrough.py` checked that silver and gold hold the *right
# MAGIC answer*. This notebook is about a different property: can you prove how the
# MAGIC answer was reached, and can you answer "what did this look like at an
# MAGIC earlier point in time?"
# MAGIC
# MAGIC Everything the pipeline produced is a Delta table, so there are two
# MAGIC independent histories available, and they answer different questions:
# MAGIC
# MAGIC | mechanism | question it answers |
# MAGIC |---|---|
# MAGIC | **Delta table history / time travel** | *what was in the table at version N / at time T?* |
# MAGIC | **change data feed (`table_changes`)** | *which rows changed, how, and in which commit?* |
# MAGIC
# MAGIC There is a third one too: the gold `*_changes` streaming tables, which are
# MAGIC that same change feed materialised as an append-only table you can query
# MAGIC without knowing a version range.
# MAGIC
# MAGIC The last section uses all of this to walk through a GDPR erasure -- which
# MAGIC is the honest test of whether the layering is real, because an SCD2
# MAGIC dimension and the right to be forgotten are in direct tension.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("silver_schema", "retail_silver", "Silver schema")
dbutils.widgets.text("gold_schema", "retail_gold", "Gold schema")

CATALOG = dbutils.widgets.get("catalog")
SILVER = f"{CATALOG}.{dbutils.widgets.get('silver_schema')}"
GOLD = f"{CATALOG}.{dbutils.widgets.get('gold_schema')}"

_results = []


def check(label, condition, detail=""):
    ok = bool(condition)
    _results.append((label, ok))
    print(f"  [{'PASS' if ok else 'FAIL'}] {label}{(' -- ' + detail) if detail else ''}")
    return ok


def scalar(sql):
    return spark.sql(sql).collect()[0][0]


def table_exists(fqn):
    try:
        spark.sql(f"SELECT 1 FROM {fqn} LIMIT 1")
        return True
    except Exception:
        return False


print(f"silver: {SILVER}\ngold  : {GOLD}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Preflight
# MAGIC
# MAGIC Both histories only exist once the pipeline has run at least once, and the
# MAGIC change feed only exists once the table has been written at least twice.

# COMMAND ----------

required = [f"{SILVER}.customers", f"{SILVER}.orders", f"{GOLD}.customers_changes"]
missing = [t for t in required if not table_exists(t)]
if missing:
    print("Not there yet:")
    for t in missing:
        print(f"  - {t}")
    print(
        "\nUpload a wave or two and run the pipeline, then re-run this notebook:\n"
        "  python tools/upload_landing.py --wave 1 && databricks bundle run retail_medallion_cdc\n"
        "  python tools/upload_landing.py --wave 2 && databricks bundle run retail_medallion_cdc"
    )
    dbutils.notebook.exit("pipeline not initialised")

print("required tables exist")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Delta history -- every pipeline update is a version
# MAGIC
# MAGIC A streaming table is still a Delta table, and every pipeline update that
# MAGIC writes to it commits a new Delta version. So the *ingestion waves* from the
# MAGIC demo map onto versions you can address.
# MAGIC
# MAGIC `DESCRIBE HISTORY` is the table's own commit log. Read it as: one row per
# MAGIC `databricks bundle run`.
# MAGIC
# MAGIC Note the version numbers are per-source-entity, not global: `customers` and
# MAGIC `orders` advance independently, because each is a separate table.

# COMMAND ----------

history = spark.sql(f"DESCRIBE HISTORY {SILVER}.customers")
display(
    history.select("version", "timestamp", "operation", "operationParameters")
    .orderBy("version", ascending=False)
    .limit(20)
)

versions = [r["version"] for r in history.select("version").collect()]
print(f"\nsilver.customers has {len(versions)} version(s): {min(versions)}..{max(versions)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Time travel -- query the past
# MAGIC
# MAGIC `VERSION AS OF` and `TIMESTAMP AS OF` are the two forms. They work on any
# MAGIC Delta table, including these, and on Unity Catalog from any compute that
# MAGIC can read the table.
# MAGIC
# MAGIC What is worth noticing below is not that it returns a number -- it is that
# MAGIC the number *grows monotonically with the version*, because versions
# MAGIC correspond to waves. An SCD2 target accumulates one version per distinct
# MAGIC customer state, so early versions are smaller.

# COMMAND ----------

earliest = min(versions)
rows_earliest = scalar(f"SELECT COUNT(*) FROM {SILVER}.customers VERSION AS OF {earliest}")
rows_latest = scalar(f"SELECT COUNT(*) FROM {SILVER}.customers")

print(f"rows at version {earliest:<4}: {rows_earliest:,}")
print(f"rows at version {max(versions):<4}: {rows_latest:,}")

check(
    "time travel: the earliest version is a strict subset of the current table",
    rows_earliest <= rows_latest,
    f"{rows_earliest} vs {rows_latest}",
)

# COMMAND ----------

# MAGIC %md
# MAGIC And the timestamp form, which is how you answer a point-in-time question
# MAGIC without knowing the version number -- "show me the customer dimension as it
# MAGIC stood at the end of wave 2".
# MAGIC
# MAGIC The commit timestamp to use is the one from `DESCRIBE HISTORY`, not a
# MAGIC business timestamp. Table-time and business-time are different clocks, and
# MAGIC keeping them straight is most of what makes this understandable: the
# MAGIC *rows* carry 2017/2018 business dates, but the *table versions* were
# MAGIC committed when the pipeline actually ran.

# COMMAND ----------

commit_ts = (
    history.orderBy("version", ascending=False)
    .select("timestamp")
    .limit(1)
    .collect()[0]["timestamp"]
)
print(f"latest commit timestamp: {commit_ts}")

display(
    spark.sql(
        f"""
        SELECT
          (SELECT COUNT(*) FROM {SILVER}.customers TIMESTAMP AS OF '{commit_ts}') AS current_rows,
          (SELECT COUNT(*) FROM {SILVER}.customers)                               AS live_rows
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. `table_changes()` -- the audit trail of the silver tables
# MAGIC
# MAGIC Time travel gives you snapshots. The change data feed gives you the
# MAGIC *deltas between* snapshots: for every change, a `_change_type`
# MAGIC (`insert`, `update_preimage`, `update_postimage`, `delete`), the
# MAGIC `_commit_version` it belonged to, and a `_commit_timestamp`.
# MAGIC
# MAGIC This is enabled on every table the pipeline writes -- the silver layer sets
# MAGIC `delta.enableChangeDataFeed = 'true'` explicitly, and the pipeline sets it as
# MAGIC a default for everything else (see `resources/pipeline.yml`), which is what
# MAGIC lets you do this on the *bronze* tables too.

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT _change_type, COUNT(*) AS records
        FROM table_changes('{SILVER}.customers', {earliest})
        GROUP BY _change_type
        ORDER BY records DESC
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### An address move, before and after
# MAGIC
# MAGIC An SCD2 change is *two* feed rows: the old version gets its `__END_AT`
# MAGIC populated (`update_postimage`) or is closed and a new row appears. Pick any
# MAGIC customer with more than one version and read its feed.

# COMMAND ----------

moved = scalar(
    f"""
    SELECT customer_id
    FROM {SILVER}.customers
    GROUP BY customer_id
    HAVING COUNT(*) > 1
    LIMIT 1
    """
)
print("customer with history:", moved)

display(
    spark.sql(
        f"""
        SELECT
          _change_type,
          _commit_version,
          _commit_timestamp,
          customer_city,
          customer_state,
          __START_AT,
          __END_AT
        FROM table_changes('{SILVER}.customers', {earliest})
        WHERE customer_id = '{moved}'
        ORDER BY _commit_version, _change_type
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### The SCD2 subtlety that surprises people
# MAGIC
# MAGIC When a version is *slotted into the past* -- a late-arriving change whose
# MAGIC business time falls before a version that has already been written -- the
# MAGIC `__START_AT` of the following version has to move. That changes that row's
# MAGIC identity: on an SCD2 target the primary key is the business key **plus**
# MAGIC `coalesce(__START_AT, __END_AT)`.
# MAGIC
# MAGIC So a late arrival shows up in the feed as an `insert`/`delete` pair rather
# MAGIC than as an `update_preimage`/`update_postimage` pair. That is not
# MAGIC corruption; it is the visible cost of correcting history, and it is exactly
# MAGIC why the gold change feeds in `04_gold_change_feeds.py` are append-only: the
# MAGIC feed is the truth about what happened, even when the table it came from was
# MAGIC being reordered underneath it.
# MAGIC
# MAGIC You have already seen the *business* consequence of this in notebook 10
# MAGIC ("out-of-order arrival did not corrupt the result"). Here you see its
# MAGIC *bookkeeping* consequence.

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT
          COUNT(*)                                                AS feed_rows,
          COUNT(DISTINCT customer_id)                             AS customers_touched,
          MIN(_commit_version)                                    AS first_version,
          MAX(_commit_version)                                    AS last_version
        FROM table_changes('{SILVER}.customers', {earliest})
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. The gold change feeds -- the feed as an addressable table
# MAGIC
# MAGIC `table_changes()` needs a version range and a source that still has the feed
# MAGIC retained. The gold layer also materialises the feed as a streaming table
# MAGIC (`{gold}.<entity>_changes`), which is what a downstream subscriber would
# MAGIC actually consume: append-only, no version range required, and retaining the
# MAGIC history even after the silver table itself has moved on.
# MAGIC
# MAGIC Each row is a silver row plus `_change_type`, `_commit_version`,
# MAGIC `_commit_timestamp`, and three convenience booleans the pipeline added
# MAGIC (`_cdc_is_insert` / `_cdc_is_update` / `_cdc_is_delete`).

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT
          _change_type,
          _cdc_is_insert,
          _cdc_is_update,
          _cdc_is_delete,
          COUNT(*)                 AS records,
          MIN(_commit_version)     AS first_version,
          MAX(_commit_version)     AS last_version
        FROM {GOLD}.customers_changes
        GROUP BY ALL
        ORDER BY records DESC
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC The order lifecycle, straight off the `orders` feed. Every order is born,
# MAGIC moves through statuses and -- for the delivered ones -- ends. Reading it as
# MAGIC a feed rather than as a table is what lets a downstream system keep an
# MAGIC incremental view without re-reading the whole order table each night.

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT
          order_id,
          order_status,
          _change_type,
          _commit_version,
          _commit_timestamp
        FROM {GOLD}.orders_changes
        WHERE order_id IN (
          SELECT order_id FROM {GOLD}.orders_changes
          GROUP BY order_id
          HAVING COUNT(*) >= 5
          LIMIT 2
        )
        ORDER BY order_id, _commit_version, _change_type
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. The right to be forgotten
# MAGIC
# MAGIC The generator planted a handful of GDPR erasures: `op = 'd'` on
# MAGIC `customers`, described as "right-to-be-forgotten". That is the genuinely
# MAGIC hard case for a medallion architecture, because SCD2 exists to *keep*
# MAGIC history and erasure exists to *remove* it.
# MAGIC
# MAGIC The layering resolves it by separating two things that are easy to conflate:
# MAGIC
# MAGIC * **current state** (`gold.dim_customer`, `gold.customer_360`) is
# MAGIC   consumption. An erased customer must not be in it. `WHERE __END_AT IS NULL`
# MAGIC   delivers that for free: AUTO CDC closes the open version and opens
# MAGIC   nothing, so the customer simply falls out.
# MAGIC * **history** (`silver.customers`) is the audit trail. It still has the
# MAGIC   closed versions, which is what you would need to answer a regulator's
# MAGIC   "when did you hold this and what did you do with it" question.
# MAGIC
# MAGIC Those are two different retention policies attached to two different
# MAGIC tables. The demo implements the first, and documents the second. The final
# MAGIC cell shows what *physical* erasure would require.

# COMMAND ----------

erased = scalar(f"SELECT COUNT(DISTINCT customer_id) FROM {GOLD}.customers_changes WHERE _cdc_is_delete")
still_in_dim = scalar(
    f"""
    SELECT COUNT(*)
    FROM {GOLD}.dim_customer d
    WHERE d.customer_id IN (
      SELECT customer_id FROM {GOLD}.customers_changes WHERE _cdc_is_delete
    )
    """
)

print(f"customers erased in the stream : {erased:,}")
print(f"of those, still in dim_customer: {still_in_dim:,}")
print()

check(
    "GDPR: every erased customer is absent from the current-state dimension",
    still_in_dim == 0,
    f"{still_in_dim} leaked",
)

# COMMAND ----------

# MAGIC %md
# MAGIC The erased customers are gone from current state, but their *versions* are
# MAGIC still in silver -- all closed, none current. That is the audit trail.

# COMMAND ----------

display(
    spark.sql(
        f"""
        WITH erased AS (
          SELECT DISTINCT customer_id
          FROM {GOLD}.customers_changes
          WHERE _cdc_is_delete
        )
        SELECT
          c.customer_id,
          c.customer_city,
          c.customer_state,
          c.__START_AT,
          c.__END_AT,
          c.__END_AT IS NULL AS is_current
        FROM {SILVER}.customers c
        JOIN erased e ON c.customer_id = e.customer_id
        ORDER BY c.customer_id, c.__START_AT
        LIMIT 20
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### What physical erasure would actually take
# MAGIC
# MAGIC The above is *logical* erasure from the consumption layer. If a regulation
# MAGIC requires the bytes to be gone, three things have to happen, and it is worth
# MAGIC being precise about them because "just DELETE it" is not enough:
# MAGIC
# MAGIC 1. **Delete the rows** from every table that holds them -- silver, gold, and
# MAGIC    the change feeds, since a change feed retains the pre-image of an update.
# MAGIC 2. **Let retention expire, then `VACUUM`.** Delta keeps old files for the
# MAGIC    time-travel/retention window; until they are vacuumed the data is still
# MAGIC    on disk. Going below the default 7-day window needs
# MAGIC    `spark.databricks.delta.retentionDurationCheck.enabled = false`, which is
# MAGIC    a deliberate, audited decision, not a default.
# MAGIC 3. **Account for the landing zone.** The original events are still gzipped
# MAGIC    JSON in the volume. Erasing downstream does not erase upstream.
# MAGIC
# MAGIC Two mechanical notes for this repo:
# MAGIC
# MAGIC * streaming tables **cannot** be `RESTORE`d (`RESTORE` is not supported on
# MAGIC   them), so point-in-time recovery is done by full-refreshing the pipeline
# MAGIC   from the landing zone, not by restoring a version;
# MAGIC * the feed tables are append-only, so erasure there means `DELETE` plus
# MAGIC   `VACUUM` exactly as below -- the append-only property is about *reads*, not
# MAGIC   about immutability of the physical files.
# MAGIC
# MAGIC The statements are shown, not run: this notebook is a tour, and a demo that
# MAGIC silently deletes data is a bad demo. `99_reset_demo.py` is the one place
# MAGIC that is allowed to be destructive, and it asks first.

# COMMAND ----------

print(
    "-- Physical erasure of one subject (illustrative; not executed here)\n"
    "\n"
    "DELETE FROM workspace.retail_silver.customers      WHERE customer_id = '<id>';\n"
    "DELETE FROM workspace.retail_gold.customers_changes WHERE customer_id = '<id>';\n"
    "DELETE FROM workspace.retail_gold.dim_customer      WHERE customer_id = '<id>';\n"
    "-- ... every other table that carries the subject's PII ...\n"
    "\n"
    "SET spark.databricks.delta.retentionDurationCheck.enabled = false;\n"
    "VACUUM workspace.retail_silver.customers      RETAIN 0 HOURS;\n"
    "VACUUM workspace.retail_gold.customers_changes RETAIN 0 HOURS;\n"
    "\n"
    "-- and the raw events are still in the landing volume:\n"
    "--   databricks fs rm -r dbfs:/Volumes/workspace/retail_landing/cdc/events/customers"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Result

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
print("  30_reprocess_and_late_data.py  -- late data, replays and reprocessing")
print("  40_ops_event_log_dq.py         -- the data-quality metrics behind it all")
print("=" * 70)
