# Databricks notebook source
# MAGIC %md
# MAGIC # 10 - The medallion walkthrough
# MAGIC
# MAGIC The guided tour from a gzipped JSON file to a star schema. Run this with
# MAGIC the pipeline already updated at least once.
# MAGIC
# MAGIC The thing to watch for throughout: **the pipeline never has to be told
# MAGIC which events are late, duplicated or out of order.** Those are resolved by
# MAGIC configuration -- a sequence column that is business time rather than
# MAGIC arrival time -- not by code that sorts things out after the fact. Each
# MAGIC section below is a check that this actually held.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("bronze_schema", "retail_bronze", "Bronze schema")
dbutils.widgets.text("silver_schema", "retail_silver", "Silver schema")
dbutils.widgets.text("gold_schema", "retail_gold", "Gold schema")

CATALOG = dbutils.widgets.get("catalog")
BRONZE = f"{CATALOG}.{dbutils.widgets.get('bronze_schema')}"
SILVER = f"{CATALOG}.{dbutils.widgets.get('silver_schema')}"
GOLD = f"{CATALOG}.{dbutils.widgets.get('gold_schema')}"

_results = []

def check(label, condition, detail=""):
    """Record a pass/fail without stopping the tour.

    A demo that dies on the first unexpected number is useless. A demo that
    prints a green/red line and keeps going is a smoke test.
    """
    ok = bool(condition)
    _results.append((label, ok))
    print(f"  [{'PASS' if ok else 'FAIL'}] {label}{(' -- ' + detail) if detail else ''}")
    return ok

def table_exists(fqn):
    try:
        spark.sql(f"SELECT 1 FROM {fqn} LIMIT 1")
        return True
    except Exception:
        return False

def count(fqn):
    return spark.sql(f"SELECT COUNT(*) AS n FROM {fqn}").collect()[0]["n"]

print(f"bronze: {BRONZE}\nsilver: {SILVER}\ngold  : {GOLD}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Preflight
# MAGIC
# MAGIC If the pipeline has not run yet, everything below is meaningless, so say
# MAGIC so plainly instead of failing with a table-not-found error.

# COMMAND ----------

required = [
    f"{BRONZE}.orders_raw",
    f"{BRONZE}.orders_cdc",
    f"{SILVER}.orders",
    f"{SILVER}.customers",
    f"{GOLD}.fact_orders",
]
missing = [t for t in required if not table_exists(t)]

if missing:
    print("The pipeline has not created these yet:")
    for t in missing:
        print(f"  - {t}")
    print(
        "\nUpload a wave and run the pipeline, then re-run this notebook:\n"
        "  python tools/upload_landing.py --wave 1\n"
        "  databricks bundle run retail_medallion_cdc"
    )
    dbutils.notebook.exit("pipeline not initialised")

print("all required tables exist\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Bronze -- raw in, conformed out, nothing lost
# MAGIC
# MAGIC Bronze has two layers on purpose:
# MAGIC
# MAGIC * `*_raw` -- the Debezium envelope exactly as it arrived, never parsed,
# MAGIC   never filtered. If a downstream rule turns out to be wrong, the original
# MAGIC   bytes are still here.
# MAGIC * `*_cdc` -- the same rows with the envelope unpacked: `after` flattened,
# MAGIC   keys coalesced with `before`, business timestamps cast.
# MAGIC
# MAGIC The invariant to hold onto is that `raw` is the union of `cdc` and
# MAGIC `quarantine`. A record is never dropped by the conform step without being
# MAGIC written down somewhere.

# COMMAND ----------

print("orders, row counts by layer:")
raw_n = count(f"{BRONZE}.orders_raw")
cdc_n = count(f"{BRONZE}.orders_cdc")
quar_n = count(f"{CATALOG}.retail_ops.orders_quarantine")

print(f"  orders_raw        : {raw_n:,}")
print(f"  orders_cdc        : {cdc_n:,}")
print(f"  orders_quarantine : {quar_n:,}")
print(f"  accounted for     : {cdc_n + quar_n:,} / {raw_n:,}")
print()

check(
    "bronze: raw == conformed + quarantine (nothing silently dropped)",
    cdc_n + quar_n == raw_n,
    f"{cdc_n + quar_n} vs {raw_n}",
)

# COMMAND ----------

# MAGIC %md
# MAGIC The quarantined rows are worth one look, because they are the payoff for
# MAGIC writing expectations instead of just trusting the producer. Three distinct
# MAGIC failure modes, each labelled with the reason it was rejected:

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT _quarantine_reason, COUNT(*) AS records
        FROM {CATALOG}.retail_ops.orders_quarantine
        GROUP BY _quarantine_reason
        ORDER BY records DESC
        """
    )
)

display(spark.sql(f"SELECT * FROM {CATALOG}.retail_ops.orders_quarantine LIMIT 5"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Silver, SCD1 -- one row per order, latest state by *business* time
# MAGIC
# MAGIC `silver.orders` is the current state of every order. No `__START_AT` /
# MAGIC `__END_AT`, because SCD1 keeps no history -- but it is *not* naive
# MAGIC last-write-wins: the winner is the change with the highest `event_ts`,
# MAGIC regardless of which file happened to deliver it.

# COMMAND ----------

silver_orders = count(f"{SILVER}.orders")
distinct_orders = spark.sql(
    f"SELECT COUNT(DISTINCT order_id) AS n FROM {BRONZE}.orders_cdc"
).collect()[0]["n"]

print(f"distinct order_ids in the change stream : {distinct_orders:,}")
print(f"rows in silver.orders                   : {silver_orders:,}")
print()

check(
    "silver SCD1: exactly one row per order, despite many changes per order",
    silver_orders == distinct_orders,
    f"{silver_orders} vs {distinct_orders}",
)

display(
    spark.sql(
        f"""
        SELECT order_status, COUNT(*) AS orders
        FROM {SILVER}.orders
        GROUP BY order_status
        ORDER BY orders DESC
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. The proof: out-of-order arrival did not corrupt the result
# MAGIC
# MAGIC This is the section to linger on, because it is the whole point of the
# MAGIC demo.
# MAGIC
# MAGIC Each event carries the wave (file) it *arrived* in inside `_source_file`.
# MAGIC The generator deliberately delivers a slice of events one or two waves
# MAGIC after they should have arrived. So for some orders, the event that arrived
# MAGIC **last** is not the event that is **newest**.
# MAGIC
# MAGIC Under a naive "last write wins" implementation, a late-arriving old event
# MAGIC would clobber a newer value. Under `SEQUENCE BY _event_ts` it cannot.
# MAGIC
# MAGIC The query below finds orders where the two rules disagree, so the
# MAGIC difference is visible rather than asserted.

# COMMAND ----------

WAVE_IN_FILE = "CAST(regexp_extract(_source_file, '_w([0-9]+)_', 1) AS INT)"

display(
    spark.sql(
        f"""
        WITH events AS (
          SELECT
            order_id,
            _event_ts,
            _lsn,
            <WAVE> AS arrival_wave,
            order_status
          FROM {BRONZE}.orders_cdc
        ),
        ranked AS (
          SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _event_ts DESC)             AS by_business_time,
            ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY arrival_wave DESC, _lsn DESC) AS by_arrival
          FROM events
        ),
        contested AS (
          SELECT order_id
          FROM ranked
          WHERE by_business_time = 1 AND by_arrival <> 1
        )
        SELECT
          e.order_id,
          e.arrival_wave,
          e.order_status,
          e._event_ts,
          e.by_business_time,
          e.by_arrival
        FROM ranked e
        JOIN contested c ON e.order_id = c.order_id
        ORDER BY e.order_id, e._event_ts
        LIMIT 40
        """.replace("<WAVE>", WAVE_IN_FILE)
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC Rows where `by_business_time = 1` but `by_arrival <> 1` are exactly the
# MAGIC cases a naive pipeline would get wrong. The assertion is that
# MAGIC `silver.orders` agrees with the *business-time* winner and not the
# MAGIC arrival-order winner:

# COMMAND ----------

mismatch = spark.sql(
    f"""
    WITH events AS (
      SELECT order_id, _event_ts, _lsn, order_status, {WAVE_IN_FILE} AS arrival_wave
      FROM {BRONZE}.orders_cdc
    ),
    ranked AS (
      SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _event_ts DESC)              AS by_business_time,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY arrival_wave DESC, _lsn DESC) AS by_arrival
      FROM events
    ),
    winners AS (
      SELECT
        order_id,
        MAX(CASE WHEN by_business_time = 1 THEN order_status END) AS status_by_business_time,
        MAX(CASE WHEN by_arrival         = 1 THEN order_status END) AS status_by_arrival
      FROM ranked
      GROUP BY order_id
      HAVING status_by_business_time <> status_by_arrival
    )
    SELECT
      COUNT(*) AS orders_where_the_two_rules_disagree,
      SUM(CASE WHEN w.status_by_business_time = s.order_status THEN 1 ELSE 0 END) AS silver_picked_business_time,
      SUM(CASE WHEN w.status_by_arrival       = s.order_status THEN 1 ELSE 0 END) AS silver_picked_arrival
    FROM winners w
    JOIN {SILVER}.orders s ON w.order_id = s.order_id
    """
).collect()[0]

print(dict(mismatch))
disagree = mismatch["orders_where_the_two_rules_disagree"]
picked_bt = mismatch["silver_picked_business_time"]

if disagree == 0:
    print("\nNo disagreements in this slice of data (upload more waves for a stronger test).")
else:
    check(
        f"silver resolved all {disagree} contested orders by business time, never by arrival",
        picked_bt == disagree,
        f"{picked_bt}/{disagree} correct",
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Silver SCD2 -- history, with real dates
# MAGIC
# MAGIC Because `SEQUENCE BY` is business time, an SCD2 backfill produces
# MAGIC intervals with the **actual** 2017/2018 dates from the source rather than
# MAGIC the wall-clock time of the pipeline run. A late-arriving change is slotted
# MAGIC into the correct historical position.
# MAGIC
# MAGIC Customers move house; each move opens a new version and closes the
# MAGIC previous one.

# COMMAND ----------

display(
    spark.sql(
        f"""
        WITH moved AS (
          SELECT customer_id
          FROM {SILVER}.customers
          GROUP BY customer_id
          HAVING COUNT(*) > 1
          LIMIT 3
        )
        SELECT
          c.customer_id,
          c.customer_city,
          c.customer_state,
          c.customer_zip_code_prefix,
          c.__START_AT,
          c.__END_AT
        FROM {SILVER}.customers c
        JOIN moved m ON c.customer_id = m.customer_id
        ORDER BY c.customer_id, c.__START_AT
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC The same change stream, a second time, reshaped into a delivery-lifecycle
# MAGIC timeline. `orders_status_history` reads the *same* bronze table as
# MAGIC `silver.orders` -- two flows over one source, no duplicated ingestion.

# COMMAND ----------

display(
    spark.sql(
        f"""
        WITH busy AS (
          SELECT order_id
          FROM {SILVER}.orders_status_history
          GROUP BY order_id
          HAVING COUNT(*) >= 4
          LIMIT 3
        )
        SELECT
          h.order_id,
          h.order_status,
          h.__START_AT,
          h.__END_AT,
          h.__END_AT IS NULL AS is_current
        FROM {SILVER}.orders_status_history h
        JOIN busy b ON h.order_id = b.order_id
        ORDER BY h.order_id, h.__START_AT
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Schema drift, arriving in silver by itself
# MAGIC
# MAGIC The two drifted columns are in silver without a single line of pipeline
# MAGIC code naming them. They flowed through because the conform step selects
# MAGIC `after.* EXCEPT (the columns it must rewrite)` rather than an explicit
# MAGIC column list, so a new source column is simply included.
# MAGIC
# MAGIC Note the types. Auto Loader infers from JSON, and JSON has no date type, so
# MAGIC `product_launch_date` lands as a **string**. That is the honest outcome,
# MAGIC and it is why the gold layer does not depend on these columns.

# COMMAND ----------

silver_products_cols = [
    f.name for f in spark.sql(f"DESCRIBE {SILVER}.products").collect()
]
drift_present = [c for c in silver_products_cols if c in ("product_launch_date", "product_name_length")]
print("drifted columns now present in silver.products:", drift_present)

if drift_present:
    display(
        spark.sql(
            f"""
            SELECT
              product_id,
              product_name_lenght,
              product_name_length,
              product_launch_date,
              __START_AT
            FROM {SILVER}.products
            WHERE product_launch_date IS NOT NULL
            LIMIT 5
            """
        )
    )
else:
    print("Not visible yet -- these arrive at wave 4. Upload more waves and re-run.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Gold -- where the change stream becomes something answerable
# MAGIC
# MAGIC Gold reads silver as *current state* (`__END_AT IS NULL` on SCD2 tables)
# MAGIC and is built as materialized views, because aggregating over the output of
# MAGIC an AUTO CDC flow is a batch-shaped operation, not a streaming one.

# COMMAND ----------

for name in ("dim_customer", "dim_product", "dim_seller", "fact_orders", "fact_order_items"):
    print(f"  {name:<20} {count(f'{GOLD}.{name}'):>10,} rows")

print()
display(
    spark.sql(
        f"""
        SELECT product_category, items_sold, orders, revenue
        FROM {GOLD}.mart_category_revenue
        ORDER BY revenue DESC
        LIMIT 10
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Delivery performance
# MAGIC
# MAGIC Note `late_rate_pct` is computed from the base timestamps
# MAGIC (`delivered_customer_date > estimated_delivery_date`). The source also
# MAGIC ships its own drifted `delivery_sla_breach` flag from wave 3. The next
# MAGIC section reconciles the two -- which is the sort of thing you actually want
# MAGIC to do when a producer adds a derived column.

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT
          purchase_month,
          COUNT(*)                             AS orders,
          ROUND(AVG(days_to_deliver), 2)       AS avg_days_to_deliver,
          ROUND(AVG(days_vs_estimate), 2)      AS avg_days_vs_estimate,
          ROUND(100.0 * AVG(CASE WHEN is_late THEN 1 ELSE 0 END), 2) AS late_rate_pct
        FROM {GOLD}.fact_orders
        WHERE is_delivered
        GROUP BY purchase_month
        ORDER BY purchase_month
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reconcile our SLA derivation against the source's drifted flag
# MAGIC
# MAGIC If the pipeline has only been run up to wave 2, `delivery_sla_breach` does
# MAGIC not exist yet and this section says so rather than failing.

# COMMAND ----------

order_cols = [f.name for f in spark.sql(f"DESCRIBE {SILVER}.orders").collect()]

if "delivery_sla_breach" not in order_cols:
    print(
        "silver.orders has no delivery_sla_breach column yet.\n"
        "That column arrives with wave 3 -- upload more waves and re-run the pipeline."
    )
else:
    display(
        spark.sql(
            f"""
            SELECT
              o.delivery_sla_breach                                   AS source_flag,
              f.is_late                                               AS derived_flag,
              COUNT(*)                                                AS orders
            FROM {SILVER}.orders o
            JOIN {GOLD}.fact_orders f ON o.order_id = f.order_id
            WHERE o.order_delivered_customer_date IS NOT NULL
            GROUP BY o.delivery_sla_breach, f.is_late
            ORDER BY orders DESC
            """
        )
    )
    print(
        "Any rows off the agreement diagonal are the interesting ones: the source's\n"
        "own flag and our recomputation disagreeing is exactly what you want to\n"
        "detect early, rather than trusting either side blindly."
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
print("  20_time_travel_and_cdf.py      -- audit the history and the change feeds")
print("  30_reprocess_and_late_data.py  -- late data, replays and reprocessing")
print("  40_ops_event_log_dq.py         -- the data-quality metrics behind it all")
print("=" * 70)
