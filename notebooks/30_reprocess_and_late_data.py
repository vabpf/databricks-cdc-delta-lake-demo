# Databricks notebook source
# MAGIC %md
# MAGIC # 30 - Late data, replays and reprocessing
# MAGIC
# MAGIC Notebook 10 asserted that the pipeline resolves out-of-order arrivals by
# MAGIC business time. This one earns that claim by **manufacturing the hard cases
# MAGIC by hand** and watching them land.
# MAGIC
# MAGIC Three things get demonstrated, in increasing order of how much they
# MAGIC surprise people:
# MAGIC
# MAGIC 1. **Replays are idempotent.** The generator delivers a slice of events
# MAGIC    twice (at-least-once delivery). Silver still has exactly one row per key.
# MAGIC 2. **Late data is resolved by business time, not arrival time.** We write
# MAGIC    two events for one order into a single file, deliberately in the wrong
# MAGIC    order, and check which one wins.
# MAGIC 3. **History tracking is opt-in per column.** A seller zip-code change is
# MAGIC    *not* a new version; a seller city change *is*. Same table, same kind of
# MAGIC    event, different outcome -- because of `TRACK HISTORY ON
# MAGIC    (seller_city, seller_state)` in `03_silver_auto_cdc.sql`.
# MAGIC
# MAGIC The hand-crafted events are written to the landing volume under
# MAGIC `*_w99_*` filenames so they are obviously not part of the generated waves.
# MAGIC The notebook writes them only if they are not already in bronze, so running
# MAGIC it repeatedly is safe.

# COMMAND ----------

import json
import time
from datetime import datetime, timedelta

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("bronze_schema", "retail_bronze", "Bronze schema")
dbutils.widgets.text("silver_schema", "retail_silver", "Silver schema")
dbutils.widgets.text("landing_schema", "retail_landing", "Landing schema")
dbutils.widgets.text("volume_name", "cdc", "Landing volume name")
dbutils.widgets.text("pipeline_name", "retail_medallion_cdc", "Pipeline name")
dbutils.widgets.text("pipeline_id", "", "Pipeline id (optional; looked up by name if blank)")
dbutils.widgets.dropdown("auto_run", "yes", ["yes", "no"], "Run the pipeline from this notebook?")

CATALOG = dbutils.widgets.get("catalog")
BRONZE = f"{CATALOG}.{dbutils.widgets.get('bronze_schema')}"
SILVER = f"{CATALOG}.{dbutils.widgets.get('silver_schema')}"
VOLUME = f"/Volumes/{CATALOG}/{dbutils.widgets.get('landing_schema')}/{dbutils.widgets.get('volume_name')}"
PIPELINE_NAME = dbutils.widgets.get("pipeline_name")
AUTO_RUN = dbutils.widgets.get("auto_run") == "yes"

# Sequence numbers for the injected events. Large on purpose so they sort after
# everything the generator emitted.
SELLER_LSN = 900000001
ORDER_LSN_DELIVERED = 900000002
ORDER_LSN_SHIPPED = 900000003

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


def fmt(value):
    """Olist-style timestamp string, as the producer writes it."""
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return value.strftime("%Y-%m-%d %H:%M:%S")


def envelope(op, entity, source_table, lsn, event_ts, after, before=None, snapshot=False):
    """Build one Debezium-envelope event, exactly as tools/generate_cdc.py does."""
    if isinstance(event_ts, str):
        event_ts = datetime.strptime(event_ts, "%Y-%m-%d %H:%M:%S")
    return {
        "op": op,
        "event_ts": event_ts.strftime("%Y-%m-%dT%H:%M:%S.000"),
        "ts_ms": int(event_ts.timestamp() * 1000),
        "lsn": lsn,
        "source": {
            "connector": "olist-cdc-sim",
            "version": "1.0.0",
            "entity": entity,
            "table": source_table,
            "snapshot": snapshot,
        },
        "before": before,
        "after": after,
    }


def put(path, events):
    """Write JSONL to a Unity Catalog volume, one event per line."""
    body = "\n".join(json.dumps(e, separators=(",", ":")) for e in events) + "\n"
    dbutils.fs.put(path, body, True)
    print(f"wrote {len(events)} event(s) -> {path}")
    return path


def ingested(entity, lsns):
    """True if any of these injected sequence numbers is already in <entity>_cdc."""
    listed = ", ".join(str(x) for x in lsns)
    return bool(scalar(f"SELECT COUNT(*) FROM {BRONZE}.{entity}_cdc WHERE _lsn IN ({listed})", 0))


print(f"bronze : {BRONZE}")
print(f"silver : {SILVER}")
print(f"volume : {VOLUME}")
print(f"auto_run: {AUTO_RUN}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Preflight, and how the pipeline gets run
# MAGIC
# MAGIC Running the pipeline is a *separate* action from this notebook: it is a
# MAGIC Lakeflow pipeline, not a script this notebook can just call. Two ways:
# MAGIC
# MAGIC * **from here**, if `auto_run = yes`, using the Databricks SDK
# MAGIC   (`pipelines.start_update`), polling until the update finishes. This is the
# MAGIC   default and makes the notebook self-contained.
# MAGIC * **from the CLI**, if `auto_run = no`:
# MAGIC   `databricks bundle run retail_medallion_cdc`
# MAGIC
# MAGIC The pipeline id is looked up in `system.lakeflow.pipelines`; if that system
# MAGIC table is unavailable, set the `pipeline_id` widget by hand.

# COMMAND ----------

if not scalar(f"SELECT COUNT(*) FROM {SILVER}.orders", 0):
    print("The pipeline has not produced silver.orders yet.")
    print("Upload a wave and run the pipeline first:")
    print("  python tools/upload_landing.py --wave 1")
    print("  databricks bundle run retail_medallion_cdc")
    dbutils.notebook.exit("pipeline not initialised")

PIPELINE_ID = dbutils.widgets.get("pipeline_id") or (scalar(
    f"SELECT pipeline_id FROM system.lakeflow.pipelines WHERE name = '{PIPELINE_NAME}' LIMIT 1",
    "",
) or "")
print(f"pipeline_id: {PIPELINE_ID or '(not resolved -- auto_run will fall back to manual)'}")


def run_pipeline(full_refresh=False):
    """Ask the pipeline for an update and wait for it. Returns True on success."""
    if not PIPELINE_ID:
        print("No pipeline_id; cannot run it from here.")
        return False
    try:
        from databricks.sdk import WorkspaceClient
    except ImportError:
        print("databricks-sdk not available in this environment.")
        return False
    try:
        w = WorkspaceClient()
        resp = w.pipelines.start_update(pipeline_id=PIPELINE_ID, full_refresh=full_refresh)
        update_id = resp.update_id
        print(f"  started update {update_id} (full_refresh={full_refresh})")
        while True:
            update = w.pipelines.get_update(pipeline_id=PIPELINE_ID, update_id=update_id).update
            state = getattr(update.state, "value", None) or str(update.state)
            print(f"  state: {state}")
            if state in ("COMPLETED", "FAILED", "CANCELED"):
                return state == "COMPLETED"
            time.sleep(15)
    except Exception as exc:  # noqa: BLE001 - many SDK failure shapes
        print(f"  could not run the pipeline automatically: {exc}")
        return False

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Replays -- why at-least-once delivery is not a problem
# MAGIC
# MAGIC The generator re-delivers ~3% of events one or two waves later, byte for
# MAGIC byte. That is what a real CDC feed does: at-least-once delivery is the
# MAGIC contract, because exactly-once delivery between two systems is not
# MAGIC achievable in general.
# MAGIC
# MAGIC So the *consumer* has to be idempotent. Here is the proof, using bronze
# MAGIC (which keeps the duplicates) and silver (which must not).

# COMMAND ----------

raw_events = scalar(f"SELECT COUNT(*) FROM {BRONZE}.orders_cdc")
distinct_keys = scalar(f"SELECT COUNT(DISTINCT order_id) FROM {BRONZE}.orders_cdc")
silver_rows = scalar(f"SELECT COUNT(*) FROM {SILVER}.orders")

# A replay is an identical envelope: same key, same image, same sequence number.
replayed = scalar(
    f"""
    SELECT COUNT(*) FROM (
      SELECT order_id, order_status, _event_ts, _lsn, COUNT(*) AS copies
      FROM {BRONZE}.orders_cdc
      GROUP BY order_id, order_status, _event_ts, _lsn
      HAVING COUNT(*) > 1
    )
    """
)

print(f"order events in the stream    : {raw_events:,}")
print(f"distinct order_ids            : {distinct_keys:,}")
print(f"rows in silver.orders         : {silver_rows:,}")
print(f"redelivered (duplicate) groups: {replayed:,}")
print()

check(
    "silver holds exactly one row per key despite duplicate deliveries",
    silver_rows == distinct_keys,
    f"{silver_rows} vs {distinct_keys}",
)
check("the raw stream really does contain duplicates", replayed > 0, f"{replayed} groups")

# COMMAND ----------

# MAGIC %md
# MAGIC The pipeline does not deduplicate as a separate step. Idempotency is a
# MAGIC *property* of `AUTO CDC`: it folds the change stream by key, so applying the
# MAGIC same change twice is the same as applying it once. There is no
# MAGIC `dropDuplicates` anywhere, and that is the point -- a dedup step would have
# MAGIC to guess an identity for a change, and the sequence column already provides
# MAGIC one.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Manufacture two hard cases
# MAGIC
# MAGIC Both hand-crafted change sets are written now, before anything is ingested,
# MAGIC so a single pipeline run covers both.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2a. Two events for one order, written in the wrong order
# MAGIC
# MAGIC We pick one delivered order and write **two update events for it into a
# MAGIC single new file**, in this order:
# MAGIC
# MAGIC | line | op | status | business time |
# MAGIC |---|---|---|---|
# MAGIC | 1 (first) | `u` | `delivered` | T + 1s |
# MAGIC | 2 (last)  | `u` | `shipped`   | T - 1 day |
# MAGIC
# MAGIC The event that arrives **last** is the *older* one. A naive
# MAGIC last-write-wins consumer would end up with the order back in `shipped`,
# MAGIC delivery dates blanked. `SEQUENCE BY STRUCT(_event_ts, _lsn)` means the
# MAGIC `delivered` event wins, because that is what happened last in the business's
# MAGIC timeline.

# COMMAND ----------

ORDER_FILE = f"{VOLUME}/events/orders/orders_w99_p001_late.jsonl"

if ingested("orders", (ORDER_LSN_DELIVERED, ORDER_LSN_SHIPPED)):
    print("Hand-crafted order events already in bronze; not writing them again.")
    ORDER_ID = scalar(
        f"SELECT order_id FROM {BRONZE}.orders_cdc WHERE _lsn = {ORDER_LSN_DELIVERED} LIMIT 1",
        "(unknown)",
    )
    print("order:", ORDER_ID)
else:
    order = spark.sql(
        f"""
        SELECT order_id, customer_id, order_status,
               order_purchase_timestamp, order_approved_at,
               order_delivered_carrier_date, order_delivered_customer_date,
               order_estimated_delivery_date
        FROM {SILVER}.orders
        WHERE order_status = 'delivered'
          AND order_delivered_customer_date IS NOT NULL
          AND order_delivered_carrier_date IS NOT NULL
        ORDER BY order_id
        LIMIT 1
        """
    ).collect()[0]

    ORDER_ID = order["order_id"]
    base_ts = scalar(
        f"SELECT MAX(_event_ts) FROM {BRONZE}.orders_cdc WHERE order_id = '{ORDER_ID}'"
    ) or datetime(2018, 1, 1)
    print("order:", ORDER_ID)
    print("latest known business time:", base_ts)

    base_image = {
        "order_id": ORDER_ID,
        "customer_id": order["customer_id"],
        "order_status": "delivered",
        "order_purchase_timestamp": fmt(order["order_purchase_timestamp"]),
        "order_approved_at": fmt(order["order_approved_at"]),
        "order_delivered_carrier_date": fmt(order["order_delivered_carrier_date"]),
        "order_delivered_customer_date": fmt(order["order_delivered_customer_date"]),
        "order_estimated_delivery_date": fmt(order["order_estimated_delivery_date"]),
    }
    delivered = dict(base_image)                      # the true, newer state
    shipped = dict(base_image)                        # the older state, arriving late
    shipped["order_status"] = "shipped"
    shipped["order_delivered_customer_date"] = None   # not delivered yet at that time

    events = [
        envelope("u", "orders", "olist.orders", ORDER_LSN_DELIVERED,
                 base_ts + timedelta(seconds=1), after=delivered, before=base_image),
        envelope("u", "orders", "olist.orders", ORDER_LSN_SHIPPED,
                 base_ts - timedelta(days=1), after=shipped, before=base_image),
    ]
    put(ORDER_FILE, events)

# COMMAND ----------

# MAGIC %md
# MAGIC The important thing is the *order of the lines*: line 1 is `delivered`,
# MAGIC line 2 is `shipped`.

# COMMAND ----------

try:
    display(spark.read.text(ORDER_FILE))
except Exception as exc:  # noqa: BLE001
    print(f"could not read the injected file: {exc}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2b. A seller zip-only change
# MAGIC
# MAGIC `silver.sellers` is SCD2, but only on `(seller_city, seller_state)`:
# MAGIC
# MAGIC ```sql
# MAGIC   STORED AS SCD TYPE 2
# MAGIC   TRACK HISTORY ON (seller_city, seller_state);
# MAGIC ```
# MAGIC
# MAGIC So a **zip-code** correction is applied to the open version in place -- no
# MAGIC new row -- while a **city** change cuts a new version. The seeded data never
# MAGIC shows this, because every generated relocation changes city, state *and*
# MAGIC zip together. So we inject a zip-only change to isolate it.
# MAGIC
# MAGIC We deliberately give it a business time *after* the seller's current open
# MAGIC version, so it is unambiguously the latest change and the outcome does not
# MAGIC depend on slotting subtleties.

# COMMAND ----------

SELLER_FILE = f"{VOLUME}/events/sellers/sellers_w99_p001_late.jsonl"

if ingested("sellers", (SELLER_LSN,)):
    print("Seller event already in bronze; not writing it again.")
    SELLER_ID = scalar(
        f"SELECT seller_id FROM {BRONZE}.sellers_cdc WHERE _lsn = {SELLER_LSN} LIMIT 1",
        "(unknown)",
    )
    print("seller:", SELLER_ID)
else:
    seller = spark.sql(
        f"""
        SELECT seller_id, seller_city, seller_state, seller_zip_code_prefix, __START_AT
        FROM {SILVER}.sellers
        WHERE __END_AT IS NULL
          AND seller_id IS NOT NULL
          AND seller_id NOT IN (
            SELECT seller_id FROM {SILVER}.sellers WHERE __END_AT IS NOT NULL
          )
        ORDER BY seller_id
        LIMIT 1
        """
    ).collect()[0]

    SELLER_ID = seller["seller_id"]
    start = seller["__START_AT"] or datetime(2018, 1, 1)
    seller_ts = max(start + timedelta(seconds=1), datetime(2018, 1, 1))

    current_zip = str(seller["seller_zip_code_prefix"])
    new_zip = "01310" if current_zip != "01310" else "20040"
    seller_before = {
        "seller_id": SELLER_ID,
        "seller_zip_code_prefix": current_zip,
        "seller_city": seller["seller_city"],
        "seller_state": seller["seller_state"],
    }
    seller_after = dict(seller_before)
    seller_after["seller_zip_code_prefix"] = new_zip

    put(SELLER_FILE, [envelope(
        "u", "sellers", "olist.sellers", SELLER_LSN, seller_ts,
        after=seller_after, before=seller_before,
    )])
    print(f"seller {SELLER_ID}: {seller['seller_city']}/{seller['seller_state']} "
          f"zip {current_zip} -> {new_zip}")

SELLER_VERSIONS_BEFORE = scalar(
    f"SELECT COUNT(*) FROM {SILVER}.sellers WHERE seller_id = '{SELLER_ID}'", 0
)
SELLER_ZIP_BEFORE = scalar(
    f"""
    SELECT MAX(seller_zip_code_prefix) FROM {SILVER}.sellers
    WHERE seller_id = '{SELLER_ID}' AND __END_AT IS NULL
    """,
    "",
)
# What the injected event actually carried, read back from bronze. This is the
# durable record of the target zip; the current-state value changes the moment
# the pipeline ingests, so the assertion below must not rely on it.
INJECTED_SELLER_ZIP = scalar(
    f"SELECT seller_zip_code_prefix FROM {BRONZE}.sellers_cdc WHERE _lsn = {SELLER_LSN} LIMIT 1",
    "",
)
print(f"seller {SELLER_ID} before ingestion: "
      f"{SELLER_VERSIONS_BEFORE} version(s), zip {SELLER_ZIP_BEFORE} "
      f"(injected {INJECTED_SELLER_ZIP})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Ingest the new file -- one pipeline run

# COMMAND ----------

if AUTO_RUN:
    if run_pipeline():
        print("\npipeline update complete")
    else:
        print("\nautomated run did not complete -- run it manually:")
        print("  databricks bundle run retail_medallion_cdc")
else:
    print("auto_run = no. Run the pipeline now:")
    print("  databricks bundle run retail_medallion_cdc")

# COMMAND ----------

if not (ingested("orders", (ORDER_LSN_DELIVERED, ORDER_LSN_SHIPPED))
        and ingested("sellers", (SELLER_LSN,))):
    print(
        "The hand-crafted events are not in bronze yet.\n"
        "Run the pipeline and then re-run this notebook:\n"
        "  databricks bundle run retail_medallion_cdc"
    )
    dbutils.notebook.exit("hand-crafted events not ingested")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Late data -- did business time win?

# COMMAND ----------

resolved = spark.sql(
    f"""
    SELECT
      order_status,
      order_delivered_customer_date IS NOT NULL AS has_delivery_date
    FROM {SILVER}.orders
    WHERE order_id = '{ORDER_ID}'
    """
).collect()[0]

print(f"order {ORDER_ID}:")
print(f"  silver status         : {resolved['order_status']}")
print(f"  delivery date present : {resolved['has_delivery_date']}")
print()

check(
    "late/out-of-order: the older 'shipped' event did NOT clobber the newer 'delivered' state",
    resolved["order_status"] == "delivered" and bool(resolved["has_delivery_date"]),
    f"status={resolved['order_status']}",
)

# COMMAND ----------

# MAGIC %md
# MAGIC And the stream itself, ordered by business time, so the reversal is visible:
# MAGIC the `shipped` event has the *highest* `_lsn` (it arrived last) and the
# MAGIC *lowest* business time. That is the disagreement the sequence column
# MAGIC resolves.

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT
          _lsn,
          order_status AS status_in_stream,
          _event_ts,
          row_number() OVER (ORDER BY _lsn DESC) AS arrival_rank
        FROM {BRONZE}.orders_cdc
        WHERE order_id = '{ORDER_ID}'
        ORDER BY _event_ts
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. `TRACK HISTORY ON` -- same table, one kind of change that does not
# MAGIC cut history

# COMMAND ----------

seller_now = spark.sql(
    f"""
    SELECT COUNT(*) AS versions,
           MAX(CASE WHEN __END_AT IS NULL THEN seller_zip_code_prefix END) AS current_zip,
           MAX(CASE WHEN __END_AT IS NULL THEN seller_city END)            AS current_city
    FROM {SILVER}.sellers
    WHERE seller_id = '{SELLER_ID}'
    """
).collect()[0]

print(f"seller {SELLER_ID}:")
print(f"  versions    : {SELLER_VERSIONS_BEFORE} -> {seller_now['versions']}")
print(f"  current zip : {SELLER_ZIP_BEFORE} -> {seller_now['current_zip']}")
print(f"  current city: {seller_now['current_city']}")
print()

check(
    "TRACK HISTORY ON (city, state): a zip-only change updates in place, no new version",
    seller_now["versions"] == SELLER_VERSIONS_BEFORE
    and str(seller_now["current_zip"]) == str(INJECTED_SELLER_ZIP),
    f"versions {SELLER_VERSIONS_BEFORE}->{seller_now['versions']}, "
    f"zip now {seller_now['current_zip']} (injected {INJECTED_SELLER_ZIP})",
)

# COMMAND ----------

# MAGIC %md
# MAGIC For contrast, a seller that *did* relocate in the seeded data has more than
# MAGIC one version -- because city and state changed.

# COMMAND ----------

display(
    spark.sql(
        f"""
        WITH relocated AS (
          SELECT seller_id
          FROM {SILVER}.sellers
          WHERE seller_id IS NOT NULL
          GROUP BY seller_id
          HAVING COUNT(*) > 1
          LIMIT 2
        )
        SELECT s.seller_id, s.seller_city, s.seller_state, s.__START_AT, s.__END_AT
        FROM {SILVER}.sellers s
        JOIN relocated r ON s.seller_id = r.seller_id
        ORDER BY s.seller_id, s.__START_AT
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Reprocessing -- incremental by default, full refresh on demand
# MAGIC
# MAGIC Every run so far has been **incremental**. Auto Loader remembers which files
# MAGIC it has already read (transactionally, in the pipeline's checkpoint), so a run
# MAGIC after a wave upload only reads the new files. That is why the injected
# MAGIC events were picked up without re-reading the 100+ generated files.
# MAGIC
# MAGIC A **full refresh** is the opposite: recompute every dataset from scratch,
# MAGIC resetting the incremental state. It is the right tool when you have changed
# MAGIC *pipeline logic* and need the existing data re-derived -- not when you have
# MAGIC new data, which the incremental run already handles.
# MAGIC
# MAGIC ```bash
# MAGIC databricks bundle run retail_medallion_cdc --full-refresh
# MAGIC # or, against the pipeline directly:
# MAGIC databricks pipelines start-update --pipeline-id <id> --full-refresh
# MAGIC ```
# MAGIC
# MAGIC Because every layer is idempotent (AUTO CDC by key, gold marts by
# MAGIC recomputation), a full refresh over the same landing zone produces the same
# MAGIC *logical* result. The cells below run one and check exactly that -- the row
# MAGIC counts must not move.

# COMMAND ----------

before = {
    name: scalar(f"SELECT COUNT(*) FROM {SILVER}.{name}")
    for name in ("customers", "orders", "order_items", "order_payments", "order_reviews")
}
print("silver row counts before full refresh:")
for name, n in before.items():
    print(f"  {name:<16} {n:>8,}")

# COMMAND ----------

if AUTO_RUN and PIPELINE_ID:
    print("running a full refresh ...")
    ok = run_pipeline(full_refresh=True)
    print("full refresh completed" if ok else "full refresh did not complete")
else:
    print("Skipping the automated full refresh (auto_run=no or no pipeline_id).")
    print("To do it by hand:")
    print("  databricks bundle run retail_medallion_cdc --full-refresh")

# COMMAND ----------

after = {name: scalar(f"SELECT COUNT(*) FROM {SILVER}.{name}") for name in before}
print("silver row counts after full refresh:")
for name, n in after.items():
    print(f"  {name:<16} {n:>8,}   (before {before[name]:,})")

check(
    "full refresh is idempotent: reprocessing every file did not change the result",
    all(after[name] == before[name] for name in before),
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
print("  40_ops_event_log_dq.py  -- the data-quality metrics behind it all")
print("  99_reset_demo.py        -- tear the demo down / start it over")
print("=" * 70)
