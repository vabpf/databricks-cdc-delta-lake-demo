# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Explore the landing zone before loading it
# MAGIC
# MAGIC This notebook reads the raw CDC files **directly** from the volume. No
# MAGIC pipeline, no Auto Loader, no tables -- just the change stream as the
# MAGIC producer emitted it.
# MAGIC
# MAGIC Doing this first is not busywork. It is how you answer the three questions
# MAGIC that decide whether a CDC pipeline is going to behave:
# MAGIC
# MAGIC 1. **What shape is the envelope?** Auto Loader is going to infer a schema
# MAGIC    from these files; you want to know what it will infer before you build
# MAGIC    anything on top of it.
# MAGIC 2. **How dirty is the stream?** Null keys, unknown op codes and truncated
# MAGIC    JSON are all present on purpose. Knowing the counts now means the
# MAGIC    quarantine tables later are a *confirmation*, not a surprise.
# MAGIC 3. **Is the schema already drifting?** A new column appearing in wave 3
# MAGIC    is the whole reason the pipeline is written the way it is.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("landing_schema", "retail_landing", "Landing schema")
dbutils.widgets.text("volume_name", "cdc", "Landing volume name")

CATALOG = dbutils.widgets.get("catalog")
VOLUME = f"/Volumes/{CATALOG}/{dbutils.widgets.get('landing_schema')}/{dbutils.widgets.get('volume_name')}"

ENTITIES = [
    "customers", "products", "sellers",
    "orders", "order_items", "order_payments", "order_reviews",
]


def read_events(entity):
    """Read one entity's landing files the way Auto Loader will.

    `rescuedDataColumn` is the important part. Without it, a truncated JSON line
    is simply *dropped*, and the demo's deliberately broken records would vanish
    with no trace. With it, the unparseable content is kept in `_rescued_data`
    so it can be counted -- and, in the pipeline, quarantined.
    """
    return (
        spark.read.option("rescuedDataColumn", "_rescued_data")
        .json(f"{VOLUME}/events/{entity}")
    )


print(f"volume: {VOLUME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. The Debezium envelope
# MAGIC
# MAGIC Every record is one change to one row:
# MAGIC
# MAGIC | field | meaning |
# MAGIC |---|---|
# MAGIC | `op` | `c` create, `u` update, `d` delete, `r` snapshot read, `t` truncate, `x` unknown/garbage |
# MAGIC | `event_ts` | **business** time of the change -- not arrival time |
# MAGIC | `ts_ms` | the same instant as epoch milliseconds |
# MAGIC | `lsn` | monotonically increasing source log position |
# MAGIC | `source` | producer metadata, including a flag for snapshot reads |
# MAGIC | `before` / `after` | the row image before and after. `after` is null on delete; `before` is null on create |
# MAGIC
# MAGIC The `before`/`after` convention is what makes the stream self-describing: a
# MAGIC consumer never has to guess whether a missing field means "unchanged" or
# MAGIC "explicitly null", because the delete case is expressed as a whole null
# MAGIC image rather than as a null column.

# COMMAND ----------

display(read_events("orders").limit(3))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Volume and shape of the stream, per entity
# MAGIC
# MAGIC Note what is *missing* from `op` as well as what is present:
# MAGIC
# MAGIC * `customers`, `products`, `sellers` have `r` (snapshot read) and no `c` --
# MAGIC   they arrive as an initial snapshot.
# MAGIC * `orders`, `order_items`, `order_payments` have `c` and no `r` -- they
# MAGIC   are created in-flight.
# MAGIC * `order_reviews` has exactly one `t` -- the truncate marker.
# MAGIC * every entity has some `x` -- the unknown-op records planted for the
# MAGIC   dead-letter path.

# COMMAND ----------

from pyspark.sql import functions as F

summary = []
for entity in ENTITIES:
    df = read_events(entity)
    counts = {r["op"]: r["count"] for r in df.groupBy("op").count().collect()}
    summary.append(
        {
            "entity": entity,
            "records": sum(counts.values()),
            "rescued": df.filter(F.col("_rescued_data").isNotNull()).count(),
            **{f"op_{k}": v for k, v in counts.items()},
        }
    )

display(spark.createDataFrame(summary).orderBy("entity"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. The records that are supposed to be rejected
# MAGIC
# MAGIC Three flavours, all planted deterministically:
# MAGIC
# MAGIC * **null primary key** -- passes JSON parsing, fails the key expectation
# MAGIC * **unknown op code** (`x`) -- passes parsing, fails the op expectation
# MAGIC * **truncated JSON** -- fails parsing entirely and lands in `_rescued_data`
# MAGIC
# MAGIC The pipeline keeps all three, but in a table you can go and look at rather
# MAGIC than in a log nobody reads.

# COMMAND ----------

bad_key = (
    read_events("orders")
    .filter(F.col("after.order_id").isNull() & F.col("before.order_id").isNull())
    .select("op", "event_ts", F.col("after.order_id"), F.col("after.customer_id"))
)
print("orders records with no usable key:", bad_key.count())
display(bad_key.limit(5))

unknown_op = read_events("orders").filter(F.col("op") == "x")
print("orders records with an unknown op code:", unknown_op.count())
display(unknown_op.select("op", "event_ts", "after").limit(5))

rescued = (
    read_events("orders")
    .filter(F.col("_rescued_data").isNotNull())
    .select("_rescued_data")
)
print("rescued (unparseable) records:", rescued.count())
display(rescued.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Schema drift, seen directly in the files
# MAGIC
# MAGIC `products` gains two columns at wave 4: a brand new date column, and the
# MAGIC *corrected* spelling of a column name that Olist really does ship
# MAGIC misspelled.
# MAGIC
# MAGIC This is the detail worth internalising: **a rename is indistinguishable
# MAGIC from "old column goes null, new column appears"** when the source is
# MAGIC JSON. There is no rename metadata in a gzipped file of JSON objects. So
# MAGIC the silver table ends up carrying both columns, and the wave-1/2/3 rows
# MAGIC have nulls in the new one. That is not a bug to be fixed; it is the honest
# MAGIC result of a source that drifted, and it is exactly why the pipeline never
# MAGIC names those columns.

# COMMAND ----------

early_wave = spark.read.json(f"{VOLUME}/events/products/products_w01_p001.jsonl.gz")
w1 = early_wave.select("after.*")
w5 = spark.read.json(f"{VOLUME}/events/products/products_w05_p001.jsonl.gz").select("after.*")

cols_w1, cols_w5 = set(w1.columns), set(w5.columns)
print("columns present in wave 1:", sorted(cols_w1))
print()
print("arrived by wave 5       :", sorted(cols_w5 - cols_w1))
print("no longer populated     :", sorted(c for c in cols_w5 if c in cols_w1))

print("\nwave 1 spelling count :", [c for c in cols_w1 if "name_leng" in c])
print("wave 5 spelling count :", [c for c in cols_w5 if "name_leng" in c])

# COMMAND ----------

# MAGIC %md
# MAGIC And the same drift on `orders`, which is a plain additive change -- a
# MAGIC computed flag that simply did not exist before wave 3:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT get_json_object(_rescued_data, '$.after.delivery_sla_breach')
# MAGIC FROM ...
# MAGIC ```
# MAGIC
# MAGIC We do not need that: reading wave 3 and wave 5 directly shows it.

# COMMAND ----------

o1 = set(spark.read.json(f"{VOLUME}/events/orders/orders_w01_p001.jsonl.gz").select("after.*").columns)
o5 = set(spark.read.json(f"{VOLUME}/events/orders/orders_w05_p001.jsonl.gz").select("after.*").columns)
print("orders columns added by drift:", sorted(o5 - o1))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. The intentionally broken file
# MAGIC
# MAGIC One file in `orders` is not a change stream at all -- it is 122 lines of
# MAGIC JSON truncated mid-object, named `..._p999_broken.jsonl.gz` so it is
# MAGIC obvious what it is. Its purpose is to prove that ingestion survives
# MAGIC garbage and that the garbage is accounted for rather than silently lost.

# COMMAND ----------

broken_path = f"{VOLUME}/events/orders/orders_w03_p999_broken.jsonl.gz"
display(spark.read.text(broken_path).limit(5))
print("lines:", spark.read.text(broken_path).count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## What this tells us about the pipeline design
# MAGIC
# MAGIC Everything below follows from what is above:
# MAGIC
# MAGIC * because records can be unparseable, Auto Loader is configured with
# MAGIC   `rescuedDataColumn`, and there is a quarantine table per entity
# MAGIC * because keys can be null and op codes can be bogus, every conformed table
# MAGIC   carries an explicit expectation that drops (and accounts for) them
# MAGIC * because `after` is null on delete, key columns must be coalesced with
# MAGIC   `before` -- but *only* keys, since coalescing a business column would
# MAGIC   silently undo a legitimate null write
# MAGIC * because the source drifts, no downstream file names a column that the
# MAGIC   source has not sent yet
# MAGIC * because `event_ts` is real business time, AUTO CDC can be sequenced by it
# MAGIC   and late arrivals land in the right place in history
# MAGIC
# MAGIC Next: `10_medallion_walkthrough.py` walks the same data through bronze,
# MAGIC silver and gold.
