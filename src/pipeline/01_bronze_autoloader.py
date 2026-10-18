# ---------------------------------------------------------------------------
# BRONZE — Auto Loader ingestion of the raw CDC envelope
# ---------------------------------------------------------------------------
# One streaming table per source entity. Nothing is parsed or filtered here:
# the raw Debezium envelope (op / before / after / source / lsn) lands intact,
# plus ingestion lineage.
#
# Key Auto Loader choices, and why:
#   cloudFiles.format=json            newline-delimited Debezium events
#   cloudFiles.schemaEvolutionMode    addNewColumns -> a new field in the
#                                     source never breaks ingestion
#   cloudFiles.rescuedDataColumn      anything that does NOT fit the schema is
#                                     kept in _rescued_data instead of being
#                                     silently dropped (our broken-JSON demo)
#   cloudFiles.inferColumnTypes       real types (bigint/double/boolean), not
#                                     everything as string
#
# Note: Auto Loader tracks processed files transactionally, so re-running the
# pipeline never double-reads a wave. New files land -> next run picks them up.
# ---------------------------------------------------------------------------
from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("retail_catalog")
BRONZE = spark.conf.get("retail_bronze")
LANDING = spark.conf.get("retail_landing")

# The volume path for each entity's event files.
EVENTS_PATH = f"{LANDING}/events"

ENTITY_COMMENTS = {
    "customers": "Raw CDC events for the customer master data",
    "products": "Raw CDC events for the product catalogue",
    "sellers": "Raw CDC events for seller master data",
    "orders": "Raw CDC events for the order lifecycle",
    "order_items": "Raw CDC events for order line items",
    "order_payments": "Raw CDC events for order payments",
    "order_reviews": "Raw CDC events for customer reviews",
}


# `before` and `after` are structs, and 02_bronze_conform.sql reads keys out of
# them (`after`.*, `before.<key>`). That shape is *not* guaranteed by anything
# here, because Auto Loader types a column from the sample it reads first: a
# field that is null in every sampled record is inferred as STRING. `before` is
# null on create and on snapshot-read, so an entity whose opening files carry
# only creates has the whole `before` struct flattened to a scalar, and
# `before.<key>` then fails at analysis -- taking every table in the pipeline
# down with it, since a pipeline analyses the whole graph before executing.
#
# The guarantee therefore comes from the source: the generator never omits a
# before image on an update, and tools/verify_stream.py asserts that.
#
# Do NOT try to enforce it with cloudFiles.schemaHints. A dotted hint can only
# *add* a nested field, so against an already-STRING `before` it fails with
# "Cannot add <key> because its parent is not a StructType" -- a second, more
# confusing error in place of the first, never a promotion. And because an
# inferred schema is sticky (it is replayed from the schema location on every
# restart), the only way to re-infer is a full refresh of the table. See item 8
# in docs/architecture.md.
def _autoloader(entity: str):
    """A streaming DataFrame over one entity's landing directory."""
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.rescuedDataColumn", "_rescued_data")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.includeExistingFiles", "true")
        .load(f"{EVENTS_PATH}/{entity}/")
    )


def _with_lineage(df):
    """Attach ingestion lineage. This is the audit trail: every silver row can
    be traced back to the exact file and micro-batch that delivered it."""
    return df.select(
        "*",
        F.col("_metadata.file_path").alias("_source_file"),
        F.col("_metadata.file_modification_time").alias("_source_file_ts"),
        F.current_timestamp().alias("_ingested_at"),
    )


def _raw(entity: str):
    return _with_lineage(_autoloader(entity))


@dp.table(
    name="customers_raw",
    comment=ENTITY_COMMENTS["customers"],
    table_properties={"quality": "bronze", "layer": "bronze", "entity": "customers"},
)
def customers_raw():
    return _raw("customers")


@dp.table(
    name="products_raw",
    comment=ENTITY_COMMENTS["products"],
    table_properties={"quality": "bronze", "layer": "bronze", "entity": "products"},
)
def products_raw():
    return _raw("products")


@dp.table(
    name="sellers_raw",
    comment=ENTITY_COMMENTS["sellers"],
    table_properties={"quality": "bronze", "layer": "bronze", "entity": "sellers"},
)
def sellers_raw():
    return _raw("sellers")


@dp.table(
    name="orders_raw",
    comment=ENTITY_COMMENTS["orders"],
    table_properties={"quality": "bronze", "layer": "bronze", "entity": "orders"},
)
def orders_raw():
    return _raw("orders")


@dp.table(
    name="order_items_raw",
    comment=ENTITY_COMMENTS["order_items"],
    table_properties={"quality": "bronze", "layer": "bronze", "entity": "order_items"},
)
def order_items_raw():
    return _raw("order_items")


@dp.table(
    name="order_payments_raw",
    comment=ENTITY_COMMENTS["order_payments"],
    table_properties={"quality": "bronze", "layer": "bronze", "entity": "order_payments"},
)
def order_payments_raw():
    return _raw("order_payments")


@dp.table(
    name="order_reviews_raw",
    comment=ENTITY_COMMENTS["order_reviews"],
    table_properties={"quality": "bronze", "layer": "bronze", "entity": "order_reviews"},
)
def order_reviews_raw():
    return _raw("order_reviews")
