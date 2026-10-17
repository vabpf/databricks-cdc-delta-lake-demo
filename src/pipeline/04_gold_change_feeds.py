# ---------------------------------------------------------------------------
# GOLD — change feeds (Delta CDF over the silver AUTO CDC targets)
# ---------------------------------------------------------------------------
# The silver layer already holds *current state* (SCD1) and *history* (SCD2).
# This layer answers a different question: "what changed, when, and in which
# commit?" That is the Delta change data feed, and it is what an audit trail, a
# reverse-ETL sync, or a downstream system subscribes to.
#
# Why this is a streaming table and not a view
# --------------------------------------------
# A change feed is append-only from the reader's point of view: each pipeline
# update sees only the changes committed since the last update. A streaming
# table is exactly that shape, so the feed is materialised incrementally and
# the history is retained even after the silver table itself moves on.
#
# What the CDF columns mean
# -------------------------
# Every row is a silver row plus three columns supplied by Delta, not by us:
#   _change_type       insert | update_preimage | update_postimage | delete
#   _commit_version    the silver table version this change was part of
#   _commit_timestamp  when that version was committed
#
# An update is two rows -- the value before and the value after. Keep them
# both: that is what makes the feed replayable. We add three booleans so
# consumers do not have to pattern-match on strings.
#
# The SCD2 subtlety worth knowing
# -------------------------------
# For an SCD2 target the primary key is the business key PLUS
# coalesce(__START_AT, __END_AT). When a version is *slotted into the past* --
# an out-of-order or late-arriving change landing before an already-written
# version -- the start time of the following row moves, which changes that
# row's identity. In the feed those show up as insert/delete pairs rather than
# update_preimage/update_postimage. That is not a bug: it is the visible
# consequence of correcting history, and it is worth pointing out in the demo
# because it is the thing that surprises people. See docs/architecture.md.
#
# Why these read *streaming tables* and not materialized views
# -----------------------------------------------------------
# Reading the change data feed of a materialized view from within the same
# pipeline that defines it is not supported (it is Beta, needs the PREVIEW
# channel and Databricks Runtime 18 LTS, and must be read from a different
# pipeline). Every source here is a streaming table, where a CDF read inside
# the same pipeline is supported on Unity Catalog from Databricks Runtime 15.2.
# ---------------------------------------------------------------------------
from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("retail_catalog")
SILVER = spark.conf.get("retail_silver")


def _change_feed(entity: str):
    """The Delta change feed of a silver entity, as an append-only stream.

    Note the two things that make this work and are easy to get wrong:

      1. readChangeFeed=true is what turns a normal Delta read into a change
         feed. The source must have delta.enableChangeDataFeed set (done in
         the silver layer) and must be published to Unity Catalog.
      2. SEQUENCE BY upstream is business time, so a late arrival is recorded
         against its real historical version here -- the feed is ordered by
         source truth, not by when we happened to receive it.
    """
    return (
        spark.readStream.option("readChangeFeed", "true")
        .table(f"{CATALOG}.{SILVER}.{entity}")
        .withColumn("_cdc_is_insert", F.col("_change_type") == F.lit("insert"))
        .withColumn("_cdc_is_update", F.col("_change_type").startswith("update"))
        .withColumn("_cdc_is_delete", F.col("_change_type") == F.lit("delete"))
    )


def _feed_props(entity: str) -> dict:
    return {
        "quality": "gold",
        "layer": "gold",
        "entity": entity,
        "feed": "change_data_feed",
        "source_table": f"{CATALOG}.{SILVER}.{entity}",
    }


@dp.table(
    name="customers_changes",
    comment="Change feed for the customer SCD2 dimension: every version opened and closed.",
    table_properties=_feed_props("customers"),
)
def customers_changes():
    return _change_feed("customers")


@dp.table(
    name="products_changes",
    comment="Change feed for the product SCD2 dimension, including the drifted wave-4 columns.",
    table_properties=_feed_props("products"),
)
def products_changes():
    return _change_feed("products")


@dp.table(
    name="sellers_changes",
    comment="Change feed for the seller SCD2 dimension (history tracked on city/state only).",
    table_properties=_feed_props("sellers"),
)
def sellers_changes():
    return _change_feed("sellers")


@dp.table(
    name="orders_changes",
    comment="Change feed for the current order state: the full lifecycle as one auditable stream.",
    table_properties=_feed_props("orders"),
)
def orders_changes():
    return _change_feed("orders")


@dp.table(
    name="order_items_changes",
    comment="Change feed for order line items, including price corrections and removals.",
    table_properties=_feed_props("order_items"),
)
def order_items_changes():
    return _change_feed("order_items")


@dp.table(
    name="order_payments_changes",
    comment="Change feed for order payments, including corrections and reversals.",
    table_properties=_feed_props("order_payments"),
)
def order_payments_changes():
    return _change_feed("order_payments")


@dp.table(
    name="order_reviews_changes",
    comment="Change feed for reviews, including the partial edits applied via IGNORE NULL UPDATES.",
    table_properties=_feed_props("order_reviews"),
)
def order_reviews_changes():
    return _change_feed("order_reviews")
