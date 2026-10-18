-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 02 - Explore the data after loading, wave by wave
-- MAGIC
-- MAGIC This notebook is pure SQL. It answers one question: **what actually
-- MAGIC arrived in each wave, and where can you see it?**
-- MAGIC
-- MAGIC It sits between `01_explore_landing_cdc` (which reads the raw files with
-- MAGIC no pipeline at all) and `10_medallion_walkthrough` (which walks the
-- MAGIC pipeline layer by layer). Here the data is already loaded, so the useful
-- MAGIC thing to do is compare deliveries.
-- MAGIC
-- MAGIC ## A wave is not one thing
-- MAGIC
-- MAGIC This is the part that trips people up, and it is worth being blunt about:
-- MAGIC
-- MAGIC * **In bronze, a wave is a property of the file.** `tools/upload_landing.py`
-- MAGIC   names every file `..._w02_p004.jsonl.gz`, so the wave is recoverable from
-- MAGIC   `_source_file` on any row. Wave attribution there is exact and needs no
-- MAGIC   timestamps.
-- MAGIC * **In silver and gold, a wave is a pipeline update** -- a set of Delta
-- MAGIC   commits made by one `databricks bundle run`. The wave number is *not* a
-- MAGIC   column anywhere upstream of gold. To slice those layers by wave you need
-- MAGIC   the update boundaries from `system.lakeflow.pipeline_update_timeline`, or
-- MAGIC   `_commit_timestamp` on the gold change feeds.
-- MAGIC
-- MAGIC So the same question -- "what did wave 2 do?" -- is answered three
-- MAGIC different ways below, and the three answers have to agree.
-- MAGIC
-- MAGIC ## What each section shows
-- MAGIC
-- MAGIC | section | lens |
-- MAGIC |---|---|
-- MAGIC | 1 | the pipeline updates, which is what defines a wave downstream |
-- MAGIC | 2 | bronze, per entity and wave, by operation |
-- MAGIC | 3 | bronze, the landed = conformed + quarantined accounting, per wave, and why |
-- MAGIC | 4 | wave A vs wave B, side by side -- the direct answer |
-- MAGIC | 5 | silver: what state exists now, SCD1 vs SCD2, and what a delete does to each |
-- MAGIC | 6 | gold change feeds: what the most recent wave changed |
-- MAGIC | 7 | gold marts: the business view |

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 0. Parameters
-- MAGIC
-- MAGIC All eight live in one cell as **session configuration**: `SET name=value`,
-- MAGIC read back textually as `${name}` below.
-- MAGIC
-- MAGIC This is deliberately not `CREATE WIDGET`. Two reasons. `${param}` as a
-- MAGIC *widget* syntax is deprecated since Databricks Runtime 15.2 (the current
-- MAGIC form is `:param`, which would force `IDENTIFIER(:catalog || '.' || ...)`
-- MAGIC on every table reference and unreadable SQL in exchange). And session
-- MAGIC substitution is not a guess here: it is the exact mechanism this repo's
-- MAGIC own pipeline depends on -- 161 `${retail_catalog}`-style placeholders in
-- MAGIC `src/pipeline/*.sql`, resolved from the `configuration` block of
-- MAGIC `resources/pipeline.yml`. Same engine, same substitution.
-- MAGIC
-- MAGIC `SET` takes a bare value: no spaces, no quotes. Change the last two lines
-- MAGIC to compare other deliveries; change the first six if you renamed the
-- MAGIC schemas.

-- COMMAND ----------

SET catalog=workspace;
SET bronze_schema=retail_bronze;
SET silver_schema=retail_silver;
SET gold_schema=retail_gold;
SET ops_schema=retail_ops;
SET pipeline_name=retail_medallion_cdc;

-- The two waves section 4 compares, as the two-character token the uploader
-- puts in every filename (_w01_, _w02_).
SET wave_a=01;
SET wave_b=02;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. The pipeline updates -- what defines a wave in silver and gold
-- MAGIC
-- MAGIC One row per update, newest first. Every `COMPLETED` `REFRESH` here is one
-- MAGIC wave, and its `period_start_time` is the boundary you would use to slice
-- MAGIC Delta versions or change-feed rows by wave.
-- MAGIC
-- MAGIC Two rows are worth looking for specifically:
-- MAGIC
-- MAGIC * a `RETRY_ON_FAILURE` row in `CANCELED` state -- evidence of the platform's
-- MAGIC   automatic retry, which the pipeline pins to zero attempts so that a bug
-- MAGIC   shows up once instead of five times
-- MAGIC * a `VALIDATE` row in `FAILED` state -- a `--validate-only` dry run. It
-- MAGIC   analyses the whole graph without materialising anything, which is why it
-- MAGIC   fails in seconds and costs nothing

-- COMMAND ----------

SELECT
  update_id,
  update_type,
  trigger_type,
  result_state,
  period_start_time,
  period_end_time,
  timestampdiff(SECOND, period_start_time, period_end_time) AS seconds
FROM system.lakeflow.pipeline_update_timeline
WHERE pipeline_id IN (
  SELECT pipeline_id
  FROM system.lakeflow.pipelines
  WHERE name = '${pipeline_name}'
)
ORDER BY period_end_time DESC
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Bronze, per entity and wave: what each delivery contained
-- MAGIC
-- MAGIC Read the shape, not just the totals, and start by dropping an assumption
-- MAGIC this data does not support: **wave 1 is not "the snapshot" and the later
-- MAGIC waves are not "only changes".**
-- MAGIC
-- MAGIC * The three dimensions arrive as a **paginated snapshot**. `r` means
-- MAGIC   "snapshot read", and `r` records keep arriving in waves 2, 3 and beyond --
-- MAGIC   so wave 2 is still mostly brand-new keys for `customers` (1,081 of 1,253
-- MAGIC   rows). Those are the tail of a read that did not fit in one delivery, not
-- MAGIC   updates.
-- MAGIC * `orders` is the one entity that is **update-dominated**: 10,979 `u`
-- MAGIC   against 4,104 `c` in wave 1, and 11,866 `u` in wave 2. That is the domain
-- MAGIC   talking -- an order changes status repeatedly over its life -- and it is
-- MAGIC   why this is the entity that justifies SCD1 with `SEQUENCE BY` rather than
-- MAGIC   a `MERGE` in arrival order.
-- MAGIC * The other transactional entities (`order_items`, `order_payments`,
-- MAGIC   `order_reviews`) are insert-dominated in both waves, with deletes present
-- MAGIC   in the first two.
-- MAGIC
-- MAGIC The columns do not sum to `events` for `order_reviews`, and that is
-- MAGIC correct: it is the one entity carrying a `t` (TRUNCATE) record, which is
-- MAGIC neither new, updated nor deleted. `01_explore_landing_cdc` catches it in
-- MAGIC the raw files, before any of this exists.
-- MAGIC
-- MAGIC Two things make these totals checkable:
-- MAGIC
-- MAGIC * These are `_cdc` counts, so they exclude records that failed the
-- MAGIC   contract. The **landed** counts in section 3 are the exact ones: they
-- MAGIC   match `data/landing/_manifest.json`'s `by_wave` accounting for all seven
-- MAGIC   entities, and landed minus conformed is precisely the quarantine table.
-- MAGIC * `r` and `c` share the `new_rows` column because to a consumer they mean
-- MAGIC   the same thing: this key is new to us.

-- COMMAND ----------

WITH events AS (
  SELECT 'customers' AS entity,
         regexp_extract(_source_file, '_w([0-9]{2})_', 1) AS wave, _op
  FROM ${catalog}.${bronze_schema}.customers_cdc
  UNION ALL
  SELECT 'products',
         regexp_extract(_source_file, '_w([0-9]{2})_', 1), _op
  FROM ${catalog}.${bronze_schema}.products_cdc
  UNION ALL
  SELECT 'sellers',
         regexp_extract(_source_file, '_w([0-9]{2})_', 1), _op
  FROM ${catalog}.${bronze_schema}.sellers_cdc
  UNION ALL
  SELECT 'orders',
         regexp_extract(_source_file, '_w([0-9]{2})_', 1), _op
  FROM ${catalog}.${bronze_schema}.orders_cdc
  UNION ALL
  SELECT 'order_items',
         regexp_extract(_source_file, '_w([0-9]{2})_', 1), _op
  FROM ${catalog}.${bronze_schema}.order_items_cdc
  UNION ALL
  SELECT 'order_payments',
         regexp_extract(_source_file, '_w([0-9]{2})_', 1), _op
  FROM ${catalog}.${bronze_schema}.order_payments_cdc
  UNION ALL
  SELECT 'order_reviews',
         regexp_extract(_source_file, '_w([0-9]{2})_', 1), _op
  FROM ${catalog}.${bronze_schema}.order_reviews_cdc
)
SELECT
  entity,
  wave,
  count(*) AS events,
  count_if(_op IN ('r', 'c')) AS new_rows,
  count_if(_op = 'u') AS updates,
  count_if(_op = 'd') AS deletes
FROM events
GROUP BY entity, wave
ORDER BY entity, wave;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Bronze: the accounting has to balance, per wave
-- MAGIC
-- MAGIC Every landed record ends up in exactly one of two tables: conformed
-- MAGIC (bridging the contract) or quarantined (with a reason). The quarantine
-- MAGIC predicate is the *exact complement* of the conform predicate, so this is
-- MAGIC an identity and `unexplained` must be 0 in every wave.
-- MAGIC
-- MAGIC This is what makes a quarantine table trustworthy. A "dropped rows" log is
-- MAGIC unauditable -- you cannot tell a rejection from a bug. An identity you can
-- MAGIC check is not.
-- MAGIC
-- MAGIC `orders` is used as the example because it carries the most garbage; swap
-- MAGIC the table name to look at another entity.

-- COMMAND ----------

WITH landed AS (
  SELECT regexp_extract(_source_file, '_w([0-9]{2})_', 1) AS wave, count(*) AS rows
  FROM ${catalog}.${bronze_schema}.orders_raw
  GROUP BY 1
),
conformed AS (
  SELECT regexp_extract(_source_file, '_w([0-9]{2})_', 1) AS wave, count(*) AS rows
  FROM ${catalog}.${bronze_schema}.orders_cdc
  GROUP BY 1
),
rejected AS (
  SELECT regexp_extract(_source_file, '_w([0-9]{2})_', 1) AS wave, count(*) AS rows
  FROM ${catalog}.${ops_schema}.orders_quarantine
  GROUP BY 1
)
SELECT
  coalesce(landed.wave, conformed.wave, rejected.wave) AS wave,
  landed.rows AS landed,
  conformed.rows AS conformed,
  rejected.rows AS quarantined,
  landed.rows - coalesce(conformed.rows, 0) - coalesce(rejected.rows, 0) AS unexplained
FROM landed
FULL OUTER JOIN conformed ON landed.wave = conformed.wave
FULL OUTER JOIN rejected ON coalesce(landed.wave, conformed.wave) = rejected.wave
ORDER BY wave;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The rejects are worth reading once, because each row is the contract being
-- MAGIC *enforced* rather than a count of failures. `null_key` is the interesting
-- MAGIC one: it is why the conform predicate has to coalesce keys out of `before`.
-- MAGIC On a delete the `after` image is empty and the key only exists in `before`,
-- MAGIC so a pipeline that reads keys from `after` alone quarantines every delete
-- MAGIC it is sent -- and fails in the direction of silently keeping rows alive.

-- COMMAND ----------

SELECT _source_entity AS entity, _quarantine_reason, count(*) AS rows
FROM (
  SELECT _source_entity, _quarantine_reason FROM ${catalog}.${ops_schema}.customers_quarantine
  UNION ALL SELECT _source_entity, _quarantine_reason FROM ${catalog}.${ops_schema}.products_quarantine
  UNION ALL SELECT _source_entity, _quarantine_reason FROM ${catalog}.${ops_schema}.sellers_quarantine
  UNION ALL SELECT _source_entity, _quarantine_reason FROM ${catalog}.${ops_schema}.orders_quarantine
  UNION ALL SELECT _source_entity, _quarantine_reason FROM ${catalog}.${ops_schema}.order_items_quarantine
  UNION ALL SELECT _source_entity, _quarantine_reason FROM ${catalog}.${ops_schema}.order_payments_quarantine
  UNION ALL SELECT _source_entity, _quarantine_reason FROM ${catalog}.${ops_schema}.order_reviews_quarantine
)
GROUP BY entity, _quarantine_reason
ORDER BY entity, rows DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Wave A vs wave B, side by side
-- MAGIC
-- MAGIC This is the direct comparison, and it is exact: both columns come from the
-- MAGIC same bronze tables, sliced by the two-character wave token in
-- MAGIC `_source_file`. Change the `wave_a` / `wave_b` values in section 0 to
-- MAGIC compare any two deliveries, with no timestamp reasoning at all.
-- MAGIC
-- MAGIC Read it entity by entity, because there is no single answer:
-- MAGIC
-- MAGIC * `customers`, `products`, `sellers` -- wave A dwarfs wave B (23,809 to
-- MAGIC   1,253 for `customers`), and wave B is still mostly `new_rows`. The
-- MAGIC   snapshot is being paginated, so a later wave keeps introducing keys you
-- MAGIC   have never seen. A pipeline that assumed "wave 1 saw every key" would be
-- MAGIC   wrong by wave 3.
-- MAGIC * `orders` -- the two waves are the same size, and `updates` outweighs
-- MAGIC   `new_rows` roughly 3:1 in both. Without `SEQUENCE BY`, wave 2's 11,866
-- MAGIC   updates would be applied in whatever order they arrived and a stale
-- MAGIC   status could win over a newer one.
-- MAGIC * `order_items`, `order_payments`, `order_reviews` -- new rows in both
-- MAGIC   waves, with deletes in the first two only.
-- MAGIC
-- MAGIC A NULL in a wave column means that entity had nothing to deliver in that
-- MAGIC wave at all -- `max(CASE ...)` over no rows -- which is a different claim
-- MAGIC from zero, and worth keeping distinct.

-- COMMAND ----------

WITH events AS (
  SELECT 'customers' AS entity,
         regexp_extract(_source_file, '_w([0-9]{2})_', 1) AS wave, _op
  FROM ${catalog}.${bronze_schema}.customers_cdc
  UNION ALL
  SELECT 'products',
         regexp_extract(_source_file, '_w([0-9]{2})_', 1), _op
  FROM ${catalog}.${bronze_schema}.products_cdc
  UNION ALL
  SELECT 'sellers',
         regexp_extract(_source_file, '_w([0-9]{2})_', 1), _op
  FROM ${catalog}.${bronze_schema}.sellers_cdc
  UNION ALL
  SELECT 'orders',
         regexp_extract(_source_file, '_w([0-9]{2})_', 1), _op
  FROM ${catalog}.${bronze_schema}.orders_cdc
  UNION ALL
  SELECT 'order_items',
         regexp_extract(_source_file, '_w([0-9]{2})_', 1), _op
  FROM ${catalog}.${bronze_schema}.order_items_cdc
  UNION ALL
  SELECT 'order_payments',
         regexp_extract(_source_file, '_w([0-9]{2})_', 1), _op
  FROM ${catalog}.${bronze_schema}.order_payments_cdc
  UNION ALL
  SELECT 'order_reviews',
         regexp_extract(_source_file, '_w([0-9]{2})_', 1), _op
  FROM ${catalog}.${bronze_schema}.order_reviews_cdc
),
per_wave AS (
  SELECT
    entity,
    wave,
    count(*) AS events,
    count_if(_op IN ('r', 'c')) AS new_rows,
    count_if(_op = 'u') AS updates,
    count_if(_op = 'd') AS deletes
  FROM events
  GROUP BY entity, wave
)
SELECT
  entity,
  max(CASE WHEN wave = '${wave_a}' THEN events  END) AS wave_a_events,
  max(CASE WHEN wave = '${wave_a}' THEN new_rows END) AS wave_a_new_rows,
  max(CASE WHEN wave = '${wave_a}' THEN updates END) AS wave_a_updates,
  max(CASE WHEN wave = '${wave_a}' THEN deletes END) AS wave_a_deletes,
  max(CASE WHEN wave = '${wave_b}' THEN events  END) AS wave_b_events,
  max(CASE WHEN wave = '${wave_b}' THEN new_rows END) AS wave_b_new_rows,
  max(CASE WHEN wave = '${wave_b}' THEN updates END) AS wave_b_updates,
  max(CASE WHEN wave = '${wave_b}' THEN deletes END) AS wave_b_deletes
FROM per_wave
GROUP BY entity
ORDER BY entity;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. Silver: what state exists now
-- MAGIC
-- MAGIC Silver is where the change stream stops being a stream: it is **current
-- MAGIC state**, and the two tables of the same entity below are shaped
-- MAGIC differently on purpose.
-- MAGIC
-- MAGIC * **SCD2** (`customers`, `products`, `sellers`, `orders_status_history`)
-- MAGIC   keeps history, so `rows_now` exceeds `distinct_keys` and the identity to
-- MAGIC   check is `rows_now = current_versions + closed_versions`.
-- MAGIC   `current_versions` counts rows whose `__END_AT` is null.
-- MAGIC * **SCD1** (`orders`, `order_items`, `order_payments`, `order_reviews`)
-- MAGIC   keeps one row per key, so the rows and keys columns agree and there is no
-- MAGIC   history to show. That is the deliberate trade-off; `20_time_travel_and_cdf`
-- MAGIC   picks it up again.
-- MAGIC
-- MAGIC One wrinkle here is the whole point of keeping both patterns side by side.
-- MAGIC In `orders_status_history` the key count (8,666) is *higher* than
-- MAGIC `current_versions` (8,646): 20 orders have every version closed. Those are
-- MAGIC deleted orders, and SCD1 `orders` holds 8,646 rows with no memory of them
-- MAGIC whatsoever. One table answers "what is true", the other answers "what
-- MAGIC happened", and they are allowed to disagree -- which is why the next cell
-- MAGIC exists.

-- COMMAND ----------

SELECT
  'customers' AS entity, 'SCD2' AS tracked_as,
  count(*) AS rows_now,
  count(DISTINCT customer_id) AS distinct_keys,
  count_if(__END_AT IS NULL) AS current_versions,
  count_if(__END_AT IS NOT NULL) AS closed_versions
FROM ${catalog}.${silver_schema}.customers
UNION ALL
SELECT 'products', 'SCD2', count(*), count(DISTINCT product_id),
  count_if(__END_AT IS NULL), count_if(__END_AT IS NOT NULL)
FROM ${catalog}.${silver_schema}.products
UNION ALL
SELECT 'sellers', 'SCD2', count(*), count(DISTINCT seller_id),
  count_if(__END_AT IS NULL), count_if(__END_AT IS NOT NULL)
FROM ${catalog}.${silver_schema}.sellers
UNION ALL
SELECT 'orders_status_history', 'SCD2', count(*), count(DISTINCT order_id),
  count_if(__END_AT IS NULL), count_if(__END_AT IS NOT NULL)
FROM ${catalog}.${silver_schema}.orders_status_history
UNION ALL
SELECT 'orders', 'SCD1', count(*), count(DISTINCT order_id), NULL, NULL
FROM ${catalog}.${silver_schema}.orders
UNION ALL
SELECT 'order_items', 'SCD1', count(*), count(DISTINCT order_id, order_item_id), NULL, NULL
FROM ${catalog}.${silver_schema}.order_items
UNION ALL
SELECT 'order_payments', 'SCD1', count(*), count(DISTINCT order_id, payment_sequential), NULL, NULL
FROM ${catalog}.${silver_schema}.order_payments
UNION ALL
SELECT 'order_reviews', 'SCD1', count(*), count(DISTINCT review_id), NULL, NULL
FROM ${catalog}.${silver_schema}.order_reviews
ORDER BY entity;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### The deleted orders, and the one that came back
-- MAGIC
-- MAGIC Bronze carries 21 `d` records for `orders` across the two waves. Silver
-- MAGIC should agree, in the only way that matters: those keys must be gone from
-- MAGIC SCD1 and fully closed in SCD2. Four numbers, and if any of them move the
-- MAGIC pipeline has a delete problem.

-- COMMAND ----------

WITH deleted AS (
  SELECT DISTINCT order_id
  FROM ${catalog}.${bronze_schema}.orders_cdc
  WHERE _op = 'd'
),
history AS (
  SELECT order_id, count_if(__END_AT IS NULL) AS open_versions
  FROM ${catalog}.${silver_schema}.orders_status_history
  GROUP BY order_id
),
scd1 AS (
  SELECT order_id FROM ${catalog}.${silver_schema}.orders
)
SELECT
  count(*) AS deleted_keys,
  count_if(history.open_versions = 0) AS now_fully_closed,
  count_if(history.open_versions > 0) AS still_alive,
  count_if(scd1.order_id IS NOT NULL) AS present_in_scd1
FROM deleted
LEFT JOIN history ON deleted.order_id = history.order_id
LEFT JOIN scd1 ON deleted.order_id = scd1.order_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC `still_alive` is 1, and that row is the most instructive event in the demo.
-- MAGIC Its bronze events, in the order the pipeline should consider them:
-- MAGIC
-- MAGIC create, approve, cancel, **delete**, then -- five days later in *business*
-- MAGIC time -- another cancel. The delete is not a tombstone; it is one more
-- MAGIC event, and `SEQUENCE BY _event_ts` means the latest event wins regardless
-- MAGIC of what it was. A delete that the producer stamps first can therefore be
-- MAGIC overturned by a change stamped afterwards, which is exactly the behaviour
-- MAGIC you want from a stream carrying late and out-of-order records.
-- MAGIC
-- MAGIC Arriving order and business order are different clocks. This is where they
-- MAGIC come apart.

-- COMMAND ----------

WITH deleted AS (
  SELECT DISTINCT order_id
  FROM ${catalog}.${bronze_schema}.orders_cdc
  WHERE _op = 'd'
),
history AS (
  SELECT order_id, count_if(__END_AT IS NULL) AS open_versions
  FROM ${catalog}.${silver_schema}.orders_status_history
  GROUP BY order_id
),
alive AS (
  SELECT deleted.order_id
  FROM deleted
  JOIN history ON deleted.order_id = history.order_id
  WHERE history.open_versions > 0
)
SELECT
  events.order_id,
  events._op,
  events._event_ts,
  events._lsn,
  events.order_status,
  regexp_extract(events._source_file, '_w([0-9]{2})_', 1) AS wave
FROM ${catalog}.${bronze_schema}.orders_cdc AS events
JOIN alive ON events.order_id = alive.order_id
ORDER BY events._event_ts, events._lsn;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 6. Gold: what the most recent wave changed
-- MAGIC
-- MAGIC The `*_changes` tables are the Delta change feed of each silver table,
-- MAGIC materialised as append-only tables. Every row carries `_change_type`,
-- MAGIC `_commit_version` and `_commit_timestamp`, so a wave is a range of commit
-- MAGIC timestamps -- and the boundary comes from the update timeline in section 1
-- MAGIC rather than from anything you have to write down.
-- MAGIC
-- MAGIC `_commit_timestamp` here is the **silver** table's commit time, not the
-- MAGIC business time of the change: these rows describe a 2017/2018 order that was
-- MAGIC committed to Delta when the pipeline ran today. Keeping those two clocks
-- MAGIC apart is most of what makes table history readable.
-- MAGIC
-- MAGIC Two cautions before you compare this against section 4:
-- MAGIC
-- MAGIC * **These are not counts of source events.** `_change_type` records what
-- MAGIC   AUTO CDC *did*, and it decides that from the primary key plus the sequence
-- MAGIC   value -- never from the producer's `op`. A `c` record for an order that
-- MAGIC   already exists is an upsert, so it surfaces as an `update_preimage` /
-- MAGIC   `update_postimage` pair even though bronze labelled it a create. Section 4
-- MAGIC   and this one disagree on purpose, and neither is wrong: bronze is what the
-- MAGIC   producer said, gold is what the pipeline did.
-- MAGIC * An update is **two** feed rows, an insert is one, so raw totals across
-- MAGIC   change types are not comparable. Compare the shape instead: which
-- MAGIC   entities moved, and how.
-- MAGIC
-- MAGIC The keys do agree, which is the check worth running: for `customers`, the
-- MAGIC distinct keys touched by the latest wave are exactly the distinct keys wave
-- MAGIC B delivered. If that ever stops holding, a change is being attributed to the
-- MAGIC wrong update.

-- COMMAND ----------

WITH boundary AS (
  SELECT max(period_start_time) AS latest_start
  FROM system.lakeflow.pipeline_update_timeline
  WHERE pipeline_id IN (
    SELECT pipeline_id FROM system.lakeflow.pipelines WHERE name = '${pipeline_name}'
  )
  AND result_state = 'COMPLETED'
),
feeds AS (
  SELECT 'customers' AS entity, _change_type, _commit_timestamp
  FROM ${catalog}.${gold_schema}.customers_changes
  UNION ALL
  SELECT 'products', _change_type, _commit_timestamp
  FROM ${catalog}.${gold_schema}.products_changes
  UNION ALL
  SELECT 'sellers', _change_type, _commit_timestamp
  FROM ${catalog}.${gold_schema}.sellers_changes
  UNION ALL
  SELECT 'orders', _change_type, _commit_timestamp
  FROM ${catalog}.${gold_schema}.orders_changes
  UNION ALL
  SELECT 'order_items', _change_type, _commit_timestamp
  FROM ${catalog}.${gold_schema}.order_items_changes
  UNION ALL
  SELECT 'order_payments', _change_type, _commit_timestamp
  FROM ${catalog}.${gold_schema}.order_payments_changes
  UNION ALL
  SELECT 'order_reviews', _change_type, _commit_timestamp
  FROM ${catalog}.${gold_schema}.order_reviews_changes
)
SELECT
  entity,
  _change_type,
  count_if(_commit_timestamp <  (SELECT latest_start FROM boundary)) AS earlier_waves,
  count_if(_commit_timestamp >= (SELECT latest_start FROM boundary)) AS latest_wave
FROM feeds
GROUP BY entity, _change_type
ORDER BY entity, _change_type;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The same data, unaggregated: one block of rows per commit, which is how you
-- MAGIC see the wave boundary with your own eyes. `update_preimage` and
-- MAGIC `update_postimage` always come in pairs -- that is an update expressed as
-- MAGIC "what it was" plus "what it became", which is what makes the feed
-- MAGIC replayable.

-- COMMAND ----------

SELECT
  date_trunc('second', _commit_timestamp) AS commit_time,
  _change_type,
  count(*) AS rows
FROM ${catalog}.${gold_schema}.orders_changes
GROUP BY ALL
ORDER BY commit_time, _change_type;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC And one concrete change, to make it less abstract. For the SCD2 dimensions
-- MAGIC the feed also carries `__START_AT` / `__END_AT`, so a single update reads as
-- MAGIC "this version closed, this one opened".

-- COMMAND ----------

SELECT
  _change_type,
  _commit_version,
  customer_id,
  customer_city,
  customer_state,
  __START_AT,
  __END_AT
FROM ${catalog}.${gold_schema}.customers_changes
ORDER BY _commit_version DESC, __START_AT DESC
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 7. Gold marts: the business view
-- MAGIC
-- MAGIC The star schema and the marts are materialized views over current silver
-- MAGIC state, so they have no per-wave history of their own -- they are recomputed
-- MAGIC from whatever silver holds. That is the right shape for a mart, and it is
-- MAGIC why the change feeds above exist separately: when you need "what changed",
-- MAGIC a mart cannot answer, and when you need "what is true now", a change feed
-- MAGIC makes you do the arithmetic.

-- COMMAND ----------

SELECT
  product_category,
  items_sold,
  orders,
  sellers,
  round(revenue, 2) AS revenue,
  round(avg_item_price, 2) AS avg_item_price
FROM ${catalog}.${gold_schema}.mart_category_revenue
ORDER BY revenue DESC
LIMIT 15;

-- COMMAND ----------

SELECT
  purchase_month,
  customer_state,
  orders,
  delivered_orders,
  late_orders,
  round(avg_days_to_deliver, 1) AS avg_days_to_deliver,
  round(late_rate_pct, 2) AS late_rate_pct
FROM ${catalog}.${gold_schema}.mart_delivery_sla
WHERE delivered_orders >= 50
ORDER BY late_rate_pct DESC
LIMIT 15;

-- COMMAND ----------

SELECT
  customer_unique_id,
  customer_state,
  lifetime_orders,
  delivered_orders,
  late_orders,
  round(lifetime_value, 2) AS lifetime_value,
  round(avg_order_value, 2) AS avg_order_value
FROM ${catalog}.${gold_schema}.customer_360
ORDER BY lifetime_value DESC
LIMIT 15;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Where to go next
-- MAGIC
-- MAGIC * **Upload the next wave and re-run this notebook.** Section 1 gains an
-- MAGIC   update, section 2 gains a wave row, and section 4 compares any two waves
-- MAGIC   if you change the two `SET` lines. Two numbers are real invariants:
-- MAGIC   `unexplained` at 0 in every row of section 3, and `rows_now =
-- MAGIC   current_versions + closed_versions` in section 5. If either breaks, the
-- MAGIC   pipeline is wrong and the query is not.
-- MAGIC * **`10_medallion_walkthrough`** takes the same data and asserts the pipeline
-- MAGIC   produced the right answer, layer by layer.
-- MAGIC * **`20_time_travel_and_cdf`** goes deeper on the two mechanisms used in
-- MAGIC   section 6 -- Delta versions and `table_changes()` -- and walks a GDPR
-- MAGIC   erasure through them.
-- MAGIC * **`30_reprocess_and_late_data`** manufactures the cases that make section 4
-- MAGIC   interesting: late arrivals that must slot into the middle of history.
