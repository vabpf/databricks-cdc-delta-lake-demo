-- ---------------------------------------------------------------------------
-- BRONZE — conform + quarantine
-- ---------------------------------------------------------------------------
-- Two jobs in this file, for every entity:
--
--   1. <entity>_cdc          unpack the Debezium envelope into a typed,
--                            flat change stream. This is the contract that
--                            every downstream layer (silver AUTO CDC, gold
--                            CDF, quarantine) is written against.
--
--   2. <entity>_quarantine   the EXACT COMPLEMENT of the conform predicate,
--                            published to the ops schema. Nothing is ever
--                            silently discarded: a row either lands in _cdc or
--                            in _quarantine, never both and never neither.
--
-- Why two datasets instead of one bad-rows table
-- ----------------------------------------------
-- `ON VIOLATION DROP ROW` is how you keep a pipeline alive, but "dropped" is
-- invisible. So we drop the row *and* keep a copy with the reason attached.
-- Pipeline expectations also publish counts to the event log (see
-- notebooks/40_ops_event_log_dq.py), which gives you the metric; the
-- quarantine table gives you the row.
--
-- The one rule that matters for schema drift
-- ------------------------------------------
-- NEVER name a column that the source has not sent yet. `after.* EXCEPT (...)`
-- is used precisely so that a brand new source column flows from landing to
-- silver without anyone editing this file. A column that does not exist yet
-- cannot be listed in an EXCEPT list, and an explicit reference to one would be
-- an analysis error on the first run (before wave 3 / wave 4 introduce
-- `orders.delivery_sla_breach` and `products.product_launch_date`).
--
-- Consequence, stated honestly: those two drifted columns arrive in silver as
-- whatever Auto Loader inferred (BOOLEAN for the flag, STRING for the date,
-- because JSON has no date type). They are therefore NOT depended on by the
-- gold layer -- gold derives SLA breaches from the base timestamps instead, and
-- the walkthrough notebook compares the two. See docs/architecture.md.
--
-- Column contract for every <entity>_cdc table
-- --------------------------------------------
--   <business columns>   flattened from `after`, keys COALESCEd with `before`
--   _op                  c | u | d | r | t   (the Debezium operation)
--   _event_ts            TIMESTAMP, the real business time of the change. This
--                        is what SEQUENCE BY uses, so SCD2 timelines carry
--                        genuine 2017/2018 dates rather than ingestion time.
--   _lsn                 monotonically increasing source log position
--   _source_entity       logical source entity as reported by the producer
--   _source_table        fully qualified source table
--   _source_snapshot     TRUE for the initial snapshot read (op = 'r')
--   _source_file         the landing file this row came from
--   _ingested_at         when Auto Loader processed the row
--   _rescued_data        non-null only for records that did not match the
--                        inferred schema (our malformed-JSON demo)
--
-- `before` is deliberately NOT carried into _cdc: AUTO CDC only needs the
-- post-image plus the operation. `before` is preserved verbatim in quarantine
-- (as JSON text) so a rejected change can still be replayed by hand.
-- ---------------------------------------------------------------------------


-- ===========================================================================
-- customers   op in {r, u, d}
-- ===========================================================================
CREATE OR REFRESH STREAMING TABLE ${retail_catalog}.${retail_bronze}.customers_cdc (
  CONSTRAINT customers_valid_row
  EXPECT (
    _op IN ('r', 'u', 'd')
    AND customer_id IS NOT NULL
    AND _event_ts IS NOT NULL
  )
  ON VIOLATION DROP ROW
)
COMMENT 'Conformed customer CDC events: one row per source change, typed, with ingestion lineage.'
TBLPROPERTIES ('quality' = 'bronze', 'layer' = 'bronze', 'entity' = 'customers')
AS SELECT
  COALESCE(`after`.customer_id, `before`.customer_id) AS customer_id,
  `after`.* EXCEPT (customer_id),
  op                                                  AS _op,
  CAST(event_ts AS TIMESTAMP)                         AS _event_ts,
  lsn                                                 AS _lsn,
  `source`.`entity`                                   AS _source_entity,
  `source`.`table`                                    AS _source_table,
  `source`.`snapshot`                                 AS _source_snapshot,
  _source_file,
  _ingested_at,
  _rescued_data
FROM STREAM(${retail_catalog}.${retail_bronze}.customers_raw);

CREATE OR REFRESH STREAMING TABLE ${retail_catalog}.${retail_ops}.customers_quarantine
COMMENT 'Customer change records rejected by customers_cdc expectations, with the rejection reason.'
TBLPROPERTIES ('quality' = 'quarantine', 'layer' = 'ops', 'entity' = 'customers')
AS SELECT
  COALESCE(`after`.customer_id, `before`.customer_id) AS customer_id,
  op                                                  AS _op,
  event_ts                                            AS _event_ts_raw,
  TRY_CAST(event_ts AS TIMESTAMP)                      AS _event_ts,
  lsn                                                 AS _lsn,
  `source`.`entity`                                   AS _source_entity,
  to_json(`before`)                                   AS before_json,
  to_json(`after`)                                    AS after_json,
  _rescued_data,
  _source_file,
  _ingested_at,
  CASE
    WHEN _rescued_data IS NOT NULL
      THEN 'rescued_data: record did not match the inferred schema'
    WHEN op IS NULL OR op NOT IN ('r', 'u', 'd')
      THEN concat('invalid_op: ', coalesce(op, 'null'))
    WHEN COALESCE(`after`.customer_id, `before`.customer_id) IS NULL
      THEN 'null_key: customer_id'
    WHEN event_ts IS NULL
      THEN 'missing_event_ts'
    ELSE 'unclassified'
  END AS _quarantine_reason
FROM STREAM(${retail_catalog}.${retail_bronze}.customers_raw)
-- MUST stay the exact complement of the customers_cdc constraint above.
WHERE NOT (
  op IN ('r', 'u', 'd')
  AND COALESCE(`after`.customer_id, `before`.customer_id) IS NOT NULL
  AND event_ts IS NOT NULL
);


-- ===========================================================================
-- products   op in {r, u, d}   (carries the wave-4 schema drift)
-- ===========================================================================
CREATE OR REFRESH STREAMING TABLE ${retail_catalog}.${retail_bronze}.products_cdc (
  CONSTRAINT products_valid_row
  EXPECT (
    _op IN ('r', 'u', 'd')
    AND product_id IS NOT NULL
    AND _event_ts IS NOT NULL
  )
  ON VIOLATION DROP ROW
)
COMMENT 'Conformed product CDC events. product_launch_date / product_name_length appear here via schema drift.'
TBLPROPERTIES ('quality' = 'bronze', 'layer' = 'bronze', 'entity' = 'products')
AS SELECT
  COALESCE(`after`.product_id, `before`.product_id) AS product_id,
  `after`.* EXCEPT (product_id),
  op                                                AS _op,
  CAST(event_ts AS TIMESTAMP)                       AS _event_ts,
  lsn                                               AS _lsn,
  `source`.`entity`                                 AS _source_entity,
  `source`.`table`                                  AS _source_table,
  `source`.`snapshot`                               AS _source_snapshot,
  _source_file,
  _ingested_at,
  _rescued_data
FROM STREAM(${retail_catalog}.${retail_bronze}.products_raw);

CREATE OR REFRESH STREAMING TABLE ${retail_catalog}.${retail_ops}.products_quarantine
COMMENT 'Product change records rejected by products_cdc expectations, with the rejection reason.'
TBLPROPERTIES ('quality' = 'quarantine', 'layer' = 'ops', 'entity' = 'products')
AS SELECT
  COALESCE(`after`.product_id, `before`.product_id) AS product_id,
  op                                                AS _op,
  event_ts                                          AS _event_ts_raw,
  TRY_CAST(event_ts AS TIMESTAMP)                    AS _event_ts,
  lsn                                               AS _lsn,
  `source`.`entity`                                 AS _source_entity,
  to_json(`before`)                                 AS before_json,
  to_json(`after`)                                  AS after_json,
  _rescued_data,
  _source_file,
  _ingested_at,
  CASE
    WHEN _rescued_data IS NOT NULL
      THEN 'rescued_data: record did not match the inferred schema'
    WHEN op IS NULL OR op NOT IN ('r', 'u', 'd')
      THEN concat('invalid_op: ', coalesce(op, 'null'))
    WHEN COALESCE(`after`.product_id, `before`.product_id) IS NULL
      THEN 'null_key: product_id'
    WHEN event_ts IS NULL
      THEN 'missing_event_ts'
    ELSE 'unclassified'
  END AS _quarantine_reason
FROM STREAM(${retail_catalog}.${retail_bronze}.products_raw)
-- MUST stay the exact complement of the products_cdc constraint above.
WHERE NOT (
  op IN ('r', 'u', 'd')
  AND COALESCE(`after`.product_id, `before`.product_id) IS NOT NULL
  AND event_ts IS NOT NULL
);


-- ===========================================================================
-- sellers   op in {r, u, d}
-- ===========================================================================
CREATE OR REFRESH STREAMING TABLE ${retail_catalog}.${retail_bronze}.sellers_cdc (
  CONSTRAINT sellers_valid_row
  EXPECT (
    _op IN ('r', 'u', 'd')
    AND seller_id IS NOT NULL
    AND _event_ts IS NOT NULL
  )
  ON VIOLATION DROP ROW
)
COMMENT 'Conformed seller CDC events: one row per source change, typed, with ingestion lineage.'
TBLPROPERTIES ('quality' = 'bronze', 'layer' = 'bronze', 'entity' = 'sellers')
AS SELECT
  COALESCE(`after`.seller_id, `before`.seller_id) AS seller_id,
  `after`.* EXCEPT (seller_id),
  op                                              AS _op,
  CAST(event_ts AS TIMESTAMP)                     AS _event_ts,
  lsn                                             AS _lsn,
  `source`.`entity`                               AS _source_entity,
  `source`.`table`                                AS _source_table,
  `source`.`snapshot`                             AS _source_snapshot,
  _source_file,
  _ingested_at,
  _rescued_data
FROM STREAM(${retail_catalog}.${retail_bronze}.sellers_raw);

CREATE OR REFRESH STREAMING TABLE ${retail_catalog}.${retail_ops}.sellers_quarantine
COMMENT 'Seller change records rejected by sellers_cdc expectations, with the rejection reason.'
TBLPROPERTIES ('quality' = 'quarantine', 'layer' = 'ops', 'entity' = 'sellers')
AS SELECT
  COALESCE(`after`.seller_id, `before`.seller_id) AS seller_id,
  op                                              AS _op,
  event_ts                                        AS _event_ts_raw,
  TRY_CAST(event_ts AS TIMESTAMP)                  AS _event_ts,
  lsn                                             AS _lsn,
  `source`.`entity`                               AS _source_entity,
  to_json(`before`)                               AS before_json,
  to_json(`after`)                                AS after_json,
  _rescued_data,
  _source_file,
  _ingested_at,
  CASE
    WHEN _rescued_data IS NOT NULL
      THEN 'rescued_data: record did not match the inferred schema'
    WHEN op IS NULL OR op NOT IN ('r', 'u', 'd')
      THEN concat('invalid_op: ', coalesce(op, 'null'))
    WHEN COALESCE(`after`.seller_id, `before`.seller_id) IS NULL
      THEN 'null_key: seller_id'
    WHEN event_ts IS NULL
      THEN 'missing_event_ts'
    ELSE 'unclassified'
  END AS _quarantine_reason
FROM STREAM(${retail_catalog}.${retail_bronze}.sellers_raw)
-- MUST stay the exact complement of the sellers_cdc constraint above.
WHERE NOT (
  op IN ('r', 'u', 'd')
  AND COALESCE(`after`.seller_id, `before`.seller_id) IS NOT NULL
  AND event_ts IS NOT NULL
);


-- ===========================================================================
-- orders   op in {c, u, d}   (carries the wave-3 schema drift)
-- ===========================================================================
-- Olist ships real timestamps as strings ("2017-05-16 15:05:00"), and JSON has
-- no date type, so Auto Loader infers STRING. They are cast here, once, so no
-- downstream layer has to think about it again.
CREATE OR REFRESH STREAMING TABLE ${retail_catalog}.${retail_bronze}.orders_cdc (
  CONSTRAINT orders_valid_row
  EXPECT (
    _op IN ('c', 'u', 'd')
    AND order_id IS NOT NULL
    AND _event_ts IS NOT NULL
  )
  ON VIOLATION DROP ROW
)
COMMENT 'Conformed order CDC events with typed lifecycle timestamps. delivery_sla_breach arrives via schema drift.'
TBLPROPERTIES ('quality' = 'bronze', 'layer' = 'bronze', 'entity' = 'orders')
AS SELECT
  COALESCE(`after`.order_id, `before`.order_id)             AS order_id,
  CAST(`after`.order_purchase_timestamp AS TIMESTAMP)        AS order_purchase_timestamp,
  CAST(`after`.order_approved_at AS TIMESTAMP)               AS order_approved_at,
  CAST(`after`.order_delivered_carrier_date AS TIMESTAMP)    AS order_delivered_carrier_date,
  CAST(`after`.order_delivered_customer_date AS TIMESTAMP)   AS order_delivered_customer_date,
  CAST(`after`.order_estimated_delivery_date AS TIMESTAMP)   AS order_estimated_delivery_date,
  `after`.* EXCEPT (
    order_id,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date
  ),
  op                                                        AS _op,
  CAST(event_ts AS TIMESTAMP)                               AS _event_ts,
  lsn                                                       AS _lsn,
  `source`.`entity`                                         AS _source_entity,
  `source`.`table`                                          AS _source_table,
  `source`.`snapshot`                                       AS _source_snapshot,
  _source_file,
  _ingested_at,
  _rescued_data
FROM STREAM(${retail_catalog}.${retail_bronze}.orders_raw);

CREATE OR REFRESH STREAMING TABLE ${retail_catalog}.${retail_ops}.orders_quarantine
COMMENT 'Order change records rejected by orders_cdc expectations, with the rejection reason.'
TBLPROPERTIES ('quality' = 'quarantine', 'layer' = 'ops', 'entity' = 'orders')
AS SELECT
  COALESCE(`after`.order_id, `before`.order_id) AS order_id,
  op                                            AS _op,
  event_ts                                      AS _event_ts_raw,
  TRY_CAST(event_ts AS TIMESTAMP)                AS _event_ts,
  lsn                                           AS _lsn,
  `source`.`entity`                             AS _source_entity,
  to_json(`before`)                             AS before_json,
  to_json(`after`)                              AS after_json,
  _rescued_data,
  _source_file,
  _ingested_at,
  CASE
    WHEN _rescued_data IS NOT NULL
      THEN 'rescued_data: record did not match the inferred schema'
    WHEN op IS NULL OR op NOT IN ('c', 'u', 'd')
      THEN concat('invalid_op: ', coalesce(op, 'null'))
    WHEN COALESCE(`after`.order_id, `before`.order_id) IS NULL
      THEN 'null_key: order_id'
    WHEN event_ts IS NULL
      THEN 'missing_event_ts'
    ELSE 'unclassified'
  END AS _quarantine_reason
FROM STREAM(${retail_catalog}.${retail_bronze}.orders_raw)
-- MUST stay the exact complement of the orders_cdc constraint above.
WHERE NOT (
  op IN ('c', 'u', 'd')
  AND COALESCE(`after`.order_id, `before`.order_id) IS NOT NULL
  AND event_ts IS NOT NULL
);


-- ===========================================================================
-- order_items   op in {c, u, d}   (composite key)
-- ===========================================================================
CREATE OR REFRESH STREAMING TABLE ${retail_catalog}.${retail_bronze}.order_items_cdc (
  CONSTRAINT order_items_valid_row
  EXPECT (
    _op IN ('c', 'u', 'd')
    AND order_id IS NOT NULL
    AND order_item_id IS NOT NULL
    AND _event_ts IS NOT NULL
  )
  ON VIOLATION DROP ROW
)
COMMENT 'Conformed order line item CDC events. Composite key (order_id, order_item_id).'
TBLPROPERTIES ('quality' = 'bronze', 'layer' = 'bronze', 'entity' = 'order_items')
AS SELECT
  COALESCE(`after`.order_id, `before`.order_id)           AS order_id,
  COALESCE(`after`.order_item_id, `before`.order_item_id) AS order_item_id,
  CAST(`after`.shipping_limit_date AS TIMESTAMP)          AS shipping_limit_date,
  `after`.* EXCEPT (order_id, order_item_id, shipping_limit_date),
  op                                                      AS _op,
  CAST(event_ts AS TIMESTAMP)                             AS _event_ts,
  lsn                                                     AS _lsn,
  `source`.`entity`                                       AS _source_entity,
  `source`.`table`                                        AS _source_table,
  `source`.`snapshot`                                     AS _source_snapshot,
  _source_file,
  _ingested_at,
  _rescued_data
FROM STREAM(${retail_catalog}.${retail_bronze}.order_items_raw);

CREATE OR REFRESH STREAMING TABLE ${retail_catalog}.${retail_ops}.order_items_quarantine
COMMENT 'Line item records rejected by order_items_cdc expectations, with the rejection reason.'
TBLPROPERTIES ('quality' = 'quarantine', 'layer' = 'ops', 'entity' = 'order_items')
AS SELECT
  COALESCE(`after`.order_id, `before`.order_id)           AS order_id,
  COALESCE(`after`.order_item_id, `before`.order_item_id) AS order_item_id,
  op                                                      AS _op,
  event_ts                                                AS _event_ts_raw,
  TRY_CAST(event_ts AS TIMESTAMP)                          AS _event_ts,
  lsn                                                     AS _lsn,
  `source`.`entity`                                       AS _source_entity,
  to_json(`before`)                                       AS before_json,
  to_json(`after`)                                        AS after_json,
  _rescued_data,
  _source_file,
  _ingested_at,
  CASE
    WHEN _rescued_data IS NOT NULL
      THEN 'rescued_data: record did not match the inferred schema'
    WHEN op IS NULL OR op NOT IN ('c', 'u', 'd')
      THEN concat('invalid_op: ', coalesce(op, 'null'))
    WHEN COALESCE(`after`.order_id, `before`.order_id) IS NULL
      THEN 'null_key: order_id'
    WHEN COALESCE(`after`.order_item_id, `before`.order_item_id) IS NULL
      THEN 'null_key: order_item_id'
    WHEN event_ts IS NULL
      THEN 'missing_event_ts'
    ELSE 'unclassified'
  END AS _quarantine_reason
FROM STREAM(${retail_catalog}.${retail_bronze}.order_items_raw)
-- MUST stay the exact complement of the order_items_cdc constraint above.
WHERE NOT (
  op IN ('c', 'u', 'd')
  AND COALESCE(`after`.order_id, `before`.order_id) IS NOT NULL
  AND COALESCE(`after`.order_item_id, `before`.order_item_id) IS NOT NULL
  AND event_ts IS NOT NULL
);


-- ===========================================================================
-- order_payments   op in {c, u, d}   (composite key)
-- ===========================================================================
CREATE OR REFRESH STREAMING TABLE ${retail_catalog}.${retail_bronze}.order_payments_cdc (
  CONSTRAINT order_payments_valid_row
  EXPECT (
    _op IN ('c', 'u', 'd')
    AND order_id IS NOT NULL
    AND payment_sequential IS NOT NULL
    AND _event_ts IS NOT NULL
  )
  ON VIOLATION DROP ROW
)
COMMENT 'Conformed payment CDC events. Composite key (order_id, payment_sequential).'
TBLPROPERTIES ('quality' = 'bronze', 'layer' = 'bronze', 'entity' = 'order_payments')
AS SELECT
  COALESCE(`after`.order_id, `before`.order_id)                       AS order_id,
  COALESCE(`after`.payment_sequential, `before`.payment_sequential)   AS payment_sequential,
  `after`.* EXCEPT (order_id, payment_sequential),
  op                                                                  AS _op,
  CAST(event_ts AS TIMESTAMP)                                         AS _event_ts,
  lsn                                                                 AS _lsn,
  `source`.`entity`                                                   AS _source_entity,
  `source`.`table`                                                    AS _source_table,
  `source`.`snapshot`                                                 AS _source_snapshot,
  _source_file,
  _ingested_at,
  _rescued_data
FROM STREAM(${retail_catalog}.${retail_bronze}.order_payments_raw);

CREATE OR REFRESH STREAMING TABLE ${retail_catalog}.${retail_ops}.order_payments_quarantine
COMMENT 'Payment records rejected by order_payments_cdc expectations, with the rejection reason.'
TBLPROPERTIES ('quality' = 'quarantine', 'layer' = 'ops', 'entity' = 'order_payments')
AS SELECT
  COALESCE(`after`.order_id, `before`.order_id)                     AS order_id,
  COALESCE(`after`.payment_sequential, `before`.payment_sequential) AS payment_sequential,
  op                                                                AS _op,
  event_ts                                                          AS _event_ts_raw,
  TRY_CAST(event_ts AS TIMESTAMP)                                    AS _event_ts,
  lsn                                                               AS _lsn,
  `source`.`entity`                                                 AS _source_entity,
  to_json(`before`)                                                 AS before_json,
  to_json(`after`)                                                  AS after_json,
  _rescued_data,
  _source_file,
  _ingested_at,
  CASE
    WHEN _rescued_data IS NOT NULL
      THEN 'rescued_data: record did not match the inferred schema'
    WHEN op IS NULL OR op NOT IN ('c', 'u', 'd')
      THEN concat('invalid_op: ', coalesce(op, 'null'))
    WHEN COALESCE(`after`.order_id, `before`.order_id) IS NULL
      THEN 'null_key: order_id'
    WHEN COALESCE(`after`.payment_sequential, `before`.payment_sequential) IS NULL
      THEN 'null_key: payment_sequential'
    WHEN event_ts IS NULL
      THEN 'missing_event_ts'
    ELSE 'unclassified'
  END AS _quarantine_reason
FROM STREAM(${retail_catalog}.${retail_bronze}.order_payments_raw)
-- MUST stay the exact complement of the order_payments_cdc constraint above.
WHERE NOT (
  op IN ('c', 'u', 'd')
  AND COALESCE(`after`.order_id, `before`.order_id) IS NOT NULL
  AND COALESCE(`after`.payment_sequential, `before`.payment_sequential) IS NOT NULL
  AND event_ts IS NOT NULL
);


-- ===========================================================================
-- order_reviews   op in {c, u, d, t}
-- ===========================================================================
-- The only entity with a `t` (TRUNCATE) op, and the only one whose predicate
-- has to allow a NULL key: a table-level TRUNCATE has no row key by definition.
--   1. null-key `t` is legal   -> `OR _op = 't'`
--   2. everything else must have a real key and a real business time
CREATE OR REFRESH STREAMING TABLE ${retail_catalog}.${retail_bronze}.order_reviews_cdc (
  CONSTRAINT order_reviews_valid_row
  EXPECT (
    _op IN ('c', 'u', 'd', 't')
    AND (_op = 't' OR (review_id IS NOT NULL AND _event_ts IS NOT NULL))
  )
  ON VIOLATION DROP ROW
)
COMMENT 'Conformed review CDC events. Carries the TRUNCATE op that drives APPLY AS TRUNCATE in silver.'
TBLPROPERTIES ('quality' = 'bronze', 'layer' = 'bronze', 'entity' = 'order_reviews')
AS SELECT
  COALESCE(`after`.review_id, `before`.review_id)     AS review_id,
  CAST(`after`.review_creation_date AS TIMESTAMP)     AS review_creation_date,
  CAST(`after`.review_answer_timestamp AS TIMESTAMP)  AS review_answer_timestamp,
  `after`.* EXCEPT (review_id, review_creation_date, review_answer_timestamp),
  op                                                  AS _op,
  CAST(event_ts AS TIMESTAMP)                         AS _event_ts,
  lsn                                                 AS _lsn,
  `source`.`entity`                                   AS _source_entity,
  `source`.`table`                                    AS _source_table,
  `source`.`snapshot`                                 AS _source_snapshot,
  _source_file,
  _ingested_at,
  _rescued_data
FROM STREAM(${retail_catalog}.${retail_bronze}.order_reviews_raw);

CREATE OR REFRESH STREAMING TABLE ${retail_catalog}.${retail_ops}.order_reviews_quarantine
COMMENT 'Review records rejected by order_reviews_cdc expectations, with the rejection reason.'
TBLPROPERTIES ('quality' = 'quarantine', 'layer' = 'ops', 'entity' = 'order_reviews')
AS SELECT
  COALESCE(`after`.review_id, `before`.review_id) AS review_id,
  op                                              AS _op,
  event_ts                                        AS _event_ts_raw,
  TRY_CAST(event_ts AS TIMESTAMP)                  AS _event_ts,
  lsn                                             AS _lsn,
  `source`.`entity`                               AS _source_entity,
  to_json(`before`)                               AS before_json,
  to_json(`after`)                                AS after_json,
  _rescued_data,
  _source_file,
  _ingested_at,
  CASE
    WHEN _rescued_data IS NOT NULL
      THEN 'rescued_data: record did not match the inferred schema'
    WHEN op IS NULL OR op NOT IN ('c', 'u', 'd', 't')
      THEN concat('invalid_op: ', coalesce(op, 'null'))
    WHEN op <> 't' AND COALESCE(`after`.review_id, `before`.review_id) IS NULL
      THEN 'null_key: review_id'
    WHEN op <> 't' AND event_ts IS NULL
      THEN 'missing_event_ts'
    ELSE 'unclassified'
  END AS _quarantine_reason
FROM STREAM(${retail_catalog}.${retail_bronze}.order_reviews_raw)
-- MUST stay the exact complement of the order_reviews_cdc constraint above.
WHERE NOT (
  op IN ('c', 'u', 'd', 't')
  AND (op = 't' OR (COALESCE(`after`.review_id, `before`.review_id) IS NOT NULL AND event_ts IS NOT NULL))
);
