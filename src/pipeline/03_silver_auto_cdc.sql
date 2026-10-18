-- ---------------------------------------------------------------------------
-- SILVER — AUTO CDC (SCD1 + SCD2)
-- ---------------------------------------------------------------------------
-- Everything above this layer produced *change events*. This layer turns them
-- into *current state with history*, using AUTO CDC. From here on, no code has
-- to reason about op codes: the out-of-order arrivals, the late arrivals and
-- the replays have already been resolved.
--
-- ONE flow, TWO declarations. `AUTO CDC` never creates its own target, so each
-- entity is a pair: a STREAMING TABLE to hold the result, then a CREATE FLOW
-- that says how to fold the change stream into it. (The inline
-- `FLOW AUTO CDC` form exists too, but it is Beta and requires the PREVIEW
-- channel -- not appropriate for a demo that has to run on Free Edition.)
--
-- SCD choice, per entity, and why
-- ------------------------------
--   customers              SCD2  addresses genuinely change; the history is the point
--   products               SCD2  recategorisation / repricing must be replayable
--   sellers                SCD2  with TRACK HISTORY ON seller_city, seller_state only
--   orders                 SCD1  latest state wins; the timeline lives in the
--                                sibling orders_status_history table
--   orders_status_history  SCD2  narrowed to (order_id, order_status) -- this is
--                                the delivery-lifecycle timeline, straight from
--                                the same change stream
--   order_items            SCD1  composite key; corrections are overwrites
--   order_payments         SCD1  composite key; corrections/reversals
--   order_reviews          SCD1  with IGNORE NULL UPDATES + APPLY AS TRUNCATE
--
-- SEQUENCE BY, and why it is not "ingestion time"
-- ----------------------------------------------
-- SEQUENCE BY is the business-time column `_event_ts`, not the time the file
-- landed. That is the whole trick that makes this demo look like a real one:
-- because the source timestamps are the true 2017/2018 lifecycle dates, an
-- SCD2 backfill closes its versions with real dates, and an out-of-order
-- arrival that shows up three waves late is still slotted into the correct
-- historical position rather than being appended as "now".
--
-- SCD1 flows sequence by STRUCT(_event_ts, _lsn) -- a total order, not just a
-- per-key order, so a tie on business time can never make the result depend on
-- which micro-batch happened to arrive first.
--
-- The metadata columns (_op, _event_ts, _lsn, the _source_* lineage and
-- _rescued_data) exist only to drive the CDC logic. They are stripped by the
-- COLUMNS clause so that silver contains business columns and nothing else.
--
-- `delta.enableChangeDataFeed` is set explicitly on every target because the
-- gold layer reads these tables as change feeds. The pipeline also sets
-- spark.databricks.delta.properties.defaults.enableChangeDataFeed = true; being
-- explicit here means the gold layer cannot silently break if that default is
-- ever dropped.
-- ---------------------------------------------------------------------------


-- ===========================================================================
-- customers -- SCD Type 2
-- ===========================================================================
CREATE OR REFRESH STREAMING TABLE ${retail_catalog}.${retail_silver}.customers
COMMENT 'Customer dimension history. One row per version, with __START_AT / __END_AT in business time.'
TBLPROPERTIES (
  'quality' = 'silver',
  'layer' = 'silver',
  'entity' = 'customers',
  'delta.enableChangeDataFeed' = 'true'
);

CREATE FLOW customers_scd2 AS AUTO CDC INTO
  ${retail_catalog}.${retail_silver}.customers
FROM STREAM(${retail_catalog}.${retail_bronze}.customers_cdc)
  KEYS (customer_id)
  APPLY AS DELETE WHEN _op = 'd'
  SEQUENCE BY _event_ts
  COLUMNS * EXCEPT (
    _op, _event_ts, _lsn,
    _source_entity, _source_table, _source_snapshot, _source_file, _ingested_at, _rescued_data
  )
  STORED AS SCD TYPE 2;


-- ===========================================================================
-- products -- SCD Type 2
-- ===========================================================================
CREATE OR REFRESH STREAMING TABLE ${retail_catalog}.${retail_silver}.products
COMMENT 'Product dimension history. Records wave-4 columns (product_launch_date, product_name_length) via schema drift.'
TBLPROPERTIES (
  'quality' = 'silver',
  'layer' = 'silver',
  'entity' = 'products',
  'delta.enableChangeDataFeed' = 'true'
);

CREATE FLOW products_scd2 AS AUTO CDC INTO
  ${retail_catalog}.${retail_silver}.products
FROM STREAM(${retail_catalog}.${retail_bronze}.products_cdc)
  KEYS (product_id)
  APPLY AS DELETE WHEN _op = 'd'
  SEQUENCE BY _event_ts
  COLUMNS * EXCEPT (
    _op, _event_ts, _lsn,
    _source_entity, _source_table, _source_snapshot, _source_file, _ingested_at, _rescued_data
  )
  STORED AS SCD TYPE 2;


-- ===========================================================================
-- sellers -- SCD Type 2, history tracked on a subset of columns
-- ===========================================================================
-- TRACK HISTORY ON seller_city, seller_state means only a *relocation* opens a
-- new version. A change to any other seller column (the zip, say) is applied to
-- the open row without cutting a new one. With this dataset every relocation
-- happens to move the zip too, so the distinction is not visible in the seeded
-- data -- notebooks/30_reprocess_and_late_data.py injects a zip-only change to
-- make it visible on demand.
--
-- SYNTAX TRAP -- read this before "tidying" the clauses below.
--
--   Column lists in AUTO CDC are BARE. No parentheses:
--     COLUMNS order_id, order_status
--     TRACK HISTORY ON seller_city, seller_state
--     COLUMNS * EXCEPT (a, b)          <- the EXCEPT form does use parens
--
-- Both clauses take the same bare `columnList`, so the parenthesised form is
-- wrong in both. It fails to parse, and it fails the same way every time:
--   [PARSE_SYNTAX_ERROR] Syntax error at or near '(' SQLSTATE: 42601
-- with the caret parked on the '('.
--
-- The Databricks docs actively mislead here. The AUTO CDC INTO reference gives
-- the example `COLUMNS (userId, name, city)` WITH parentheses, and the syntax
-- block lists the same `columnList` nonterminal for COLUMNS, IGNORE NULL
-- UPDATES ON and TRACK HISTORY ON. Only the first is documented wrong, but it
-- is enough to break the pipeline -- twice, in this file's history.
--
-- And because a pipeline analyses the whole graph before executing anything,
-- one parenthesised list stops *every* table in the pipeline from being
-- created. The update fails in about a second and builds nothing, so a
-- two-character mistake looks like a total failure.
CREATE OR REFRESH STREAMING TABLE ${retail_catalog}.${retail_silver}.sellers
COMMENT 'Seller dimension history. History is tracked on (seller_city, seller_state) only.'
TBLPROPERTIES (
  'quality' = 'silver',
  'layer' = 'silver',
  'entity' = 'sellers',
  'delta.enableChangeDataFeed' = 'true'
);

CREATE FLOW sellers_scd2 AS AUTO CDC INTO
  ${retail_catalog}.${retail_silver}.sellers
FROM STREAM(${retail_catalog}.${retail_bronze}.sellers_cdc)
  KEYS (seller_id)
  APPLY AS DELETE WHEN _op = 'd'
  SEQUENCE BY _event_ts
  COLUMNS * EXCEPT (
    _op, _event_ts, _lsn,
    _source_entity, _source_table, _source_snapshot, _source_file, _ingested_at, _rescued_data
  )
  STORED AS SCD TYPE 2
  TRACK HISTORY ON seller_city, seller_state;


-- ===========================================================================
-- orders -- SCD Type 1 (current state)
-- ===========================================================================
-- Note this reads the *same* bronze change stream as orders_status_history
-- below. Two flows, two shapes, one source, no duplicated ingestion.
CREATE OR REFRESH STREAMING TABLE ${retail_catalog}.${retail_silver}.orders
COMMENT 'Current order state. Latest change wins; the status timeline is in orders_status_history.'
TBLPROPERTIES (
  'quality' = 'silver',
  'layer' = 'silver',
  'entity' = 'orders',
  'delta.enableChangeDataFeed' = 'true'
);

CREATE FLOW orders_scd1 AS AUTO CDC INTO
  ${retail_catalog}.${retail_silver}.orders
FROM STREAM(${retail_catalog}.${retail_bronze}.orders_cdc)
  KEYS (order_id)
  APPLY AS DELETE WHEN _op = 'd'
  SEQUENCE BY STRUCT(_event_ts, _lsn)
  COLUMNS * EXCEPT (
    _op, _event_ts, _lsn,
    _source_entity, _source_table, _source_snapshot, _source_file, _ingested_at, _rescued_data
  )
  STORED AS SCD TYPE 1;


-- ===========================================================================
-- orders_status_history -- SCD Type 2, narrowed to the status column
-- ===========================================================================
-- COLUMNS order_id, order_status drops everything except the key and the
-- tracked attribute, so the target is a clean delivery-lifecycle timeline:
-- created -> approved -> shipped|invoiced -> delivered, or -> canceled.
-- TRACK HISTORY ON order_status makes the intent explicit: only a status change
-- opens a version. Bare lists, no parentheses -- see the syntax trap at the
-- sellers flow above.
CREATE OR REFRESH STREAMING TABLE ${retail_catalog}.${retail_silver}.orders_status_history
COMMENT 'Order status timeline (SCD2). One row per status period, straight from the order change stream.'
TBLPROPERTIES (
  'quality' = 'silver',
  'layer' = 'silver',
  'entity' = 'orders_status_history',
  'delta.enableChangeDataFeed' = 'true'
);

CREATE FLOW orders_status_history_scd2 AS AUTO CDC INTO
  ${retail_catalog}.${retail_silver}.orders_status_history
FROM STREAM(${retail_catalog}.${retail_bronze}.orders_cdc)
  KEYS (order_id)
  APPLY AS DELETE WHEN _op = 'd'
  SEQUENCE BY _event_ts
  COLUMNS order_id, order_status
  STORED AS SCD TYPE 2
  TRACK HISTORY ON order_status;


-- ===========================================================================
-- order_items -- SCD Type 1 (composite key)
-- ===========================================================================
CREATE OR REFRESH STREAMING TABLE ${retail_catalog}.${retail_silver}.order_items
COMMENT 'Current order line items. Composite key (order_id, order_item_id); price corrections overwrite.'
TBLPROPERTIES (
  'quality' = 'silver',
  'layer' = 'silver',
  'entity' = 'order_items',
  'delta.enableChangeDataFeed' = 'true'
);

CREATE FLOW order_items_scd1 AS AUTO CDC INTO
  ${retail_catalog}.${retail_silver}.order_items
FROM STREAM(${retail_catalog}.${retail_bronze}.order_items_cdc)
  KEYS (order_id, order_item_id)
  APPLY AS DELETE WHEN _op = 'd'
  SEQUENCE BY STRUCT(_event_ts, _lsn)
  COLUMNS * EXCEPT (
    _op, _event_ts, _lsn,
    _source_entity, _source_table, _source_snapshot, _source_file, _ingested_at, _rescued_data
  )
  STORED AS SCD TYPE 1;


-- ===========================================================================
-- order_payments -- SCD Type 1 (composite key)
-- ===========================================================================
CREATE OR REFRESH STREAMING TABLE ${retail_catalog}.${retail_silver}.order_payments
COMMENT 'Current order payments. Composite key (order_id, payment_sequential); reversals delete the row.'
TBLPROPERTIES (
  'quality' = 'silver',
  'layer' = 'silver',
  'entity' = 'order_payments',
  'delta.enableChangeDataFeed' = 'true'
);

CREATE FLOW order_payments_scd1 AS AUTO CDC INTO
  ${retail_catalog}.${retail_silver}.order_payments
FROM STREAM(${retail_catalog}.${retail_bronze}.order_payments_cdc)
  KEYS (order_id, payment_sequential)
  APPLY AS DELETE WHEN _op = 'd'
  SEQUENCE BY STRUCT(_event_ts, _lsn)
  COLUMNS * EXCEPT (
    _op, _event_ts, _lsn,
    _source_entity, _source_table, _source_snapshot, _source_file, _ingested_at, _rescued_data
  )
  STORED AS SCD TYPE 1;


-- ===========================================================================
-- order_reviews -- SCD Type 1, partial updates + table-level TRUNCATE
-- ===========================================================================
-- The two clauses here are doing real work:
--
--   IGNORE NULL UPDATES   a review edit carries only review_score (and the
--                         identifying columns). Without this clause the null
--                         title/message on that record would blank out the
--                         customer's original comment. With it, null means
--                         "unchanged".
--                         Cost of the convenience: a genuine "delete this
--                         comment" (an explicit null that should overwrite)
--                         can no longer be expressed. When that matters, the
--                         documented alternative is COLUMNS TO UPDATE, where
--                         each record names the columns it intends to write.
--
--   APPLY AS TRUNCATE     the producer sends a `t` op meaning "table truncated,
--                         reloading from scratch". Honouring it is what stops
--                         the reload from being mistaken for 25k duplicate
--                         reviews. SCD1 only -- SCD2 has no truncate.
--
-- The truncate record has a NULL review_id by design (a whole-table event has
-- no row key); the bronze expectation for this entity is written to allow that.
CREATE OR REFRESH STREAMING TABLE ${retail_catalog}.${retail_silver}.order_reviews
COMMENT 'Current reviews with partial-update handling, and TRUNCATE support for source reloads.'
TBLPROPERTIES (
  'quality' = 'silver',
  'layer' = 'silver',
  'entity' = 'order_reviews',
  'delta.enableChangeDataFeed' = 'true'
);

CREATE FLOW order_reviews_scd1 AS AUTO CDC INTO
  ${retail_catalog}.${retail_silver}.order_reviews
FROM STREAM(${retail_catalog}.${retail_bronze}.order_reviews_cdc)
  KEYS (review_id)
  IGNORE NULL UPDATES
  APPLY AS DELETE WHEN _op = 'd'
  APPLY AS TRUNCATE WHEN _op = 't'
  SEQUENCE BY _event_ts
  COLUMNS * EXCEPT (
    _op, _event_ts, _lsn,
    _source_entity, _source_table, _source_snapshot, _source_file, _ingested_at, _rescued_data
  )
  STORED AS SCD TYPE 1;
