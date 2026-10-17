-- ---------------------------------------------------------------------------
-- GOLD — star schema + marts (materialized views)
-- ---------------------------------------------------------------------------
-- Why materialized views and not streaming tables
-- -----------------------------------------------
-- Everything here joins and aggregates, and the sources are the targets of
-- AUTO CDC flows. Databricks is explicit that an AUTO CDC target is not a valid
-- input to a streaming table (it updates rows, it does not append them), and
-- that these transformations "are better suited to materialized views". So the
-- gold layer is batch-shaped on purpose: each pipeline update recomputes it
-- from the current state of silver.
--
-- Reading current state from an SCD2 target
-- ----------------------------------------
-- An SCD2 target holds every version of a row. The current one is the version
-- that has not been closed:
--
--     WHERE __END_AT IS NULL
--
-- That single predicate also enforces deletion: when AUTO CDC applies a
-- `d` op to an SCD2 target it closes the open version and opens nothing, so a
-- GDPR-erased customer simply falls out of every gold mart. Right-to-be-
-- forgotten propagates for free. See notebooks/20_time_travel_and_cdf.py.
--
-- `AND <key> IS NOT NULL` is belt-and-braces: an SCD2 delete is retained
-- internally as a tombstone for out-of-order handling, and we never want a
-- tombstone to survive into a dimension.
--
-- Careful: SCD1 targets have no __START_AT / __END_AT. `silver.orders` is
-- already current state, so there is nothing to filter there.
--
-- The one thing NOT used here, on purpose
-- --------------------------------------
-- `orders.delivery_sla_breach` and `products.product_launch_date` are the two
-- columns that arrive by schema drift (wave 3 / wave 4). They are deliberately
-- absent from every query below, for two reasons:
--
--   1. Naming a column that does not exist yet is an analysis error, and on the
--      first run (wave 1) those columns have not arrived. A gold mart that
--      mentions them would take the whole pipeline down until wave 4.
--   2. Deriving the SLA breach ourselves from the base timestamps is the better
--      engineering answer anyway -- it does not depend on the source's
--      interpretation of its own flag, and it can be recomputed historically.
--
-- So gold computes the breach (see fact_orders) and the walkthrough notebook
-- reconciles our derivation against the source's drifted flag.
-- ---------------------------------------------------------------------------


-- ===========================================================================
-- dim_geolocation — reference data, published as a dimension
-- ===========================================================================
-- inferSchema is switched OFF deliberately. With inference on, Spark sees
-- "01001" and types the zip prefix as INT, which silently drops the leading
-- zero and breaks the join to customers. Read everything as text, then cast
-- only the columns that really are numbers.
CREATE OR REFRESH MATERIALIZED VIEW ${retail_catalog}.${retail_gold}.dim_geolocation
COMMENT 'Reference geolocation, aggregated to zip-code-prefix grain (mean lat/lng per prefix).'
AS SELECT
  geolocation_zip_code_prefix       AS zip_code_prefix,
  CAST(geolocation_lat AS DOUBLE)   AS latitude,
  CAST(geolocation_lng AS DOUBLE)   AS longitude,
  geolocation_city                  AS city,
  geolocation_state                 AS state,
  CAST(sample_rows AS INT)          AS sample_rows
FROM read_files(
  '${retail_landing}/reference/geolocation_by_zip.csv',
  format => 'csv',
  header => true,
  inferSchema => false
);


-- ===========================================================================
-- dim_customer — current version, geolocation-enriched
-- ===========================================================================
CREATE OR REFRESH MATERIALIZED VIEW ${retail_catalog}.${retail_gold}.dim_customer
COMMENT 'Current customer dimension (one row per live customer), enriched with geolocation.'
AS SELECT
  c.customer_id,
  c.customer_unique_id,
  c.customer_zip_code_prefix        AS customer_zip_code,
  c.customer_city,
  c.customer_state,
  g.latitude                        AS customer_lat,
  g.longitude                       AS customer_lng,
  c.__START_AT                      AS valid_from,
  c.__END_AT                        AS valid_to
FROM ${retail_catalog}.${retail_silver}.customers c
LEFT JOIN ${retail_catalog}.${retail_gold}.dim_geolocation g
  ON c.customer_zip_code_prefix = g.zip_code_prefix
WHERE c.__END_AT IS NULL
  AND c.customer_id IS NOT NULL;


-- ===========================================================================
-- dim_customer_history — every version, kept for point-in-time analysis
-- ===========================================================================
CREATE OR REFRESH MATERIALIZED VIEW ${retail_catalog}.${retail_gold}.dim_customer_history
COMMENT 'All customer versions (SCD2) with validity columns renamed for consumption.'
AS SELECT
  c.customer_id,
  c.customer_unique_id,
  c.customer_zip_code_prefix        AS customer_zip_code,
  c.customer_city,
  c.customer_state,
  c.__START_AT                      AS valid_from,
  c.__END_AT                        AS valid_to,
  c.__END_AT IS NULL                AS is_current
FROM ${retail_catalog}.${retail_silver}.customers c
WHERE c.customer_id IS NOT NULL;


-- ===========================================================================
-- dim_product — current version
-- ===========================================================================
CREATE OR REFRESH MATERIALIZED VIEW ${retail_catalog}.${retail_gold}.dim_product
COMMENT 'Current product dimension. Excludes the drifted wave-4 columns (see the note at the top of this file).'
AS SELECT
  p.product_id,
  p.product_category_name,
  p.product_category_name_english,
  p.product_name_lenght             AS product_name_len,   -- source misspelling, kept as shipped
  p.product_description_lenght      AS product_description_len,
  p.product_photos_qty,
  p.product_weight_g,
  p.product_length_cm,
  p.product_height_cm,
  p.product_width_cm,
  p.__START_AT                      AS valid_from,
  p.__END_AT                        AS valid_to
FROM ${retail_catalog}.${retail_silver}.products p
WHERE p.__END_AT IS NULL
  AND p.product_id IS NOT NULL;


-- ===========================================================================
-- dim_seller — current version, geolocation-enriched
-- ===========================================================================
CREATE OR REFRESH MATERIALIZED VIEW ${retail_catalog}.${retail_gold}.dim_seller
COMMENT 'Current seller dimension (one row per live seller), enriched with geolocation.'
AS SELECT
  s.seller_id,
  s.seller_zip_code_prefix          AS seller_zip_code,
  s.seller_city,
  s.seller_state,
  g.latitude                        AS seller_lat,
  g.longitude                       AS seller_lng,
  s.__START_AT                      AS valid_from,
  s.__END_AT                        AS valid_to
FROM ${retail_catalog}.${retail_silver}.sellers s
LEFT JOIN ${retail_catalog}.${retail_gold}.dim_geolocation g
  ON s.seller_zip_code_prefix = g.zip_code_prefix
WHERE s.__END_AT IS NULL
  AND s.seller_id IS NOT NULL;


-- ===========================================================================
-- fact_orders — order grain, with delivery-SLA metrics derived from base timestamps
-- ===========================================================================
CREATE OR REFRESH MATERIALIZED VIEW ${retail_catalog}.${retail_gold}.fact_orders
COMMENT 'Order-grain fact table with computed approval, carrier and delivery lead times and an SLA-breach flag.'
AS SELECT
  o.order_id,
  o.customer_id,
  o.order_status,
  o.order_status = 'delivered'                                          AS is_delivered,
  o.order_status IN ('canceled', 'unavailable')                         AS is_canceled,
  o.order_purchase_timestamp,
  o.order_approved_at,
  o.order_delivered_carrier_date,
  o.order_delivered_customer_date,
  o.order_estimated_delivery_date,
  CAST(o.order_purchase_timestamp AS DATE)                              AS purchase_date,
  DATE_TRUNC('MONTH', o.order_purchase_timestamp)                       AS purchase_month,
  DATEDIFF(o.order_approved_at, o.order_purchase_timestamp)             AS days_to_approve,
  DATEDIFF(o.order_delivered_carrier_date, o.order_purchase_timestamp)  AS days_to_carrier,
  DATEDIFF(o.order_delivered_customer_date, o.order_purchase_timestamp) AS days_to_deliver,
  DATEDIFF(o.order_delivered_customer_date, o.order_estimated_delivery_date) AS days_vs_estimate,
  (
    o.order_delivered_customer_date IS NOT NULL
    AND o.order_estimated_delivery_date IS NOT NULL
    AND o.order_delivered_customer_date > o.order_estimated_delivery_date
  )                                                                     AS is_late
FROM ${retail_catalog}.${retail_silver}.orders o;


-- ===========================================================================
-- fact_order_items — line-item grain (the revenue fact)
-- ===========================================================================
CREATE OR REFRESH MATERIALIZED VIEW ${retail_catalog}.${retail_gold}.fact_order_items
COMMENT 'Line-item-grain fact table: revenue, freight and the conformed dimension keys.'
AS SELECT
  i.order_id,
  i.order_item_id,
  i.product_id,
  i.seller_id,
  o.customer_id,
  p.product_category_name_english       AS product_category,
  CAST(o.order_purchase_timestamp AS DATE) AS purchase_date,
  o.order_status,
  o.order_status = 'delivered'          AS is_delivered,
  i.price,
  i.freight_value,
  i.price + i.freight_value             AS item_total
FROM ${retail_catalog}.${retail_silver}.order_items i
LEFT JOIN ${retail_catalog}.${retail_silver}.orders o
  ON i.order_id = o.order_id
LEFT JOIN ${retail_catalog}.${retail_gold}.dim_product p
  ON i.product_id = p.product_id;


-- ===========================================================================
-- mart_delivery_sla — delivery performance by month and customer state
-- ===========================================================================
CREATE OR REFRESH MATERIALIZED VIEW ${retail_catalog}.${retail_gold}.mart_delivery_sla
COMMENT 'Delivery performance by purchase month and customer state: volume, lead time and late rate.'
AS SELECT
  f.purchase_month,
  c.customer_state,
  COUNT(*)                                                              AS orders,
  SUM(CASE WHEN f.is_delivered THEN 1 ELSE 0 END)                       AS delivered_orders,
  SUM(CASE WHEN f.is_late THEN 1 ELSE 0 END)                            AS late_orders,
  ROUND(AVG(CASE WHEN f.is_delivered THEN f.days_to_deliver END), 2)    AS avg_days_to_deliver,
  ROUND(AVG(CASE WHEN f.is_delivered THEN f.days_to_approve END), 2)    AS avg_days_to_approve,
  ROUND(
    100.0 * SUM(CASE WHEN f.is_late THEN 1 ELSE 0 END)
    / NULLIF(SUM(CASE WHEN f.is_delivered THEN 1 ELSE 0 END), 0)
  , 2)                                                                  AS late_rate_pct
FROM ${retail_catalog}.${retail_gold}.fact_orders f
LEFT JOIN ${retail_catalog}.${retail_gold}.dim_customer c
  ON f.customer_id = c.customer_id
GROUP BY f.purchase_month, c.customer_state;


-- ===========================================================================
-- mart_category_revenue — revenue by product category
-- ===========================================================================
CREATE OR REFRESH MATERIALIZED VIEW ${retail_catalog}.${retail_gold}.mart_category_revenue
COMMENT 'Revenue, volume and basket metrics by product category. Canceled and unavailable orders excluded.'
AS SELECT
  COALESCE(i.product_category, 'unknown')       AS product_category,
  COUNT(*)                                      AS items_sold,
  COUNT(DISTINCT i.order_id)                    AS orders,
  COUNT(DISTINCT i.seller_id)                   AS sellers,
  ROUND(SUM(i.item_total), 2)                   AS revenue,
  ROUND(AVG(i.price), 2)                        AS avg_item_price,
  ROUND(AVG(i.freight_value), 2)                AS avg_freight_value
FROM ${retail_catalog}.${retail_gold}.fact_order_items i
WHERE i.order_status NOT IN ('canceled', 'unavailable')
GROUP BY COALESCE(i.product_category, 'unknown');


-- ===========================================================================
-- customer_360 — one row per *person*
-- ===========================================================================
-- In Olist, customer_id is minted per order; customer_unique_id is the human.
-- Rolling up by customer_id would report 1.0 orders per customer and look
-- broken. Rolling up by customer_unique_id is the number a retail team actually
-- means by "customer".
CREATE OR REFRESH MATERIALIZED VIEW ${retail_catalog}.${retail_gold}.customer_360
COMMENT 'Per-customer (customer_unique_id) rollup: lifetime orders, value, delivery experience and recency.'
AS WITH orders_by_person AS (
  SELECT
    c.customer_unique_id,
    COUNT(*)                                                          AS lifetime_orders,
    SUM(CASE WHEN f.is_delivered THEN 1 ELSE 0 END)                   AS delivered_orders,
    SUM(CASE WHEN f.is_canceled THEN 1 ELSE 0 END)                    AS canceled_orders,
    SUM(CASE WHEN f.is_late THEN 1 ELSE 0 END)                        AS late_orders,
    MIN(f.order_purchase_timestamp)                                   AS first_order_at,
    MAX(f.order_purchase_timestamp)                                   AS last_order_at,
    ROUND(AVG(CASE WHEN f.is_delivered THEN f.days_to_deliver END), 2) AS avg_days_to_deliver
  FROM ${retail_catalog}.${retail_gold}.fact_orders f
  JOIN ${retail_catalog}.${retail_gold}.dim_customer c
    ON f.customer_id = c.customer_id
  GROUP BY c.customer_unique_id
),
value_by_person AS (
  SELECT
    c.customer_unique_id,
    ROUND(SUM(i.item_total), 2)                                       AS lifetime_value
  FROM ${retail_catalog}.${retail_gold}.fact_order_items i
  JOIN ${retail_catalog}.${retail_gold}.dim_customer c
    ON i.customer_id = c.customer_id
  GROUP BY c.customer_unique_id
)
SELECT
  c.customer_unique_id,
  MIN(c.customer_state)                                               AS customer_state,
  MIN(c.customer_city)                                                AS customer_city,
  o.lifetime_orders,
  o.delivered_orders,
  o.canceled_orders,
  o.late_orders,
  o.first_order_at,
  o.last_order_at,
  o.avg_days_to_deliver,
  v.lifetime_value,
  ROUND(v.lifetime_value / NULLIF(o.lifetime_orders, 0), 2)           AS avg_order_value
FROM ${retail_catalog}.${retail_gold}.dim_customer c
LEFT JOIN orders_by_person o ON c.customer_unique_id = o.customer_unique_id
LEFT JOIN value_by_person v ON c.customer_unique_id = v.customer_unique_id
GROUP BY
  c.customer_unique_id,
  o.lifetime_orders,
  o.delivered_orders,
  o.canceled_orders,
  o.late_orders,
  o.first_order_at,
  o.last_order_at,
  o.avg_days_to_deliver,
  v.lifetime_value;
