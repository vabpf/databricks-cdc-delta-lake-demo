# Architecture

This document is the reasoning behind the code. Where a decision has a
consequence a reader might reasonably get wrong, the consequence is stated -- and
so is the wrong answer.

- [1. The problem CDC actually solves](#1-the-problem-cdc-actually-solves)
- [2. The source contract](#2-the-source-contract)
- [3. Bronze: two layers, three outcomes per record](#3-bronze-two-layers-three-outcomes-per-record)
- [4. Silver: AUTO CDC, and why each entity is shaped the way it is](#4-silver-auto-cdc-and-why-each-entity-is-shaped-the-way-it-is)
- [5. Sequencing: business time, not arrival time](#5-sequencing-business-time-not-arrival-time)
- [6. Schema drift, and the one thing you must not do](#6-schema-drift-and-the-one-thing-you-must-not-do)
- [7. Quarantine: making "dropped" visible](#7-quarantine-making-dropped-visible)
- [8. Gold: change feeds and the star schema](#8-gold-change-feeds-and-the-star-schema)
- [9. The SCD2 change-feed subtlety](#9-the-scd2-change-feed-subtlety)
- [10. Unity Catalog vs the Hive metastore](#10-unity-catalog-vs-the-hive-metastore)
- [11. Deliberate non-choices (extensions)](#11-deliberate-non-choices-extensions)
- [12. Free Edition constraints, and what they forced](#12-free-edition-constraints-and-what-they-forced)
- [13. What to watch if you change something](#13-what-to-watch-if-you-change-something)

---

## 1. The problem CDC actually solves

A nightly batch extract tells you what the world looks like *now*. A change data
capture stream tells you *what changed*: which row, from what to what, when, and
in what order. That distinction is what makes the interesting problems appear:

* events arrive **out of order** relative to the business events they describe;
* events arrive **late** -- sometimes days late;
* events are delivered **more than once** (at-least-once is the only achievable
  end-to-end guarantee);
* rows are **deleted**, and a delete has no "after" image to read;
* the source's **schema drifts** under you;
* some events are simply **garbage**.

A medallion pipeline whose source is a CDC stream has to answer all six. The
sections below are how this one does.

---

## 2. The source contract

The Kaggle dataset is snapshots, so `tools/generate_cdc.py` reconstructs the
lifecycle and emits Debezium-shaped events. One event is one row change:

```json
{
  "op": "u",
  "event_ts": "2018-03-14T09:22:11.000",
  "ts_ms": 1521019331000,
  "lsn": 41231,
  "source": {"connector": "olist-cdc-sim", "version": "1.0.0",
             "entity": "orders", "table": "olist.orders", "snapshot": false},
  "before": { ...row image before the change... },
  "after":  { ...row image after the change... }
}
```

The conventions, all of them load-bearing:

* `op ∈ {c, u, d, r, t, x}` -- create, update, delete, snapshot-read, truncate,
  unknown. The `r`/`c` split matters: reference entities (`customers`, `products`,
  `sellers`) arrive as an initial **snapshot** (`r`); transactional entities
  (`orders`, `order_items`, `order_payments`, `order_reviews`) are **created**
  in flight (`c`).
* `after` is `null` on delete, `before` is `null` on create. The whole-image-null
  convention means a consumer never has to guess whether a missing field means
  "unchanged" or "explicitly null".
* `event_ts` is **business time** -- the real 2017/2018 date the change happened.
  This is the property everything else depends on.
* `lsn` is a monotonically increasing source log position, used as a tie-breaker.

Entities and the operations each one actually carries:

| entity | ops | notes |
|---|---|---|
| `customers` | `r, u, d` | address moves (`u`), GDPR erasure (`d`) |
| `products` | `r, u, d` | recategorisation/discontinuation; carries the schema drift |
| `sellers` | `r, u, d` | relocations |
| `orders` | `c, u, d` | the lifecycle; carries schema drift; cancellations sometimes hard-deleted |
| `order_items` | `c, u, d` | composite key `(order_id, order_item_id)`; price corrections |
| `order_payments` | `c, u, d` | composite key `(order_id, payment_sequential)`; reversals |
| `order_reviews` | `c, u, d, t` | partial edits, and exactly one table-level `t` |

The landing layout is a **wave** per file:

```
<volume>/events/<entity>/<entity>_w<NN>_p<PPP>.jsonl.gz
<volume>/reference/geolocation_by_zip.csv
<volume>/_manifest.json
```

`w<NN>` is the delivery wave, and it lives in the *filename*, not a directory.
That makes a wave a property of a file: appending one file is one delivery, and
Auto Loader needs no extra bookkeeping to notice it.

---

## 3. Bronze: two layers, three outcomes per record

Bronze is split on purpose.

### `<entity>_raw` -- the envelope, untouched

`01_bronze_autoloader.py`. Nothing is parsed or filtered. The raw envelope lands
intact, plus lineage (`_source_file`, `_source_file_ts`, `_ingested_at`). If a
downstream rule turns out to be wrong, the original bytes are still here.

Auto Loader options that matter:

| option | why |
|---|---|
| `cloudFiles.schemaEvolutionMode = addNewColumns` | a new source field never breaks ingestion |
| `cloudFiles.rescuedDataColumn = _rescued_data` | unparseable records are *kept*, not dropped |
| `cloudFiles.inferColumnTypes = true` | real types, not everything as string |
| `cloudFiles.includeExistingFiles = true` | wave 1 is read on the first run |

### `<entity>_cdc` -- the contract

`02_bronze_conform.sql`. The envelope is unpacked into one typed, flat row:

* business columns come from `after.*`;
* **key** columns are `COALESCE(after.key, before.key)` -- and only keys, because
  coalescing a business column would silently undo a legitimate null write;
* a handful of nested timestamp columns are `CAST(... AS TIMESTAMP)` once, here,
  so nothing downstream thinks about it;
* the metadata columns are appended: `_op`, `_event_ts`, `_lsn`, `_source_entity`,
  `_source_table`, `_source_snapshot`, `_source_file`, `_ingested_at`,
  `_rescued_data`.

`before` is **not** carried forward. AUTO CDC only needs the post-image plus the
operation. The full `before` image survives in quarantine (as JSON text) so a
rejected change can still be replayed by hand.

### `<entity>_quarantine` -- the complement

A row either lands in `_cdc` or in `_quarantine`. Never both, never neither. See
[§7](#7-quarantine-making-dropped-visible).

So every input record has exactly one of three fates: conformed, quarantined, or
none -- and the third is impossible by construction.

---

## 4. Silver: AUTO CDC, and why each entity is shaped the way it is

`03_silver_auto_cdc.sql`. This is where change events become **current state with
history**, and from here down no code reasons about `op` codes.

### The two-declaration pattern

`AUTO CDC` never creates its target. Each entity is a pair:

```sql
CREATE OR REFRESH STREAMING TABLE ... <target> ...;

CREATE FLOW <name> AS AUTO CDC INTO <target>
FROM STREAM(<bronze source>)
  KEYS (...)
  [IGNORE NULL UPDATES]
  APPLY AS DELETE WHEN _op = 'd'
  [APPLY AS TRUNCATE WHEN _op = 't']
  SEQUENCE BY ...
  COLUMNS ...
  STORED AS SCD TYPE n
  [TRACK HISTORY ON ...];
```

Clause order is fixed, and `TRACK HISTORY ON` belongs at the *end*, after
`STORED AS`. Two clause shapes are easy to confuse and worth stating plainly,
because getting them wrong is a parse error, not a warning:

```sql
COLUMNS (a, b)                  -- parenthesised list
TRACK HISTORY ON a, b           -- bare list, NO parentheses
COLUMNS TO UPDATE <column>      -- names a source column holding array<string>
```

The docs give the same `columnList` nonterminal for `COLUMNS` and
`TRACK HISTORY ON`, which is what makes the asymmetry surprising. See the
worked failure in §13.

The inline single-statement form (`CREATE ... FLOW AUTO CDC ...`) exists, but it
is **Beta and requires the Preview channel**, so it is avoided. This matters
because the deliverable has to run on Free Edition on the stable channel.

### Entity choices

| entity | SCD | target | why |
|---|---|---|---|
| `customers` | 2 | `silver.customers` | addresses genuinely change; history is the point |
| `products` | 2 | `silver.products` | recategorisation must be replayable |
| `sellers` | 2 | `silver.sellers` | `TRACK HISTORY ON seller_city, seller_state` -- only a *relocation* opens a version, a zip correction does not |
| `orders` | 1 | `silver.orders` | a single current row per order |
| (same source) | 2 | `silver.orders_status_history` | the delivery timeline: `COLUMNS (order_id, order_status)`, `TRACK HISTORY ON order_status` |
| `order_items` | 1 | `silver.order_items` | composite key; corrections are overwrites |
| `order_payments` | 1 | `silver.order_payments` | composite key; reversals delete |
| `order_reviews` | 1 | `silver.order_reviews` | `IGNORE NULL UPDATES` + `APPLY AS TRUNCATE` |

Two flows over the `orders` change stream (`orders` and `orders_status_history`)
is the cleanest illustration of the model: one source, two shapes, no duplicated
ingestion.

### The two clauses on `order_reviews` are doing real work

* `IGNORE NULL UPDATES` -- a review edit carries only `review_score` and the
  identifying columns. Without the clause, the null title/message on that record
  would blank the customer's original comment. The cost is that a genuine
  "delete this comment" (an explicit null that *should* overwrite) can no longer
  be expressed; the documented alternative is `COLUMNS TO UPDATE`.
* `APPLY AS TRUNCATE WHEN _op = 't'` -- the producer's `t` means "table truncated,
  re-delivering from scratch". The generator times that marker at the **start** of
  the stream (`2016-08-01`) precisely so the reload that follows is applied on top
  of an empty table. Timed at the end, it would wipe the table -- correct
  behaviour, terrible demo. SCD1 only; SCD2 has no truncate.

### The metadata columns are stripped by `COLUMNS`

`COLUMNS * EXCEPT (_op, _event_ts, _lsn, _source_entity, _source_table,
_source_snapshot, _source_file, _ingested_at, _rescued_data)` leaves silver with
business columns and nothing else. The `__START_AT` / `__END_AT` columns are
added by AUTO CDC itself.

---

## 5. Sequencing: business time, not arrival time

This is the demo's centrepiece.

```sql
SEQUENCE BY _event_ts                  -- SCD2
SEQUENCE BY STRUCT(_event_ts, _lsn)    -- SCD1
```

Sequence order is applied **per key**. Under `SEQUENCE BY _event_ts`:

* a late-arriving event is slotted into the correct **historical** position;
* an out-of-order event cannot clobber a newer value, no matter which file
  delivered it last;
* an SCD2 backfill closes its versions with the genuine 2017/2018 dates rather
  than the wall-clock time of the pipeline run.

SCD1 uses `STRUCT(_event_ts, _lsn)` because it is a total order, not just a
per-key order. A tie on business time can then never make the result depend on
which micro-batch happened to arrive first -- the log position breaks the tie
deterministically.

`notebooks/10_medallion_walkthrough.py` finds the orders where "latest by
business time" and "latest by arrival" disagree, and asserts that silver agreed
with the former. `notebooks/30_reprocess_and_late_data.py` manufactures such a
pair by hand.

**Wrong answer to avoid:** thinking `SEQUENCE BY` is about ingestion order. It is
not. It defines the *truth order* of the stream, and it is the reason no cleanup
code is needed anywhere else.

---

## 6. Schema drift, and the one thing you must not do

`tools/generate_cdc.py` injects two kinds of drift:

* a **new column** mid-stream: `orders.delivery_sla_breach` (wave 3),
  `products.product_launch_date` (wave 4);
* a **rename**, disguised as a new column: `products.product_name_lenght` stops
  being populated and `product_name_length` starts (wave 4). The source really
  does ship the misspelling, so "fixing" it looks exactly like
  "old column goes null, new column appears" -- there is no rename metadata in a
  gzipped file of JSON.

The conform step therefore selects `after.* EXCEPT (the columns it must rewrite)`
rather than an explicit column list. A brand new source column is simply
included.

> **The rule: never name a column the source has not sent yet.**
>
> An explicit reference to `orders.delivery_sla_breach` anywhere in bronze, silver
> or gold is an *analysis error* on the wave-1 run, because the column does not
> exist yet. This is why the two drifted columns appear in **no** pipeline query.
> They arrive in silver (via `after.*` and merge-schema evolution) and stop there.

Consequence, stated honestly: the drifted columns arrive with whatever Auto Loader
inferred -- `BOOLEAN` for the flag, `STRING` for the date, because JSON has no
date type. Gold does not depend on them. Instead gold **derives** the SLA breach
from the base timestamps (`fact_orders.is_late`), and notebook 10 reconciles our
derivation against the source's own flag.

### The Auto Loader "fail once" behaviour

`addNewColumns` is not magic. The first time Auto Loader sees a genuinely new
column, the schema it inferred no longer matches the data and the update **fails
once** with `UnknownFieldException`; the rescanned schema is persisted, and the
next run succeeds. This is authentic behaviour and is called out here so that a
failure at wave 3/4 is recognisable as "this is the drift arriving", not "the
pipeline is broken". A `bundle run` after the failed one is all it takes.

---

## 7. Quarantine: making "dropped" visible

`ON VIOLATION DROP ROW` is how you keep a pipeline alive. But "dropped" is
invisible, and invisible data loss is how you find out six months later.

So the pipeline **drops the row and keeps a copy**:

```sql
CREATE OR REFRESH STREAMING TABLE <entity>_cdc (
  CONSTRAINT <entity>_valid_row
  EXPECT ( <predicate> )
  ON VIOLATION DROP ROW
) COMMENT '...' TBLPROPERTIES (...) AS SELECT ...;
```

and the quarantine table is the *exact complement*:

```sql
CREATE OR REFRESH STREAMING TABLE <entity>_quarantine AS
SELECT ..., <reason> AS _quarantine_reason
FROM STREAM(<entity>_raw)
WHERE NOT ( <predicate> );
```

Design points:

* **Clause order matters.** `CREATE OR REFRESH STREAMING TABLE name (CONSTRAINT
  ... EXPECT ... ON VIOLATION DROP ROW) COMMENT ... TBLPROPERTIES ... AS SELECT
  ...`. The name, then the table specification, then `COMMENT`, then
  `TBLPROPERTIES`, then `AS`.
* **Expectations see the table's output columns, not `after`.** `expectation_expr`
  is "column identifiers within the table", so the predicate refers to
  `_op`, `customer_id`, `_event_ts` -- never to the raw `after` struct.
* **`_reason` is computed, not inferred.** Three planted modes plus the rescued
  path: `null_key`, `invalid_op`, `missing_event_ts`, `rescued_data`.
* **Payloads go out as JSON text.** `to_json(before)` / `to_json(after)` rather
  than the structs themselves, because a quarantined struct is itself subject to
  schema drift, and a quarantine table that keeps changing shape is a bad
  quarantine table.
* **`order_reviews` allows a null key, but only for `t`.** A table-level truncate
  has no row key by definition, so its predicate is
  `_op IN ('c','u','d','t') AND (_op = 't' OR (review_id IS NOT NULL AND _event_ts IS NOT NULL))`.

Notebook 40 reconciles `COUNT(*) FROM <entity>_quarantine` against the pipeline's
`failed_records` expectation metric from the event log. They must be equal. If
they ever diverge, the two predicates have drifted apart -- the single most likely
way for this design to rot, and the reason both are annotated in the source.

---

## 8. Gold: change feeds and the star schema

Gold is two things, because they answer different questions.

### `<entity>_changes` -- the Delta change feed, as a table

`04_gold_change_feeds.py`. Reads silver with `readChangeFeed = true` and
materialises the feed as an append-only streaming table. Every row is a silver row
plus `_change_type` (`insert`/`update_preimage`/`update_postimage`/`delete`),
`_commit_version`, `_commit_timestamp`, and three convenience booleans
(`_cdc_is_insert`/`_cdc_is_update`/`_cdc_is_delete`).

Why a change feed and not a view: a change feed is append-only from the reader's
point of view -- each update sees only the changes committed since the last -- and
retains history even after the silver table moves on. That is what an audit trail,
a reverse-ETL sync or a downstream subscriber actually needs.

### Star schema + marts -- materialized views

`05_gold_marts.sql`. `dim_geolocation`, `dim_customer`, `dim_customer_history`,
`dim_product`, `dim_seller`, `fact_orders`, `fact_order_items`,
`mart_delivery_sla`, `mart_category_revenue`, `customer_360`.

Why materialized views and not streaming tables:

* an AUTO CDC target is **not a valid input to a streaming table** -- it updates
  rows, it does not append them;
* Databricks is explicit that these transformations "are better suited to
  materialized views".

So gold is batch-shaped on purpose: each pipeline update recomputes it from the
current state of silver.

Reading current state from an SCD2 target:

```sql
WHERE __END_AT IS NULL
  AND <key> IS NOT NULL
```

The first predicate enforces deletion: an SCD2 delete closes the open version and
opens nothing, so an erased customer falls out of every gold mart for free. The
second is belt-and-braces against an internal tombstone surviving into a
dimension. **SCD1 targets have no `__START_AT`/`__END_AT`** -- `silver.orders` is
already current state, so there is nothing to filter.

Two more things gold gets right on purpose:

* **`dim_geolocation` reads with `inferSchema => false`.** With inference on,
  Spark sees zip prefix `"01001"`, types it as `INT`, drops the leading zero and
  silently breaks the join. Read as text, cast deliberately.
* **`customer_360` rolls up by `customer_unique_id`, not `customer_id`.** In
  Olist, `customer_id` is minted per order; `customer_unique_id` is the human.
  Rolling up by `customer_id` reports 1.0 orders per customer and looks broken.

---

## 9. The SCD2 change-feed subtlety

On an SCD2 target the primary key is the business key **plus**
`coalesce(__START_AT, __END_AT)`.

When a version is slotted into the past -- a late arrival whose business time
falls before an already-written version, which is *normal* here -- the
`__START_AT` of the following version moves, which changes that row's identity.
In the change feed those show up as **`insert`/`delete` pairs** rather than
`update_preimage`/`update_postimage`.

That is not a bug. It is the visible cost of correcting history, and it is why
the gold change feeds are append-only: the feed is the truth about what happened,
even while the table it came from is being reordered underneath it. Notebook 20
walks through it.

---

## 10. Unity Catalog vs the Hive metastore

A widely-quoted caveat says that when the AUTO CDC target is a **view**, the real
storage lives in a separate `__apply_changes_storage_<target>` table, and the
change feed must be read from that.

That is the **Hive metastore** behaviour. On **Unity Catalog** an AUTO CDC target
is a real streaming table, so `readChangeFeed` on the target itself works, from
DBR 15.2, provided the table is UC-published. This pipeline is UC-only, so
`04_gold_change_feeds.py` reads the targets directly and there is no
`__apply_changes_storage_` indirection to reason about.

---

## 11. Deliberate non-choices (extensions)

Documented so a reader knows they were considered and left out for a reason, not
missed:

| feature | why not used |
|---|---|
| inline `CREATE ... FLOW AUTO CDC INTO` (single statement) | Beta, requires the Preview channel |
| bitemporal AUTO CDC | beyond the demo's scope; a genuine extension worth building |
| `COLUMNS TO UPDATE` on `order_reviews` | `IGNORE NULL UPDATES` is the simpler fit here; `COLUMNS TO UPDATE` is the escape hatch when explicit-null writes matter |
| `allowSourceColumnRename` | the drift is deliberately presented as "new column appears", which is what a JSON source actually looks like |
| materialized-view change feeds | Beta, Preview channel, DBR 18 LTS, and must be read from a *different* pipeline |

The guiding constraint for the whole repo: **prefer non-Beta, GA features.** The
deliverable is a repo, and its correctness rests on documented behaviour.

---

## 12. Free Edition constraints, and what they forced

| constraint | consequence |
|---|---|
| serverless-only | pipeline is `serverless: true`; no cluster config anywhere |
| one ETL pipeline | the entire medallion is one pipeline; the dependency graph is expressed by the code, and one `bundle run` refreshes the lot in order |
| no DBFS root | the landing zone is a **UC volume** (`/Volumes/workspace/retail_landing/cdc`) |
| restricted egress (Kaggle unreachable) | data is downloaded and the CDC stream generated **locally**, then uploaded |
| no Beta/Preview | drives §11 |
| one writable catalog (`workspace`), and no folders in Unity Catalog | the five schemas share it with `default` and `information_schema`; the `retail_*` prefix plus `project`/`layer` tags keep them findable |

Tags are not declarable in the bundle: a `schemas` resource supports `comment`
and `properties`, but Unity Catalog tags go through a separate
`entity-tag-assignments` API. Hence the small post-deploy script,
`tools/tag_schemas.py`.

There is an asymmetry that makes tags only half a solution here: **schemas are
not tag-searchable.** Tag search -- the workspace search bar, with
`tag:<key>[:<value>]` -- covers tables, views, volumes, models, functions,
dashboards and notebooks, and explicitly excludes catalogs, schemas and columns;
Catalog Explorer's filter box matches names, not tags. So on a schema the tag is
metadata, readable on its Overview page and from
`information_schema.schema_tags`, and the `retail_*` prefix stays the thing that
makes the schema list navigable. The script tags tables and volumes too, which is
where tag search actually pays off.

The `configuration` block in `resources/pipeline.yml` is the single source of
truth for naming. Python reads it with `spark.conf.get("retail_silver")`; SQL
reads it with `${retail_silver}`. Both sides reference the same bundle variables,
so the names cannot drift.

---

## 13. What to watch if you change something

A short list of the invariants this design depends on, and where each one lives.
Break one and the demo breaks in a way that is not always obvious.

1. **The conform predicate and the quarantine predicate are complements.**
   Change one, change the other. Annotated in `02_bronze_conform.sql`; verified by
   notebook 40.
2. **No query names a column the source has not sent.** Adding
   `delivery_sla_breach` to a gold query takes the pipeline down until wave 3.
3. **`SEQUENCE BY` stays business time.** Switching to ingestion time silently
   destroys the out-of-order story and fabricates 2026 dates in SCD2 intervals.
4. **Change feeds read streaming tables, not materialized views.** Adding an MV
   source to `04_gold_change_feeds.py` will not work in-pipeline.
5. **Chaos injection runs after drift application.** A redelivered record must be
   byte-identical or replay is not idempotent. (`generate_cdc.py`, `main()`.)
6. **The generator is deterministic.** It uses `zlib.crc32`, not `hash()`, and
   `_monotonic` guarantees per-key strictly-increasing `event_ts` (skipped for
   null-key rows). Change either and the seeded stream changes.
7. **`COLUMNS (...)` is parenthesised; `TRACK HISTORY ON ...` is not.** They look
   like the same clause and are documented with the same `columnList`
   nonterminal, but `TRACK HISTORY ON (a, b)` is a parse error. Because a
   pipeline analyses the whole graph before executing, one stray pair of
   parentheses stops every table in the pipeline from being created -- the
   update fails in seconds with `[PARSE_SYNTAX_ERROR] ... 'Syntax error at or
   near ('` and nothing is built, so the failure looks bigger than the bug. The
   first wave of this demo failed exactly this way; the clause shape is pinned
   in a comment on `silver.sellers` in `03_silver_auto_cdc.sql`.
