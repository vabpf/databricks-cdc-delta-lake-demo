# Databricks CDC / Medallion demo

A complete, runnable change-data-capture pipeline on Databricks, built on the
[Olist Brazilian E-Commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
dataset and sized to fit inside **Databricks Free Edition**.

It is a medallion architecture (`bronze → silver → gold`) where the source is a
**Debezium-shaped change stream**, not a batch extract:

```
Kaggle CSVs ─► tools/generate_cdc.py ─► Debezium events (*.jsonl.gz)
                                              │
                                    UC volume (landing zone)
                                              │
                              ┌───────────────▼───────────────┐
                              │  Lakeflow pipeline            │
                              │  (one pipeline, five files)   │
                              └───────────────┬───────────────┘
                                              │
   BRONZE   Auto Loader ─► <entity>_raw  ─►  conform + quarantine  ─► <entity>_cdc
                                              │                     └► <entity>_quarantine
                                              │
   SILVER   AUTO CDC   ─► SCD1 current state + SCD2 history
                                              │
   GOLD     Delta CDF   ─► <entity>_changes   (append-only change feeds)
            Materialized ─► star schema + marts (dim_*, fact_*, mart_*, customer_360)
```

The point of the demo is not the dashboards. It is that the pipeline is **handed
dirty data on purpose** -- out-of-order arrivals, late arrivals, replays,
deletes, a table truncate, malformed JSON and mid-stream schema drift -- and that
it stays correct *by configuration* rather than by cleanup code bolted on
afterwards.

## What is in here

| path | what it is |
|---|---|
| `tools/download_olist.py` | fetch the Kaggle CSVs (kagglehub) |
| `tools/generate_cdc.py` | reconstruct the true Olist lifecycle and emit a Debezium-shaped change stream, with chaos injected deterministically |
| `tools/upload_landing.py` | push the landing zone into a Unity Catalog volume, **wave by wave** |
| `tools/verify_stream.py` | assert the generated stream's invariants hold (run locally) |
| `tools/validate_notebooks.py` | check the notebooks are well-formed (run locally; no workspace needed) |
| `tools/tag_schemas.py` | label the demo schemas and volumes with `project`/`layer` tags so they are findable |
| `src/pipeline/` | the Lakeflow pipeline, five files |
| `resources/` | bundle resources: 5 schemas, 1 volume, the pipeline, a paused job |
| `notebooks/` | guided tour + four focused notebooks |
| `docs/` | architecture notes and a demo script |

## The source: why a synthetic CDC stream

Kaggle gives you **snapshots**: one row per order, with the lifecycle baked into
four timestamp columns and a status. A CDC demo needs **events**. So
`tools/generate_cdc.py` reconstructs the lifecycle from the snapshots and emits
one event per transition:

```
created ─► approved ─► shipped/invoiced ─► delivered
                   └──► canceled | unavailable
```

Each event is a real Debezium envelope -- `op` (`c`/`u`/`d`/`r`/`t`/`x`),
`before`/`after` row images, `event_ts` (business time), `ts_ms`, `lsn`, and
`source` metadata -- so the pipeline is written against the shape a real
connector would produce, not a shape invented for the demo.

**Business time is the real 2017/2018 date of the change.** That is the single
most important property of the generated stream: it is what makes SCD2 produce
genuine historical intervals and what makes late arrivals land in the right place.

Chaos, all deterministic (seeded) and all *inside genuine envelopes*:

| injection | what it exercises |
|---|---|
| arrivals shuffled within a wave | `SEQUENCE BY` business time, not arrival time |
| ~6% of events delivered 1–2 waves late | late-arriving data |
| ~3% of events redelivered byte-identical | at-least-once idempotency |
| null keys, unknown `op` codes | dropped-row expectations + quarantine |
| truncated JSON lines | Auto Loader `rescuedDataColumn` |
| a `t` (truncate) event | `APPLY AS TRUNCATE` |
| GDPR `d` events on customers | right-to-be-forgotten through SCD2 |
| new columns at wave 3/4, and a column **rename** | Auto Loader schema evolution |

At the default settings (seed 42) that is **6 waves, 126 files, ~227,000 events**,
including 122 deliberately broken lines.

## Quick start

### 0. Prerequisites

* A Databricks workspace (the demo targets **Free Edition**), and the
  [Databricks CLI](https://docs.databricks.com/aws/en/dev-tools/cli/) v0.230+
  authenticated (`databricks auth login`).
* Optional: the [Databricks VS Code extension](https://docs.databricks.com/aws/en/dev-tools/vscode-ext/).
  Its bundle view reports *"Invalid host for target dev"* unless the target
  declares `workspace.host`; the CLI does not need it. The `dev` target ships
  one, so if you point this bundle at another workspace, edit that line. See the
  note in [`databricks.yml`](databricks.yml).
* Python 3.10+ locally: `pip install -r requirements.txt`.
* Kaggle credentials for the one-time download. Create a Kaggle API token
  (Kaggle → Account → *Create New API Token*) and put it at
  `~/.kaggle/kaggle.json`, or set `KAGGLE_USERNAME` / `KAGGLE_KEY`.

### 1. Generate the landing zone (local)

```bash
python tools/download_olist.py                       # ► data/olist/
python tools/generate_cdc.py --out data/landing      # ► data/landing/
python tools/verify_stream.py --landing data/landing   # sanity check
```

`generate_cdc.py` defaults to the Olist window `2017-01-01`–`2018-07-01` and
takes roughly two minutes. It is seeded, so you get the same stream every time.

### 2. Deploy the bundle

```bash
databricks bundle validate     # resolves everything, changes nothing
databricks bundle deploy
```

This creates the five schemas and the landing volume declared in `resources/`.
It does **not** upload data and does **not** start the pipeline.

Then label them:

```bash
python tools/tag_schemas.py
```

Free Edition gives you a single writable catalog (`workspace`) and Unity Catalog
has no folders, so the five schemas share that catalog with `default` and
`information_schema`. The `retail_*` prefix is what narrows them down in Catalog
Explorer's filter box. The tags the script applies are read on an object's
Overview page, and in SQL:

```sql
SELECT * FROM workspace.information_schema.schema_tags;
```

One documented limitation is worth knowing before you go looking for them:
**schemas cannot be tag-searched.** Databricks' tag search covers tables, views,
volumes, models, functions, dashboards and notebooks -- catalogs, schemas and
columns are explicitly excluded -- and Catalog Explorer's filter box matches
names only, not tags. The searchable payoff is therefore the objects *inside* the
schemas, which the script tags as well. In the workspace search bar (Cmd-P):

```
tag:project:retail-cdc-demo
```

The bundle cannot set these tags itself (an Asset Bundle `Schema` resource has no
`tags` field), which is why it is a separate step. Re-run it after the pipeline
has built tables, to pick up the new ones.

### 3. Run it, wave by wave

This is the part that makes the demo a *story*. Upload one wave, run the
pipeline, upload the next, run again:

```bash
python tools/upload_landing.py --wave 1
databricks bundle run retail_medallion_cdc --validate-only   # dry run: checks the graph, builds nothing
databricks bundle run retail_medallion_cdc

python tools/upload_landing.py --wave 2
databricks bundle run retail_medallion_cdc

# ... through wave 6, or `python tools/upload_landing.py --all` to do the lot
```

`--validate-only` analyses the whole pipeline graph and resolves every dataset
without materialising any data, so it reports parse and analysis errors in
seconds at no compute cost. Every hard failure this repo hit while being built
was an analysis error, so it is worth doing before spending a real update --
and a validate update never retries. Skip it between waves: once a wave has
built cleanly, the schema is known and the update is doing real work.

Each run only ingests the **new** files: Auto Loader tracks processed files
transactionally. Schema drift does not appear until wave 3 (`orders`) and
wave 4 (`products`), so running all six waves in one shot is fine but runs the
drift past you.

Two traps worth knowing before you start editing pipeline code:

* **`bundle run` does not sync your working tree.** It re-runs whatever the last
  `bundle deploy` uploaded. Change anything under `src/pipeline/` and you must
  `databricks bundle deploy` again first, or you will re-run the old code and
  debug a fix that was never applied.
* **The platform retries a failed update on its own** (`cause: RETRY_ON_FAILURE`,
  backing off about 25s / 45s / 85s / 166s). An API- or job-triggered update
  uses automatic retry-and-restart, which defaults to **five update retries plus
  two per flow**, so one bug looks like several, quota burns, and an ad-hoc run
  in the meantime is rejected with *"An active update already exists for
  pipeline"*. This bundle pins both bounds to zero for the iteration loop
  (`pipelines.numUpdateRetryAttempts` and `pipelines.maxFlowRetryAttempts` in
  `resources/pipeline.yml`); drop those two lines for a scheduled pipeline. If a
  retry loop is ever live, stop it before intervening:

  ```bash
  databricks pipelines stop <pipeline-id>
  ```

Then take the tour:

```
notebooks/00_setup_workspace.py         # verify the environment, see the waves
notebooks/01_explore_landing_cdc.py     # look at the raw stream first
notebooks/10_medallion_walkthrough.py   # the flagship tour, with PASS/FAIL checks
notebooks/20_time_travel_and_cdf.py     # histories, change feeds, GDPR
notebooks/30_reprocess_and_late_data.py # hand-crafted late data, replays, refresh
notebooks/40_ops_event_log_dq.py        # event log, expectations, quarantine
notebooks/99_reset_demo.py              # full refresh / clean slate
```

## The notebooks

| notebook | what it shows |
|---|---|
| `00_setup_workspace` | read-only check: schemas exist, how many waves have arrived, pipeline state |
| `01_explore_landing_cdc` | read the raw files directly: the envelope, the planted garbage, the drift |
| `10_medallion_walkthrough` | the guided tour, ending in a set of assertions (`N/N checks passed`) |
| `20_time_travel_and_cdf` | Delta time travel, `table_changes()`, the gold change feeds, and a worked GDPR erasure |
| `30_reprocess_and_late_data` | manufactures late/out-of-order events by hand, proves replay idempotency, and shows `TRACK HISTORY ON` and full-refresh idempotency |
| `40_ops_event_log_dq` | `event_log()` over the pipeline, expectation metrics reconciled against the quarantine tables |
| `99_reset_demo` | full refresh and clean-slate teardown, both confirmation-gated |

Notebooks 10–40 end with `N/N checks passed` and raise if any check fails, so
they double as smoke tests.

## Design notes worth knowing before you read the pipeline

These are the decisions that shaped the code. Each is explained where it lives,
but they are also the things that will trip you up if you only skim.

* **No column is named that the source has not sent yet.** `after.* EXCEPT (...)`
  rather than an explicit column list is what lets a new source column flow from
  landing to silver untouched. The two drifted columns (`orders.delivery_sla_breach`,
  `products.product_launch_date`) are therefore *not* referenced anywhere upstream of
  gold -- naming one would be an analysis error on the wave-1 run.

* **`SEQUENCE BY` is business time.** `SEQUENCE BY _event_ts` for SCD2 and
  `STRUCT(_event_ts, _lsn)` for SCD1. This is the whole reason the demo behaves:
  the out-of-order and late arrivals resolve themselves.

* **Two declarations per AUTO CDC target.** `AUTO CDC` never creates its target,
  so each entity is a `CREATE OR REFRESH STREAMING TABLE` *and* a
  `CREATE FLOW ... AS AUTO CDC INTO`. The inline `FLOW AUTO CDC` form is Beta and
  needs the Preview channel, so it is avoided.

* **Quarantine is the exact complement of the conform predicate.** A row lands in
  `_cdc` or `_quarantine`, never both, never neither. The two predicates are in
  `02_bronze_conform.sql` and annotated "MUST stay the exact complement"; notebook
  40 reconciles the row counts against the pipeline's own expectation metrics.

* **Gold is materialized views.** An AUTO CDC target is not a valid input to a
  streaming table, and aggregations are "better suited to materialized views".
  Current state from an SCD2 target is `WHERE __END_AT IS NULL`.

* **Change feeds read only silver *streaming* tables.** Materialized-view CDF is
  Beta, needs the Preview channel and DBR 18 LTS, and cannot be read from the same
  pipeline. Every source in `04_gold_change_feeds.py` is a streaming table, where
  an in-pipeline CDF read is supported on Unity Catalog from DBR 15.2.

Full detail, including the trap around streaming-table primary keys and the
tombstone behaviour that is *not* what the Hive-metastore docs describe, is in
[`docs/architecture.md`](docs/architecture.md).

## Free Edition constraints

The demo is deliberately shaped by what Free Edition allows, and each constraint
turned out to be a useful design pressure:

| constraint | how the demo handles it |
|---|---|
| serverless-only compute | the pipeline is `serverless: true`; nothing needs a cluster policy |
| one ETL pipeline | the whole medallion -- all layers -- is one pipeline |
| no DBFS root | the landing zone is a **Unity Catalog volume** |
| restricted outbound egress (Kaggle unreachable) | data is downloaded locally and the CDC stream is generated locally, then uploaded |
| only one writable catalog (`workspace`), and no folders in Unity Catalog | the five schemas share it under a `retail_*` prefix and carry `project`/`layer` tags -- see `tools/tag_schemas.py` |
| no Beta / Preview features | inline `FLOW AUTO CDC`, bitemporal AUTO CDC and MV change feeds are documented as extensions instead |

## The scheduled job

`resources/job.yml` declares a paused daily refresh. It exists to show that the
same pipeline resource is schedulable and that "incremental" is the right shape
for a refresh -- each update picks up whatever files have landed since the last.
Nothing runs until you unpause it.

## Nothing is committed that can be regenerated

`data/olist/` and `data/landing/` are gitignored: both are rebuilt by the tools,
and the landing zone is 100+ files of gzipped JSON. The repo is the *recipe*.

## Teardown

```bash
databricks bundle destroy      # drops the schemas and the volume
```

Or, to keep the environment and just rebuild the tables, run
`notebooks/99_reset_demo.py`.

## A note on the numbers

`tools/generate_cdc.py` is seeded, so counts are reproducible, but they move if
you change the window, the scale or the probabilities. The notebook checks all
compare against the *data*, not against hardcoded counts, so they hold whatever
you generate.
