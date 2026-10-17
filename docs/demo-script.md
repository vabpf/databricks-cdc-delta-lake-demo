# Demo script

A presenter's walkthrough. Each act lists the **commands**, what to **say**, and
what should be on screen. Timings are for a ~30 minute run; everything after
Act 3 can be dropped or reordered without losing the story.

The one-sentence thesis to keep returning to:

> The pipeline is handed dirty data on purpose and stays correct **by
> configuration**, not by cleanup code.

---

## Before the room is in the room (~15 min, once per environment)

```bash
# local, one-time
python tools/download_olist.py
python tools/generate_cdc.py --out data/landing
python tools/verify_stream.py --landing data/landing   # expect: RESULT: PASS

# workspace
databricks bundle validate
databricks bundle deploy
```

Leave the environment deployed with **no data loaded**. That way Act 1 is a real
first upload rather than a pre-baked screenshot.

If you want a warm environment instead, run Act 2's wave 1 beforehand and start at
Act 3.

---

## Act 1 — the landing zone (3 min)

```bash
python tools/upload_landing.py --wave 1
```

**Say:** "This is a CDC stream. Every file is a *delivery*, and the wave number is
in the filename. We're going to ingest it the way you'd ingest a real feed: one
delivery at a time, incrementally."

Open `notebooks/01_explore_landing_cdc.py` and run it.

Point at three things:

1. **The envelope.** `op`, `before`, `after`, `event_ts`, `lsn`. "`after` is null
   on delete, `before` is null on create. The stream is self-describing."
2. **The planted garbage.** null keys, `op = 'x'`, truncated JSON. "These are not
   accidents. They're what we're going to account for."
3. **The drift.** Wave 1 `products` has `product_name_lenght` (misspelled) and no
   `product_launch_date`. Wave 5 has both, spelled both ways. "A rename in a JSON
   feed looks exactly like 'old column goes null, new column appears'. There is no
   rename metadata."

Note: nothing has been loaded yet. That is deliberate.

---

## Act 2 — bronze: raw in, conformed out, nothing lost (5 min)

```bash
databricks bundle run retail_medallion_cdc
```

Open `notebooks/10_medallion_walkthrough.py` and run it through section 1.

**Say:** "Bronze is two layers. `*_raw` is the envelope untouched -- if a rule is
wrong, the original bytes are still here. `*_cdc` is the same rows unpacked."

Point at the split:

```
orders_raw        : N
orders_cdc        : N - k
orders_quarantine : k
accounted for     : N / N
```

**Say:** "`raw == conformed + quarantine`, always. A record is never dropped by
the conform step without being written down somewhere. `ON VIOLATION DROP ROW`
keeps the pipeline alive; the quarantine table makes 'dropped' visible."

Show the quarantine reasons: `null_key`, `invalid_op`, `rescued_data`.

---

## Act 3 — silver: the proof (6 min)

Still in notebook 10, sections 2–3.

**Say:** "`silver.orders` is SCD1 -- one row per order, current state. SCD1 means
last-write-wins, right? No. It means *newest by business time* wins."

Show the contested-orders query: orders where "latest by business time" and
"latest by arrival" disagree.

**Say:** "The generator delivered ~6% of events a wave or two late. Under a naive
implementation those late events would clobber newer values. Watch the check."

Expect:

```
[PASS] silver resolved all <n> contested orders by business time, never by arrival
```

**The line to land:** "There is no `dropDuplicates`. There is no sort. There is no
lateness handling anywhere in the pipeline. There is one config line --
`SEQUENCE BY _event_ts` -- and everything else falls out of it."

---

## Act 4 — silver: history, with real dates (4 min)

Notebook 10, section 4.

Show `silver.customers` for a customer with more than one version: address moves
produce `__START_AT` / `__END_AT` intervals with **2017/2018 dates**.

**Say:** "If we sequenced by ingestion time, every version would start today and
the whole history would be a stack of identical timestamps. Because we sequence by
business time, an SCD2 backfill reconstructs the real timeline -- and a
late-arriving move gets slotted into the *right* place, not appended."

Show `silver.orders_status_history` -- the delivery lifecycle as an SCD2 timeline,
read from the *same* bronze table as `silver.orders`. "Two shapes, one source, no
duplicated ingestion."

---

## Act 5 — schema drift arrives by itself (3 min)

Notebook 10, section 5.

**Say:** "Nothing in the pipeline names these columns. Not one line. They flowed
through because the conform step selects `after.* EXCEPT (what it must rewrite)`,
so a new source column is simply included."

Point at the types: `product_launch_date` is a **STRING**, not a date. "JSON has
no date type. That's the honest outcome, and it's why gold doesn't depend on it."

Then the failure to *expect*, if you want to show it: the first run that sees a
new column fails once with `UnknownFieldException`; the next run succeeds. "That's
Auto Loader's schema evolution. Recognise it; re-run and it's gone."

---

## Act 6 — gold: star schema and change feeds (5 min)

Notebook 10, section 6.

**Say:** "Gold is materialized views, because an AUTO CDC target isn't a valid
input to a streaming table, and aggregations belong in MVs. Current state from an
SCD2 target is `WHERE __END_AT IS NULL` -- and that predicate also enforces
deletion, because an SCD2 delete closes the version and opens nothing."

Show `mart_category_revenue`, `fact_orders`, then the SLA reconciliation:
our derived flag vs the source's drifted `delivery_sla_breach`. "Whenever a
producer ships a derived column, reconcile it against your own derivation. Off-
diagonal rows are the interesting ones."

Open `notebooks/20_time_travel_and_cdf.py` for the change feeds.

**Say:** "Every row remembers which commit it changed in. An update is two rows --
pre-image and post-image. That's what makes the feed replayable, and it's what a
reverse-ETL sync subscribes to."

---

## Act 7 — late data, manufactured by hand (5 min)

Open `notebooks/30_reprocess_and_late_data.py`.

**Say:** "I want to prove the late-data claim rather than assert it, so let's write
the hard case ourselves. Two events, one order, one file -- in the *wrong order*."

Show the file: line 1 is `delivered` (business time T+1s), line 2 is `shipped`
(business time T−1 day). The event that arrived **last** is the *older* one.

Run the notebook (it triggers the pipeline itself). Expect:

```
[PASS] late/out-of-order: the older 'shipped' event did NOT clobber the newer 'delivered' state
```

**Say:** "Arrival order said `shipped`. Business order said `delivered`. Silver
says `delivered`. That's the whole demo in one row."

Then the `TRACK HISTORY ON` section: a seller zip-only change updates in place
with **no new version**, because history is tracked on `(seller_city,
seller_state)` only. "Same table, same kind of event, different outcome -- and
it's a config line."

---

## Act 8 — time travel and the right to be forgotten (4 min)

Back to `notebooks/20_time_travel_and_cdf.py`, section 5.

**Say:** "SCD2 keeps history. GDPR says delete. Those are in tension, so let's be
precise about how the layering resolves it."

Show that the erased customers are **gone from `dim_customer`** (consumption) but
**still in `silver.customers`** (audit trail), all versions closed.

**Say:** "Current state is for consumption. History is for the regulator. Two
retention policies, two tables. If you need the bytes gone, that's `DELETE` plus
`VACUUM` across *every* table including the feeds -- and the raw events are still
in the volume. 'Just delete it' isn't enough."

---

## Act 9 — the data-quality scorecard (3 min)

Open `notebooks/40_ops_event_log_dq.py`.

**Say:** "The pipeline publishes its own operations data as a queryable table:
`event_log('<pipeline-id>')`."

Show:

* update history (`update_progress`);
* per-flow throughput (`flow_progress`);
* expectation metrics, exploded out of the VARIANT `details` column;
* the reconciliation: `failed_records` from the event log vs row counts in the
  quarantine tables. **They must be equal.** "If they ever diverge, the conform and
  quarantine predicates have drifted apart -- the one way this design rots."

End on the scorecard: per entity, `raw == conformed + quarantined`.

---

## Closing (1 min)

**Say:** "Six things went in dirty: out-of-order, late, duplicated, deleted,
truncated and malformed -- plus drift. None of it needed special-case code. The
whole thing is one pipeline, one `bundle run`, and the correctness lives in three
places: a sequence column, a set of expectations whose complement is a quarantine
table, and the discipline not to name a column that hasn't arrived yet."

```bash
notebooks/99_reset_demo.py     # full refresh, or clean slate + re-upload
databricks bundle destroy      # remove everything the bundle declared
```

---

## Handling the awkward questions

**"Why not just `dropDuplicates`?"**
A change needs an identity, and the sequence column already provides one. Dedup
would have to invent a key, and it can't tell a replay from a genuine repeat of the
same status.

**"Why is gold batch-shaped?"**
An AUTO CDC target updates rows; a streaming table expects appends. Databricks'
own guidance is that these aggregations belong in materialized views.

**"Why does the first drift run fail?"**
It doesn't fail the *data* -- it fails the update once while Auto Loader rescans
the schema. Re-run and it completes. That is the documented behaviour of
`addNewColumns`.

**"Why two tables for `orders`?"**
One change stream, two questions: "what is this order now?" (SCD1) and "what was
its lifecycle?" (SCD2). Same bronze source; zero duplicated ingestion.

**"Where would you put a unit test?"**
There is no test framework for a repo-only deliverable, but the notebooks end in
assertions (`N/N checks passed`) and `tools/verify_stream.py` asserts the landing
zone's invariants locally. That is the test surface.
