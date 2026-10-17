#!/usr/bin/env python3
"""
Olist (Kaggle)  ->  Debezium-style CDC event generator
=====================================================

Kaggle gives us *static snapshots*. Databricks CDC tooling (Auto Loader +
`AUTO CDC INTO`) wants a *change stream*. This script bridges the two: it reads
the raw Olist CSVs, reconstructs the business lifecycle that must have produced
them, and emits a realistic, intentionally-imperfect Debezium-shaped event log
as newline-delimited JSON.

Why synthetic CDC is legitimate here
------------------------------------
Olist is a snapshot of a transactional system. But the snapshot carries history
inside it: `order_status` is the *final* state, and the four timestamp columns
are the *transitions*. So we can replay the lifecycle deterministically:

    created -> approved -> shipped/invoiced -> delivered
    created -> approved -> canceled | unavailable

Every other table (items, payments, reviews) has its own natural timestamp too.
So the event stream is derived from real data, not invented.

What makes it "advanced"
------------------------
The stream is deliberately messy, because that is what production CDC looks
like. We inject, deterministically (seeded):

  * out-of-order arrival   - events are shuffled inside a wave, so arrival
                             order != business order
  * late-arriving events   - a slice of events is delivered 1..N waves late
  * at-least-once replays  - duplicates re-delivered in a later wave
  * hard deletes           - cancellations + GDPR-style erasure
  * truncates              - a `t` op for the TRUNCATE demo
  * schema drift           - a new column appears, and a column is RENAMED
  * partial updates        - updates that only carry the columns that changed
  * malformed records      - null keys / bad op / broken JSON for a DLQ

Output layout (uploaded verbatim into a Unity Catalog volume)
------------------------------------------------------------
    <out>/events/<entity>/<entity>_w<NN>_p<PPP>.jsonl[.gz]
    <out>/reference/geolocation_by_zip.csv
    <out>/_manifest.json

`w<NN>` is the delivery wave. Upload wave 1, run the pipeline, upload wave 2,
run again ... and you get genuine incremental ingestion with late data.

Usage
-----
    python tools/generate_cdc.py --olist-dir <dir> --out ./data/landing
    python tools/generate_cdc.py --help
"""

from __future__ import annotations

import argparse
import bisect
import gzip
import json
import logging
import random
import shutil
import zlib
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path

import pandas as pd

log = logging.getLogger("generate_cdc")

# --------------------------------------------------------------------------- #
# Entity specification
# --------------------------------------------------------------------------- #
# `key`        : primary key columns (drives AUTO CDC KEYS + quarantine checks)
# `columns`    : business column order in the emitted `after` image
# `scd`        : how the pipeline materialises it (documented, not used here)
@dataclass(frozen=True)
class Entity:
    name: str
    key: tuple[str, ...]
    columns: tuple[str, ...]
    scd: str
    op_initial: str  # "r" (snapshot read) or "c" (created in-flight)
    source_table: str = ""


ENTITIES: dict[str, Entity] = {
    "customers": Entity(
        name="customers",
        key=("customer_id",),
        columns=(
            "customer_id",
            "customer_unique_id",
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state",
        ),
        scd="SCD2",
        op_initial="r",
        source_table="olist.customers",
    ),
    "products": Entity(
        name="products",
        key=("product_id",),
        columns=(
            "product_id",
            "product_category_name",
            "product_category_name_english",
            # NOTE: `product_name_lenght` is the misspelling that ships in the
            # real Kaggle file. We keep it for waves 1..N-2 and then "fix" it by
            # emitting `product_name_length`, exercising Auto Loader schema drift.
            "product_name_lenght",
            "product_description_lenght",
            "product_photos_qty",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
        ),
        scd="SCD2",
        op_initial="r",
        source_table="olist.products",
    ),
    "sellers": Entity(
        name="sellers",
        key=("seller_id",),
        columns=(
            "seller_id",
            "seller_zip_code_prefix",
            "seller_city",
            "seller_state",
        ),
        scd="SCD1",
        op_initial="r",
        source_table="olist.sellers",
    ),
    "orders": Entity(
        name="orders",
        key=("order_id",),
        columns=(
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ),
        scd="SCD1 (+ SCD2 status history)",
        op_initial="c",
        source_table="olist.orders",
    ),
    "order_items": Entity(
        name="order_items",
        key=("order_id", "order_item_id"),
        columns=(
            "order_id",
            "order_item_id",
            "product_id",
            "seller_id",
            "shipping_limit_date",
            "price",
            "freight_value",
        ),
        scd="SCD1",
        op_initial="c",
        source_table="olist.order_items",
    ),
    "order_payments": Entity(
        name="order_payments",
        key=("order_id", "payment_sequential"),
        columns=(
            "order_id",
            "payment_sequential",
            "payment_type",
            "payment_installments",
            "payment_value",
        ),
        scd="SCD1",
        op_initial="c",
        source_table="olist.order_payments",
    ),
    "order_reviews": Entity(
        name="order_reviews",
        key=("review_id",),
        columns=(
            "review_id",
            "order_id",
            "review_score",
            "review_comment_title",
            "review_comment_message",
            "review_creation_date",
            "review_answer_timestamp",
        ),
        scd="SCD1 (IGNORE NULL UPDATES)",
        op_initial="c",
        source_table="olist.order_reviews",
    ),
}

# Column typing. Everything is read as text (no inference surprises, no lost
# leading zeros); these are the columns we coerce back to real types. Anything
# not listed stays a string -- which is the honest type for ids, enums, etc.
COERCE_INT: dict[str, tuple[str, ...]] = {
    "order_items": ("order_item_id",),
    "order_payments": ("payment_sequential", "payment_installments"),
    "order_reviews": ("review_score",),
    "products": (
        "product_name_lenght",
        "product_description_lenght",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
    ),
}
COERCE_DOUBLE: dict[str, tuple[str, ...]] = {
    "order_items": ("price", "freight_value"),
    "order_payments": ("payment_value",),
}
# zip codes must stay zero-padded strings so the geolocation join works
COERCE_ZIP5: dict[str, tuple[str, ...]] = {
    "customers": ("customer_zip_code_prefix",),
    "sellers": ("seller_zip_code_prefix",),
    "geolocation": ("geolocation_zip_code_prefix",),
}

# Columns added mid-stream to exercise Auto Loader addNewColumns evolution.
DRIFT_NEW_COLUMNS: dict[str, tuple[str, int]] = {
    # entity -> (column, first wave that carries it, 1-based)
    "orders": ("delivery_sla_breach", 3),
    "products": ("product_launch_date", 4),
}
# Column rename: entity -> (old, new, first wave emitting the new name)
DRIFT_RENAME: dict[str, tuple[str, str, int]] = {
    "products": ("product_name_lenght", "product_name_length", 4),
}


# --------------------------------------------------------------------------- #
# Small helpers
# --------------------------------------------------------------------------- #
def ts_to_ms(value) -> int | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    t = pd.Timestamp(value)
    if pd.isna(t):
        return None
    return int(t.timestamp() * 1000)


def ms_to_iso(ms: int | None) -> str | None:
    if ms is None:
        return None
    return pd.Timestamp(ms, unit="ms").strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]


def nan_to_none(value):
    """pandas/NaN -> JSON-friendly python value."""
    if value is None:
        return None
    if isinstance(value, float):
        if value != value:  # NaN
            return None
        return float(value)
    if isinstance(value, (pd.Timestamp,)):
        if pd.isna(value):
            return None
        return pd.Timestamp(value).strftime("%Y-%m-%d %H:%M:%S")
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, (bool, int, str)):
        return value
    if hasattr(value, "item"):
        return value.item()
    return str(value)


def row_to_payload(row: pd.Series, columns: tuple[str, ...]) -> dict:
    return {c: nan_to_none(row.get(c)) for c in columns}


# --------------------------------------------------------------------------- #
# Event collector
# --------------------------------------------------------------------------- #
@dataclass
class Event:
    entity: str
    op: str
    event_ts: int | None  # epoch ms, business time, strictly increasing per key
    lsn: int
    before: dict | None
    after: dict | None
    source_table: str
    snapshot: bool = False
    redelivery: bool = False
    wave: int = 0
    note: str | None = None  # human annotation for demo/debugging


class EventLog:
    """Collects events, keeps per-key business time strictly increasing."""

    def __init__(self) -> None:
        self.events: list[Event] = []
        self._last_ts: dict[tuple[str, tuple], int] = {}
        self._lsn = 0

    def _monotonic(self, entity: str, key_values: tuple, ms: int) -> int:
        k = (entity, key_values)
        prev = self._last_ts.get(k)
        if prev is not None and ms <= prev:
            ms = prev + 1  # 1 ms nudge keeps ordering total without lying much
        self._last_ts[k] = ms
        return ms

    def emit(
        self,
        entity: Entity,
        op: str,
        event_ts,
        before: dict | None,
        after: dict | None,
        *,
        snapshot: bool = False,
        note: str | None = None,
        redelivery: bool = False,
    ) -> None:
        ms = ts_to_ms(event_ts)
        if ms is None:
            # A record with no usable business time: emit it anyway so the
            # pipeline has something real to quarantine.
            ms = self._last_ts.get((entity.name, ()), 0) or 0
        key_values = tuple((after or before or {}).get(k) for k in entity.key)
        # A null/degenerate key must NOT poison the monotonic map; emit as-is.
        if any(v is None for v in key_values):
            safe_ms = ms
        else:
            safe_ms = self._monotonic(entity.name, key_values, ms)
        self._lsn += 1
        self.events.append(
            Event(
                entity=entity.name,
                op=op,
                event_ts=safe_ms,
                lsn=self._lsn,
                before=before,
                after=after,
                source_table=entity.source_table or f"olist.{entity.name}",
                snapshot=snapshot,
                redelivery=redelivery,
                note=note,
            )
        )

    def to_json(self, ev: Event) -> dict:
        def clean(payload):
            if payload is None:
                return None
            return {k: nan_to_none(v) for k, v in payload.items()}

        return {
            "op": ev.op,
            "event_ts": ms_to_iso(ev.event_ts),
            "ts_ms": ev.event_ts,
            "lsn": ev.lsn,
            "source": {
                "connector": "olist-cdc-sim",
                "version": "1.0.0",
                "entity": ev.entity,
                "table": ev.source_table,
                "snapshot": ev.snapshot,
            },
            "before": clean(ev.before),
            "after": clean(ev.after),
        }


# --------------------------------------------------------------------------- #
# Load + lifecycle reconstruction
# --------------------------------------------------------------------------- #
def load_olist(base: Path) -> dict[str, pd.DataFrame]:
    """
    Read every Olist CSV as text, then coerce the columns we actually want to
    be numeric / zero-padded. Returns a dict keyed by logical entity.
    """
    files = {
        "customers": "olist_customers_dataset.csv",
        "geolocation": "olist_geolocation_dataset.csv",
        "order_items": "olist_order_items_dataset.csv",
        "order_payments": "olist_order_payments_dataset.csv",
        "order_reviews": "olist_order_reviews_dataset.csv",
        "orders": "olist_orders_dataset.csv",
        "products": "olist_products_dataset.csv",
        "sellers": "olist_sellers_dataset.csv",
        "category_translation": "product_category_name_translation.csv",
    }
    log.info("reading Olist CSVs from %s", base)
    data: dict[str, pd.DataFrame] = {}
    for entity, fname in files.items():
        path = base / fname
        if not path.exists():
            raise FileNotFoundError(
                f"missing {path}. Run tools/download_olist.py first."
            )
        df = pd.read_csv(path, encoding="utf-8-sig", dtype=str)
        for col in COERCE_INT.get(entity, ()):
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
        for col in COERCE_DOUBLE.get(entity, ()):
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        for col in COERCE_ZIP5.get(entity, ()):
            if col in df.columns:
                df[col] = (
                    df[col]
                    .fillna("")
                    .str.replace(r"\.0$", "", regex=True)
                    .str.strip()
                    .str.zfill(5)
                    .replace("", pd.NA)
                )
        data[entity] = df
    log.info("loaded %d tables", len(data))
    return data


def select_orders(
    orders: pd.DataFrame, max_orders: int | None, start: str | None, end: str | None
) -> pd.DataFrame:
    o = orders.copy()
    o["_purchase"] = pd.to_datetime(o["order_purchase_timestamp"], errors="coerce")
    if start:
        o = o[o["_purchase"] >= pd.Timestamp(start)]
    if end:
        o = o[o["_purchase"] < pd.Timestamp(end)]
    o = o.dropna(subset=["_purchase"]).sort_values("_purchase")
    if max_orders and len(o) > max_orders:
        # Take a contiguous time slice rather than a random sample: keeps the
        # lifecycle coherent and the wave boundaries meaningful.
        o = o.iloc[:max_orders]
    return o.drop(columns=["_purchase"]).reset_index(drop=True)


def build_reference_entities(log_: EventLog, data: dict) -> None:
    """Initial snapshot rows for customers / products / sellers."""
    translation = dict(
        zip(
            data["category_translation"]["product_category_name"],
            data["category_translation"]["product_category_name_english"],
        )
    )

    for _, r in data["customers"].iterrows():
        payload = row_to_payload(r, ENTITIES["customers"].columns)
        log_.emit(
            ENTITIES["customers"],
            "r",
            # A snapshot has no event time of its own; anchor it just before the
            # earliest activity so the SCD2 timeline stays sensible.
            pd.Timestamp("2016-08-01"),
            None,
            payload,
            snapshot=True,
            note="initial snapshot",
        )

    for _, r in data["products"].iterrows():
        payload = row_to_payload(r, ENTITIES["products"].columns)
        payload["product_category_name_english"] = translation.get(
            payload.get("product_category_name")
        )
        log_.emit(
            ENTITIES["products"],
            "r",
            pd.Timestamp("2016-08-01"),
            None,
            payload,
            snapshot=True,
            note="initial snapshot",
        )

    for _, r in data["sellers"].iterrows():
        payload = row_to_payload(r, ENTITIES["sellers"].columns)
        log_.emit(
            ENTITIES["sellers"],
            "r",
            pd.Timestamp("2016-08-01"),
            None,
            payload,
            snapshot=True,
            note="initial snapshot",
        )


# Order lifecycle: which states the final `order_status` implies, and the
# timestamp column that triggers each transition.
LIFECYCLE_TS_COLUMN = {
    "approved": "order_approved_at",
    "shipped": "order_delivered_carrier_date",
    "invoiced": "order_delivered_carrier_date",
    "delivered": "order_delivered_customer_date",
}


def order_state_path(final_status: str) -> list[str]:
    """Expand a final order_status into the state sequence that produced it."""
    if final_status == "delivered":
        return ["created", "approved", "shipped", "delivered"]
    if final_status == "shipped":
        return ["created", "approved", "shipped"]
    if final_status == "invoiced":
        return ["created", "approved", "invoiced"]
    if final_status == "processing":
        return ["created", "approved", "processing"]
    if final_status == "approved":
        return ["created", "approved"]
    if final_status == "created":
        return ["created"]
    if final_status == "canceled":
        return ["created", "approved", "canceled", "__maybe_delete__"]
    if final_status == "unavailable":
        return ["created", "approved", "unavailable"]
    return ["created"]


def build_order_events(
    log_: EventLog, orders: pd.DataFrame, rng: random.Random, cfg: argparse.Namespace
) -> None:
    ent = ENTITIES["orders"]
    est_delta = defaultdict(int)

    for _, r in orders.iterrows():
        base = row_to_payload(r, ent.columns)
        purchase = pd.to_datetime(r["order_purchase_timestamp"], errors="coerce")
        if pd.isna(purchase):
            continue

        path = order_state_path(str(r["order_status"]))
        prev = None
        carrier_ts: pd.Timestamp | None = None
        delivered_ts: pd.Timestamp | None = None

        for idx, state in enumerate(path):
            if state == "__maybe_delete__":
                # Cancelled orders: sometimes the row is hard-deleted
                # (a "purge"), sometimes it survives as status=canceled.
                if rng.random() < cfg.p_delete_canceled:
                    log_.emit(
                        ent,
                        "d",
                        prev_ts,
                        before=prev,
                        after=None,
                        note="cancelled order purged",
                    )
                continue

            if state == "created":
                cur_ts = purchase
            elif state == "canceled":
                # No cancel timestamp exists in Olist; synthesize one shortly
                # after purchase, before the estimated delivery date.
                cur_ts = purchase + pd.Timedelta(
                    days=rng.randint(1, 25), hours=rng.randint(0, 23)
                )
            elif state == "unavailable":
                cur_ts = purchase + pd.Timedelta(
                    days=rng.randint(2, 30), hours=rng.randint(0, 23)
                )
            elif state == "processing":
                # No dedicated column: processing sits between approval and
                # hand-over to the carrier.
                approved = pd.to_datetime(r.get("order_approved_at"),
                                          errors="coerce")
                anchor = approved if not pd.isna(approved) else purchase
                cur_ts = pd.Timestamp(anchor) + pd.Timedelta(
                    hours=rng.randint(1, 48)
                )
            else:
                col = LIFECYCLE_TS_COLUMN[state]
                raw = r.get(col)
                cur_ts = pd.to_datetime(raw, errors="coerce")
                if pd.isna(cur_ts):
                    # Missing transition timestamp -> stay in the previous
                    # state; this is exactly how Olist encodes in-flight rows.
                    continue
                if pd.Timestamp(cur_ts) < purchase:
                    cur_ts = purchase
                if state == "shipped":
                    carrier_ts = pd.Timestamp(cur_ts)
                if state == "delivered":
                    delivered_ts = pd.Timestamp(cur_ts)

            after = dict(base)
            after["order_status"] = state
            # A CDC image must reflect the row *as it was at that moment*: no
            # future timestamps may leak into a non-terminal state.
            if state == "created":
                after["order_approved_at"] = None
            if state not in ("shipped", "invoiced", "delivered"):
                after["order_delivered_carrier_date"] = None
            if state != "delivered":
                after["order_delivered_customer_date"] = None

            # Enrichment column that only starts appearing from wave N onwards
            # (auto-computed for delivered orders) -- Auto Loader schema drift.
            if delivered_ts is not None and not pd.isna(delivered_ts):
                est = pd.to_datetime(r.get("order_estimated_delivery_date"),
                                     errors="coerce")
                if not pd.isna(est):
                    after["delivery_sla_breach"] = bool(
                        pd.Timestamp(delivered_ts) > pd.Timestamp(est)
                    )

            prev_ts = cur_ts
            log_.emit(
                ent,
                "c" if idx == 0 else "u",
                cur_ts,
                before=prev,
                after=after,
                note=f"lifecycle -> {state}",
            )
            prev = after

        # Occasionally the customer nudges the estimated delivery date.
        if rng.random() < cfg.p_eta_change and prev is not None:
            after = dict(prev)
            bump = prev_ts + pd.Timedelta(days=rng.randint(1, 5))
            after["order_estimated_delivery_date"] = (
                pd.Timestamp(after["order_estimated_delivery_date"]) + timedelta(days=rng.randint(1, 7))
                if after.get("order_estimated_delivery_date")
                else None
            )
            log_.emit(ent, "u", bump, before=prev, after=after,
                      note="estimated delivery date revised")


def build_child_events(
    log_: EventLog,
    data: dict,
    orders: pd.DataFrame,
    rng: random.Random,
    cfg: argparse.Namespace,
) -> None:
    order_ids = set(orders["order_id"])
    purchase_by_order = dict(
        zip(orders["order_id"],
            pd.to_datetime(orders["order_purchase_timestamp"], errors="coerce"))
    )

    # ---- order_items -----------------------------------------------------
    ent = ENTITIES["order_items"]
    items = data["order_items"]
    items = items[items["order_id"].isin(order_ids)]
    for _, r in items.iterrows():
        payload = row_to_payload(r, ent.columns)
        ts = purchase_by_order.get(r["order_id"])
        if ts is None or pd.isna(ts):
            ts = pd.Timestamp("2017-01-01")
        log_.emit(ent, "c", ts + pd.Timedelta(minutes=1), None, payload,
                  note="item added")
        # price correction / freight correction
        if rng.random() < cfg.p_item_price_change:
            after = dict(payload)
            after["price"] = round(float(payload["price"]) *
                                   rng.uniform(0.9, 1.1), 2)
            log_.emit(ent, "u", ts + pd.Timedelta(days=rng.randint(1, 10)),
                      before=payload, after=after, note="price corrected")
            payload = after
        # item removed from the order
        if rng.random() < cfg.p_item_removed:
            log_.emit(ent, "d", ts + pd.Timedelta(days=rng.randint(2, 20)),
                      before=payload, after=None, note="item removed")

    # ---- order_payments --------------------------------------------------
    ent = ENTITIES["order_payments"]
    pays = data["order_payments"]
    pays = pays[pays["order_id"].isin(order_ids)]
    for _, r in pays.iterrows():
        payload = row_to_payload(r, ent.columns)
        ts = purchase_by_order.get(r["order_id"])
        if ts is None or pd.isna(ts):
            ts = pd.Timestamp("2017-01-01")
        log_.emit(ent, "c", ts + pd.Timedelta(seconds=30), None, payload,
                  note="payment captured")
        roll = rng.random()
        if roll < cfg.p_payment_correction:
            after = dict(payload)
            after["payment_value"] = round(float(payload["payment_value"]) *
                                           rng.uniform(0.95, 1.05), 2)
            log_.emit(ent, "u", ts + pd.Timedelta(days=rng.randint(1, 5)),
                      before=payload, after=after, note="payment corrected")
        elif roll < cfg.p_payment_correction + cfg.p_payment_failed:
            log_.emit(ent, "d", ts + pd.Timedelta(days=rng.randint(1, 3)),
                      before=payload, after=None, note="payment reversed")

    # ---- order_reviews ---------------------------------------------------
    ent = ENTITIES["order_reviews"]
    revs = data["order_reviews"]
    revs = revs[revs["order_id"].isin(order_ids)]
    review_edit_ids: list[tuple[str, pd.Timestamp, dict]] = []
    for _, r in revs.iterrows():
        payload = row_to_payload(r, ent.columns)
        created = pd.to_datetime(r.get("review_creation_date"), errors="coerce")
        if pd.isna(created):
            created = purchase_by_order.get(r["order_id"])
            if created is None or pd.isna(created):
                created = pd.Timestamp("2017-01-01")
            created = pd.Timestamp(created) + pd.Timedelta(days=7)
        log_.emit(ent, "c", created, None, payload, note="review submitted")
        # Reviews are the one entity where a *partial* update is realistic:
        # the customer edits only the score/comment and the CRM sends only the
        # changed columns. The pipeline uses IGNORE NULL UPDATES for this.
        if rng.random() < cfg.p_review_edit:
            answered = pd.to_datetime(r.get("review_answer_timestamp"),
                                      errors="coerce")
            edit_ts = (
                pd.Timestamp(answered) + pd.Timedelta(hours=2)
                if not pd.isna(answered)
                else pd.Timestamp(created) + pd.Timedelta(days=2)
            )
            partial = {
                "review_id": payload["review_id"],
                "order_id": payload["order_id"],
                "review_score": rng.choice([1, 2, 3, 4, 5]),
                "review_comment_title": payload.get("review_comment_title"),
                "review_comment_message": payload.get("review_comment_message"),
            }
            review_edit_ids.append((payload["review_id"], edit_ts, partial))

    for _, edit_ts, partial in review_edit_ids:
        log_.emit(ent, "u", edit_ts, before=None, after=partial,
                  note="review edited (partial update)")


def build_dimension_changes(
    log_: EventLog, rng: random.Random, cfg: argparse.Namespace
) -> None:
    """
    Post-snapshot mutations on the reference entities: address moves, category
    recategorisation, discontinuations and a GDPR erasure. These produce the
    SCD2 history that makes the silver layer interesting.
    """
    # --- customer address changes + GDPR erasure -------------------------
    cust_events = [e for e in log_.events
                   if e.entity == "customers" and e.op == "r"]
    rng.shuffle(cust_events)
    for ev in cust_events[: int(len(cust_events) * cfg.p_customer_move)]:
        base = dict(ev.after)
        move_ts = pd.Timestamp(
            f"2017-{rng.randint(1, 12):02d}-{rng.randint(1, 28):02d} "
            f"{rng.randint(0, 23):02d}:{rng.randint(0, 59):02d}:00"
        )
        after = dict(base)
        after["customer_city"] = rng.choice(
            ["sao paulo", "rio de janeiro", "belo horizonte", "curitiba",
             "porto alegre", "salvador", "recife", "brasilia"]
        )
        after["customer_state"] = rng.choice(["SP", "RJ", "MG", "PR", "RS",
                                              "BA", "PE", "DF"])
        after["customer_zip_code_prefix"] = f"{rng.randint(1000, 99999):05d}"
        log_.emit(ENTITIES["customers"], "u", move_ts, before=base, after=after,
                  note="address change")

    rng.shuffle(cust_events)
    for ev in cust_events[: max(1, int(len(cust_events) * cfg.p_gdpr_delete))]:
        base = dict(ev.after)
        del_ts = pd.Timestamp(
            f"2018-{rng.randint(1, 8):02d}-{rng.randint(1, 28):02d} "
            f"{rng.randint(0, 23):02d}:{rng.randint(0, 59):02d}:00"
        )
        log_.emit(ENTITIES["customers"], "d", del_ts, before=base, after=None,
                  note="GDPR right-to-be-forgotten")

    # --- product recategorisation + discontinuation ----------------------
    prod_events = [e for e in log_.events
                   if e.entity == "products" and e.op == "r"]
    rng.shuffle(prod_events)
    for ev in prod_events[: int(len(prod_events) * cfg.p_product_change)]:
        base = dict(ev.after)
        chg_ts = pd.Timestamp(
            f"2017-{rng.randint(1, 12):02d}-{rng.randint(1, 28):02d} "
            f"{rng.randint(0, 23):02d}:{rng.randint(0, 59):02d}:00"
        )
        after = dict(base)
        if rng.random() < 0.5:
            after["product_category_name"] = rng.choice(
                ["cama_mesa_banho", "esporte_lazer", "moveis_decoracao",
                 "utilidades_domesticas", "informatica_acessorios"]
            )
            after["product_category_name_english"] = rng.choice(
                ["bed_bath_table", "sports_leisure", "furniture_decor",
                 "housewares", "computers_accessories"]
            )
        else:
            after["product_weight_g"] = int(rng.randint(100, 8000))
            after["product_photos_qty"] = int(rng.randint(1, 6))
        log_.emit(ENTITIES["products"], "u", chg_ts, before=base, after=after,
                  note="catalogue update")

    rng.shuffle(prod_events)
    for ev in prod_events[: max(1, int(len(prod_events) * cfg.p_product_discontinued))]:
        base = dict(ev.after)
        del_ts = pd.Timestamp(
            f"2018-{rng.randint(1, 8):02d}-{rng.randint(1, 28):02d} "
            f"{rng.randint(0, 23):02d}:{rng.randint(0, 59):02d}:00"
        )
        log_.emit(ENTITIES["products"], "d", del_ts, before=base, after=None,
                  note="product discontinued")

    # --- seller relocation ------------------------------------------------
    seller_events = [e for e in log_.events
                     if e.entity == "sellers" and e.op == "r"]
    rng.shuffle(seller_events)
    for ev in seller_events[: int(len(seller_events) * cfg.p_seller_move)]:
        base = dict(ev.after)
        mv_ts = pd.Timestamp(
            f"2017-{rng.randint(1, 12):02d}-{rng.randint(1, 28):02d} "
            f"{rng.randint(0, 23):02d}:{rng.randint(0, 59):02d}:00"
        )
        after = dict(base)
        after["seller_city"] = rng.choice(["sao paulo", "campinas", "curitiba",
                                           "guarulhos", "osasco"])
        after["seller_state"] = rng.choice(["SP", "PR"])
        log_.emit(ENTITIES["sellers"], "u", mv_ts, before=base, after=after,
                  note="seller relocated")


def build_truncate(log_: EventLog, cfg: argparse.Namespace) -> None:
    """
    A TRUNCATE op, so APPLY AS TRUNCATE in the pipeline has something real.

    Timed at the very start of the stream on purpose. A `t` op means "the source
    truncated this table and then re-delivered it from scratch" -- which is what
    APPLY AS TRUNCATE exists to survive. To model that faithfully the marker has
    to arrive BEFORE the reload, so the target ends up complete.

    Do not be tempted to timestamp this at the end of the stream: the truncate
    would then be the last thing the flow sees and the whole table would be
    wiped, which is correct behaviour but a terrible demo. The /notebooks
    30_reprocess_and_late_data.py walks through the destructive case explicitly.
    """
    ent = ENTITIES["order_reviews"]
    log_.emit(ent, "t", pd.Timestamp("2016-08-01T00:00:00"), before=None,
              after=None, note="full reload marker (truncate, then re-deliver)")


# --------------------------------------------------------------------------- #
# Chaos: waves, lateness, duplication, malformed rows
# --------------------------------------------------------------------------- #
def assign_waves(log_: EventLog, n_waves: int) -> None:
    times = sorted({e.event_ts for e in log_.events})
    if not times:
        return
    # equal-count quantile boundaries -> each wave is a comparable size
    bounds = [times[min(len(times) - 1, int((i / n_waves) * len(times)))]
              for i in range(1, n_waves)]
    for e in log_.events:
        e.wave = bisect.bisect_left(bounds, e.event_ts) + 1


def inject_lateness_and_dupes(
    log_: EventLog, rng: random.Random, cfg: argparse.Namespace
) -> None:
    extra: list[Event] = []
    for e in list(log_.events):
        # late arrival: delivered in a later wave than its business time
        if rng.random() < cfg.p_late and e.wave < cfg.waves:
            shift = rng.randint(1, max(1, cfg.late_max_waves))
            shifted = Event(**{**e.__dict__})
            shifted.wave = min(cfg.waves, e.wave + shift)
            shifted.note = (e.note or "") + f" | late by {shift} wave(s)"
            e.wave = -1  # mark original for removal, replaced by shifted copy
            extra.append(shifted)
        # at-least-once redelivery
        elif rng.random() < cfg.p_duplicate and e.wave < cfg.waves:
            dup = Event(**{**e.__dict__})
            dup.wave = min(cfg.waves, e.wave + rng.randint(1, 2))
            dup.redelivery = True
            dup.note = (e.note or "") + " | at-least-once redelivery"
            extra.append(dup)
    log_.events = [e for e in log_.events if e.wave != -1] + extra


def inject_malformed(log_: EventLog, rng: random.Random,
                     cfg: argparse.Namespace) -> list[str]:
    """
    Adds rows that *should* be rejected (and are, by pipeline expectations) and
    returns a list of raw broken JSON lines for the rescued-data path.
    """
    broken: list[str] = []
    # Only create/read events: their before-image is null, so a nulled key
    # really is null after the pipeline's COALESCE(after, before).
    candidates = [e for e in log_.events if e.op in ("c", "r") and e.after]
    rng.shuffle(candidates)
    n = max(1, int(len(candidates) * cfg.p_malformed))

    for e in candidates[:n]:
        roll = rng.random()
        if roll < 0.3:
            # null primary key -> expectation violation, quarantined
            bad = Event(**{**e.__dict__})
            bad.after = {**bad.after, ENTITIES[e.entity].key[0]: None}
            bad.note = "malformed: null primary key"
            log_.events.append(bad)
        elif roll < 0.6:
            # unknown op code -> expectation violation
            bad = Event(**{**e.__dict__})
            bad.op = "x"
            bad.note = "malformed: unknown op code"
            log_.events.append(bad)
        elif roll < 0.8:
            # unparseable business time
            bad = Event(**{**e.__dict__})
            bad.event_ts = None
            bad.note = "malformed: missing event_ts"
            log_.events.append(bad)
        else:
            # genuinely broken JSON (truncated) -> Auto Loader rescued data
            line = json.dumps(log_.to_json(e))
            broken.append(line[: max(10, len(line) // 2)])

    log_.events.sort(key=lambda ev: (ev.lsn,))
    return broken


# --------------------------------------------------------------------------- #
# Writers
# --------------------------------------------------------------------------- #
def write_landing(
    log_: EventLog, broken: list[str], out: Path, rng: random.Random,
    cfg: argparse.Namespace,
) -> dict:
    events_dir = out / "events"
    if out.exists() and cfg.clean:
        shutil.rmtree(out)
    events_dir.mkdir(parents=True, exist_ok=True)

    groups: dict[tuple[str, int], list[Event]] = defaultdict(list)
    for e in log_.events:
        groups[(e.entity, e.wave)].append(e)

    manifest = {
        "generator": "tools/generate_cdc.py",
        "seed": cfg.seed,
        "waves": cfg.waves,
        "entities": {},
        "totals": {"events": 0, "files": 0, "broken_lines": 0},
    }

    opener = gzip.open if cfg.gzip else open
    ext = ".jsonl.gz" if cfg.gzip else ".jsonl"
    kwargs = {"mode": "wt", "encoding": "utf-8"} if cfg.gzip else {"mode": "w",
                                                                  "encoding": "utf-8"}

    for (entity, wave), evs in sorted(groups.items()):
        # arrival order deliberately != business order
        if cfg.shuffle_arrival:
            rng.shuffle(evs)
        n_parts = max(1, cfg.parts_per_wave)
        per_part = max(1, (len(evs) + n_parts - 1) // n_parts)
        ent_dir = events_dir / entity
        ent_dir.mkdir(parents=True, exist_ok=True)
        stats = manifest["entities"].setdefault(
            entity, {"events": 0, "by_op": defaultdict(int), "by_wave": {}}
        )
        for p in range(n_parts):
            chunk = evs[p * per_part: (p + 1) * per_part]
            if not chunk:
                continue
            fname = f"{entity}_w{wave:02d}_p{p + 1:03d}{ext}"
            with opener(ent_dir / fname, **kwargs) as fh:
                for e in chunk:
                    fh.write(json.dumps(log_.to_json(e), separators=(",", ":")) + "\n")
            manifest["totals"]["files"] += 1
        for e in evs:
            stats["events"] += 1
            stats["by_op"][e.op] += 1
            stats["by_wave"][str(wave)] = stats["by_wave"].get(str(wave), 0) + 1
        manifest["totals"]["events"] += len(evs)

    # a wave-3 file of literally-broken JSON, for the rescued-data demo
    if broken:
        d = events_dir / "orders"
        d.mkdir(parents=True, exist_ok=True)
        fname = f"orders_w{max(1, cfg.waves // 2):02d}_p999_broken{ext}"
        with opener(d / fname, **kwargs) as fh:
            for line in broken:
                fh.write(line + "\n")
        manifest["totals"]["files"] += 1
        manifest["totals"]["broken_lines"] = len(broken)

    # normalise defaultdicts for json
    for v in manifest["entities"].values():
        v["by_op"] = dict(v["by_op"])

    (out / "_manifest.json").write_text(json.dumps(manifest, indent=2))
    return manifest


def write_reference(out: Path, data: dict, cfg: argparse.Namespace) -> None:
    ref = out / "reference"
    ref.mkdir(parents=True, exist_ok=True)
    g = data["geolocation"].copy()
    if cfg.geolocation_rows and len(g) > cfg.geolocation_rows:
        g = g.sample(n=cfg.geolocation_rows, random_state=cfg.seed)
    g["geolocation_lat"] = pd.to_numeric(g["geolocation_lat"], errors="coerce")
    g["geolocation_lng"] = pd.to_numeric(g["geolocation_lng"], errors="coerce")
    agg = (
        g.groupby("geolocation_zip_code_prefix", as_index=False)
        .agg(
            geolocation_lat=("geolocation_lat", "mean"),
            geolocation_lng=("geolocation_lng", "mean"),
            geolocation_city=("geolocation_city", "first"),
            geolocation_state=("geolocation_state", "first"),
            sample_rows=("geolocation_lat", "size"),
        )
        .round({"geolocation_lat": 5, "geolocation_lng": 5})
    )
    agg.to_csv(ref / "geolocation_by_zip.csv", index=False)
    log.info("reference/geolocation_by_zip.csv: %d zip prefixes", len(agg))


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Turn the Kaggle Olist snapshot into a messy Debezium-style CDC stream.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    src = p.add_argument_group("source")
    src.add_argument("--olist-dir", type=Path,
                     default=Path("data/olist"),
                     help="directory containing the raw Olist CSV files")
    src.add_argument("--max-orders", type=int, default=25000,
                     help="cap orders (keeps a Free Edition run snappy); 0 = all")
    src.add_argument("--start-date", default="2017-01-01")
    src.add_argument("--end-date", default="2018-07-01")
    src.add_argument("--geolocation-rows", type=int, default=400000,
                     help="sample size before aggregating to zip level; 0 = all")

    gen = p.add_argument_group("stream shape")
    gen.add_argument("--out", type=Path, default=Path("data/landing"))
    gen.add_argument("--waves", type=int, default=6,
                     help="number of delivery waves (≈ incremental runs)")
    gen.add_argument("--parts-per-wave", type=int, default=3,
                     help="files per entity per wave")
    gen.add_argument("--seed", type=int, default=42)
    gen.add_argument("--gzip", dest="gzip", action="store_true", default=True)
    gen.add_argument("--no-gzip", dest="gzip", action="store_false")
    gen.add_argument("--clean", dest="clean", action="store_true", default=True,
                     help="wipe --out before writing")
    gen.add_argument("--no-clean", dest="clean", action="store_false")
    gen.add_argument("--shuffle-arrival", dest="shuffle_arrival",
                     action="store_true", default=True,
                     help="shuffle events within a wave so arrival order != business order")
    gen.add_argument("--ordered-arrival", dest="shuffle_arrival",
                     action="store_false")

    chaos = p.add_argument_group("chaos injection")
    chaos.add_argument("--late", dest="p_late", type=float, default=0.06,
                       help="probability an event arrives late")
    chaos.add_argument("--late-max-waves", type=int, default=2)
    chaos.add_argument("--duplicate", dest="p_duplicate", type=float, default=0.03,
                       help="probability an event is redelivered")
    chaos.add_argument("--malformed", dest="p_malformed", type=float, default=0.004,
                       help="probability an event is malformed (-> quarantine)")
    chaos.add_argument("--delete-canceled", dest="p_delete_canceled",
                       type=float, default=0.35)
    chaos.add_argument("--customer-move", dest="p_customer_move",
                       type=float, default=0.05)
    chaos.add_argument("--gdpr-delete", dest="p_gdpr_delete", type=float, default=0.002)
    chaos.add_argument("--product-change", dest="p_product_change",
                       type=float, default=0.06)
    chaos.add_argument("--product-discontinued", dest="p_product_discontinued",
                       type=float, default=0.01)
    chaos.add_argument("--seller-move", dest="p_seller_move", type=float, default=0.03)
    chaos.add_argument("--eta-change", dest="p_eta_change", type=float, default=0.05)
    chaos.add_argument("--item-price-change", dest="p_item_price_change",
                       type=float, default=0.02)
    chaos.add_argument("--item-removed", dest="p_item_removed", type=float, default=0.01)
    chaos.add_argument("--payment-correction", dest="p_payment_correction",
                       type=float, default=0.02)
    chaos.add_argument("--payment-failed", dest="p_payment_failed", type=float, default=0.01)
    chaos.add_argument("--review-edit", dest="p_review_edit", type=float, default=0.04)
    chaos.add_argument("--truncate", dest="truncate", action="store_true",
                       default=True, help="emit a TRUNCATE op for order_reviews")
    chaos.add_argument("--no-truncate", dest="truncate", action="store_false")
    return p.parse_args(argv)


def main(argv=None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    cfg = parse_args(argv)
    rng = random.Random(cfg.seed)

    data = load_olist(cfg.olist_dir)
    orders = select_orders(data["orders"], cfg.max_orders or None,
                           cfg.start_date, cfg.end_date)
    log.info("orders in scope: %d", len(orders))

    keep_customers = set(orders["customer_id"])
    data["customers"] = data["customers"][
        data["customers"]["customer_id"].isin(keep_customers)
    ]
    keep_orders = set(orders["order_id"])
    for t in ("order_items", "order_payments", "order_reviews"):
        data[t] = data[t][data[t]["order_id"].isin(keep_orders)]
    # products/sellers referenced by kept items (+ their full history)
    keep_products = set(data["order_items"]["product_id"])
    keep_sellers = set(data["order_items"]["seller_id"])
    data["products"] = data["products"][
        data["products"]["product_id"].isin(keep_products)
    ]
    data["sellers"] = data["sellers"][data["sellers"]["seller_id"].isin(keep_sellers)]
    log.info("customers=%d products=%d sellers=%d items=%d payments=%d reviews=%d",
             len(data["customers"]), len(data["products"]), len(data["sellers"]),
             len(data["order_items"]), len(data["order_payments"]),
             len(data["order_reviews"]))

    log_ = EventLog()
    build_reference_entities(log_, data)
    build_order_events(log_, orders, rng, cfg)
    build_child_events(log_, data, orders, rng, cfg)
    build_dimension_changes(log_, rng, cfg)
    if cfg.truncate:
        build_truncate(log_, cfg)

    assign_waves(log_, cfg.waves)
    # Drift is a property of the SOURCE schema at a point in business time, so
    # it must be applied before lateness/duplication: a redelivered record has
    # to carry byte-identical content, otherwise replays are not idempotent.
    apply_column_drift(log_, cfg)
    inject_lateness_and_dupes(log_, rng, cfg)
    broken = inject_malformed(log_, rng, cfg)

    manifest = write_landing(log_, broken, cfg.out, rng, cfg)
    write_reference(cfg.out, data, cfg)

    log.info("wrote %d events across %d files to %s",
             manifest["totals"]["events"], manifest["totals"]["files"], cfg.out)
    for entity, stats in sorted(manifest["entities"].items()):
        log.info("  %-16s %7d events  ops=%s", entity, stats["events"],
                 stats["by_op"])
    print(json.dumps(manifest["totals"], indent=2))
    return 0


def apply_column_drift(log_: EventLog, cfg: argparse.Namespace) -> None:
    """
    Make the schema evolve the way real source systems do:
      * add a brand-new column from wave N onwards
      * RENAME a column (Olist's real `product_name_lenght` typo)
    Auto Loader's addNewColumns handles both without failing the pipeline --
    a rename shows up as "new column appears, old column goes null", which is
    exactly the behaviour worth demonstrating (and worth knowing about).
    """
    for e in log_.events:
        if e.after is None:
            continue
        drift = DRIFT_NEW_COLUMNS.get(e.entity)
        if drift:
            col, first_wave = drift
            if e.wave >= first_wave and col not in e.after:
                e.after = {**e.after, col: _drift_default(e, col)}
        ren = DRIFT_RENAME.get(e.entity)
        if ren:
            old, new, first_wave = ren
            if e.wave >= first_wave and old in e.after:
                after = {k: v for k, v in e.after.items() if k != old}
                after[new] = e.after[old]
                e.after = after


def _drift_default(ev: Event, column: str):
    """Give a newly-appeared column a plausible value (not just NULL)."""
    if column == "product_launch_date":
        pid = str((ev.after or {}).get("product_id", ev.lsn))
        seed = zlib.crc32(pid.encode("utf-8")) % 900
        return (pd.Timestamp("2015-01-01") + pd.Timedelta(days=seed)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
    if column == "delivery_sla_breach":
        return False
    return None


if __name__ == "__main__":
    raise SystemExit(main())
