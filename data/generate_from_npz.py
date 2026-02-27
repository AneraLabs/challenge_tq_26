#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import random
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Optional, Tuple

import numpy as np


# =============================================================================
# HFTBacktest constants
# =============================================================================
DEPTH_EVENT = 1
TRADE_EVENT = 2
DEPTH_CLEAR_EVENT = 3
DEPTH_SNAPSHOT_EVENT = 4

BUY_EVENT = 1 << 29
SELL_EVENT = 1 << 28

# Custom event types (ignored for market_events.csv)
FUNDING_RATE_EVENT = 102
OPEN_INTEREST_EVENT = 106
MARK_PRICE_EVENT = 104
INDEX_PRICE_EVENT = 105
TICK_SIZE_CHANGE_EVENT = 200

CUSTOM_TYPES = {
    FUNDING_RATE_EVENT,
    OPEN_INTEREST_EVENT,
    MARK_PRICE_EVENT,
    INDEX_PRICE_EVENT,
    TICK_SIZE_CHANGE_EVENT,
}


# =============================================================================
# Output conventions
# =============================================================================
# market_events.csv columns:
#   ts,type,side,price,size,trade_aggr_side
# simulated_events.csv columns:
#   t0,side,qty,horizon_ms,price_mode,price

SIDE_BID = "B"
SIDE_ASK = "A"

AGGR_BUY = "B"   # buyer-initiated -> consumes asks
AGGR_SELL = "S"  # seller-initiated -> consumes bids


# =============================================================================
# Structured input helpers
# =============================================================================

def load_structured_array(path: str, events_key: Optional[str]) -> np.ndarray:
    """
    Supports:
      - .npy containing a structured array
      - .npz containing a structured array member
    """
    obj = np.load(path, mmap_mode="r", allow_pickle=False)

    # .npz case: dict-like NpzFile with .files
    if isinstance(obj, np.lib.npyio.NpzFile):  # type: ignore[attr-defined]
        keys = list(obj.files)
        if events_key is not None:
            if events_key not in keys:
                raise SystemExit(f"--events-key {events_key!r} not in NPZ. Keys={keys}")
            arr = obj[events_key]
        else:
            # try sensible defaults, else first structured
            arr = None
            for k in ("events", "data", "ticks", "records"):
                if k in keys and isinstance(obj[k], np.ndarray) and obj[k].dtype.fields:
                    arr = obj[k]
                    break
            if arr is None:
                for k in keys:
                    cand = obj[k]
                    if isinstance(cand, np.ndarray) and cand.dtype.fields:
                        arr = cand
                        break
            if arr is None:
                raise SystemExit(f"No structured array found in NPZ. Keys={keys}")
        if not (isinstance(arr, np.ndarray) and arr.dtype.fields):
            raise SystemExit("Selected NPZ member is not a structured array.")
        return arr

    # .npy case
    if not (isinstance(obj, np.ndarray) and obj.dtype.fields):
        raise SystemExit("Input is not a structured .npy, or an .npz containing a structured array.")
    return obj


def decode_base_type(ev: int) -> int:
    # Lower 28 bits carry the base type in this scheme
    return ev & ((1 << 28) - 1)


def has_buy(ev: int) -> bool:
    return (ev & BUY_EVENT) != 0


def has_sell(ev: int) -> bool:
    return (ev & SELL_EVENT) != 0


def infer_book_side(ev: int) -> Optional[str]:
    # Depth updates should have BUY_EVENT or SELL_EVENT to indicate side
    if has_buy(ev) and not has_sell(ev):
        return SIDE_BID
    if has_sell(ev) and not has_buy(ev):
        return SIDE_ASK
    # ambiguous / missing
    return None


def infer_trade_aggressor(ev: int) -> str:
    # Trades may carry BUY_EVENT (buyer-initiated) or SELL_EVENT (seller-initiated)
    if has_buy(ev) and not has_sell(ev):
        return AGGR_BUY
    if has_sell(ev) and not has_buy(ev):
        return AGGR_SELL
    return ""


def ev_histogram(events: np.ndarray, max_unique: int = 50) -> list[tuple[int, int]]:
    ev = events["ev"]
    uniq, counts = np.unique(ev, return_counts=True)
    pairs = list(zip([int(x) for x in uniq], [int(c) for c in counts]))
    pairs.sort(key=lambda x: x[1], reverse=True)
    return pairs[:max_unique]


# =============================================================================
# Conversion to market_events.csv
# =============================================================================

def iter_market_rows(
    events: np.ndarray,
    ts_field: str,
    *,
    price_mult: float,
    skip_clear_events: bool,
) -> Iterator[Tuple[int, str, str, int, float, str]]:
    """
    Yields rows for market_events.csv:
      (ts, type, side, price, size, trade_aggr_side)

    - DEPTH_EVENT / DEPTH_SNAPSHOT_EVENT -> type="book" (side=B/A, trade_aggr_side="")
    - TRADE_EVENT -> type="trade" (side="T", trade_aggr_side="B"/"S"/"")
    - DEPTH_CLEAR_EVENT -> skipped by default (see notes)
    - Custom types -> skipped
    """
    ev_arr = events["ev"]
    ts_arr = events[ts_field]
    px_arr = events["px"]
    qty_arr = events["qty"]

    n = int(events.shape[0])
    for i in range(n):
        ev = int(ev_arr[i])
        base = decode_base_type(ev)

        if base in CUSTOM_TYPES:
            continue

        if base == DEPTH_CLEAR_EVENT:
            if skip_clear_events:
                continue
            # If you wanted to represent clears explicitly, you’d need a richer format
            # (because it implies clearing many levels). We skip in this challenge.
            continue

        ts = int(ts_arr[i])
        price = int(round(float(px_arr[i]) * price_mult))
        qty = float(qty_arr[i])

        if base == DEPTH_EVENT or base == DEPTH_SNAPSHOT_EVENT:
            side = infer_book_side(ev)
            if side is None:
                # If your data ever lacks BUY/SELL flags on depth rows,
                # you can choose a fallback here; for now we skip ambiguous.
                continue
            # In this dataset qty is assumed to be the *absolute* size at that price level
            # (consistent with your earlier structured schema).
            yield (ts, "book", side, price, qty, "")

        elif base == TRADE_EVENT:
            ag = infer_trade_aggressor(ev)
            yield (ts, "trade", "T", price, qty, ag)

        else:
            # Unknown base type -> skip
            continue


def write_market_events_csv(
    events: np.ndarray,
    out_csv: str,
    ts_field: str,
    *,
    price_mult: float,
    skip_clear_events: bool,
) -> int:
    outp = Path(out_csv)
    outp.parent.mkdir(parents=True, exist_ok=True)

    rows = 0
    with open(out_csv, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["ts", "type", "side", "price", "size", "trade_aggr_side"])
        for t, typ, side, price, size, ag in iter_market_rows(
            events,
            ts_field=ts_field,
            price_mult=price_mult,
            skip_clear_events=skip_clear_events,
        ):
            w.writerow([t, typ, side, price, f"{size:.10f}", ag])
            rows += 1
    return rows


# =============================================================================
# simulated_events.csv generation
# =============================================================================

def generate_simulated_events_csv(
    events: np.ndarray,
    out_csv: str,
    ts_field: str,
    *,
    n_simulated: int,
    join_best_frac: float,
    qty_choices: Tuple[float, ...],
    horizon_choices: Tuple[int, ...],
    fixed_price_tick_offsets: Tuple[int, ...],
    seed: int,
    price_mult: float,
) -> int:
    """
    Samples placement times from ts_field.

    For fixed_price, uses px at sampled index converted to ticks (+/- offset).
    This avoids reconstructing best bid/ask here (candidate code does join_best).
    """
    random.seed(seed)

    ts = events[ts_field]
    px = events["px"]
    n = int(events.shape[0])
    if n < 10:
        raise SystemExit("Too few rows in input to generate simulated events.")

    n_simulated = min(n_simulated, max(1, n - 2))
    indices = sorted(random.sample(range(1, n - 1), k=n_simulated))

    outp = Path(out_csv)
    outp.parent.mkdir(parents=True, exist_ok=True)

    with open(out_csv, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["t0", "side", "qty", "horizon_ms", "price_mode", "price"])

        for idx in indices:
            t0 = int(ts[idx])
            side = "buy" if random.random() < 0.5 else "sell"
            qty_v = float(random.choice(qty_choices))
            horizon = int(random.choice(horizon_choices))

            if random.random() < join_best_frac:
                w.writerow([t0, side, f"{qty_v:.6f}", horizon, "join_best", 0])
            else:
                base_px_ticks = int(round(float(px[idx]) * price_mult))
                price = base_px_ticks + int(random.choice(fixed_price_tick_offsets))
                w.writerow([t0, side, f"{qty_v:.6f}", horizon, "fixed_price", price])

    return n_simulated


# =============================================================================
# CLI
# =============================================================================

def parse_float_tuple(s: str) -> Tuple[float, ...]:
    return tuple(float(x.strip()) for x in s.split(",") if x.strip())


def parse_int_tuple(s: str) -> Tuple[int, ...]:
    return tuple(int(x.strip()) for x in s.split(",") if x.strip())


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Convert structured HFTBacktest events (.npy or .npz) into market_events.csv and generate simulated_events.csv"
    )
    p.add_argument("--in", dest="inp", required=True, help="Input .npy (structured) or .npz containing structured array")
    p.add_argument("--events-key", default=None, help="If input is .npz, which member key holds the structured array")
    p.add_argument("--out-dir", default=".", help="Output directory")

    p.add_argument("--market-csv", default="market_events.csv")
    p.add_argument("--simulated-csv", default="simulated_events.csv")

    p.add_argument("--ts-field", default="exch_ts", choices=["exch_ts", "local_ts"],
                   help="Which timestamp field to use for output and simulation times")

    p.add_argument("--price-mult", type=float, default=1.0,
                   help="Convert float px -> integer ticks via round(px * price_mult). Example: 100 for cents.")

    p.add_argument("--skip-clear-events", action="store_true",
                   help="Skip DEPTH_CLEAR_EVENT rows (recommended).")

    # Simulated events knobs
    p.add_argument("--n-simulated", type=int, default=800)
    p.add_argument("--join-best-frac", type=float, default=0.8)
    p.add_argument("--seed", type=int, default=7)
    p.add_argument("--qty-choices", default="1,2,5")
    p.add_argument("--horizon-choices", default="250,500,1000,2000")
    p.add_argument("--fixed-price-offsets", default="-2,-1,0,1,2")

    # Diagnostics
    p.add_argument("--show-ev-hist", action="store_true", help="Print top ev codes + base types and exit")
    p.add_argument("--max-ev", type=int, default=40)
    return p.parse_args()


def main() -> int:
    args = parse_args()

    events = load_structured_array(args.inp, args.events_key)
    fields = list(events.dtype.fields.keys()) if events.dtype.fields else []
    required = {"ev", "exch_ts", "local_ts", "px", "qty"}
    missing = sorted(required - set(fields))
    if missing:
        raise SystemExit(f"Structured array missing required fields: {missing}. Fields present: {fields}")

    if args.show_ev_hist:
        hist = ev_histogram(events, max_unique=max(args.max_ev, 1))
        print("ev,count,base_type,has_buy,has_sell")
        for ev, cnt in hist[: args.max_ev]:
            base = decode_base_type(ev)
            print(f"{ev},{cnt},{base},{int(has_buy(ev))},{int(has_sell(ev))}")
        return 0

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    market_csv = str(out_dir / args.market_csv)
    sim_csv = str(out_dir / args.simulated_csv)

    rows = write_market_events_csv(
        events,
        out_csv=market_csv,
        ts_field=args.ts_field,
        price_mult=args.price_mult,
        skip_clear_events=args.skip_clear_events,
    )

    n_sim = generate_simulated_events_csv(
        events,
        out_csv=sim_csv,
        ts_field=args.ts_field,
        n_simulated=args.n_simulated,
        join_best_frac=args.join_best_frac,
        qty_choices=parse_float_tuple(args.qty_choices),
        horizon_choices=parse_int_tuple(args.horizon_choices),
        fixed_price_tick_offsets=parse_int_tuple(args.fixed_price_offsets),
        seed=args.seed,
        price_mult=args.price_mult,
    )

    print(f"Wrote market events: {market_csv} ({rows} rows)")
    print(f"Wrote simulated events: {sim_csv} ({n_sim} rows)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())