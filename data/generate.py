#!/usr/bin/env python3
from __future__ import annotations

import csv
import random
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple


# Market event types
ETYPE_BOOK = "book"
ETYPE_TRADE = "trade"

# Book sides
SIDE_BID = "B"   # bid book side (buy liquidity)
SIDE_ASK = "A"   # ask book side (sell liquidity)

# Trade aggressor side (who initiated)
AGGR_BUY = "B"   # buyer-initiated -> consumes asks
AGGR_SELL = "S"  # seller-initiated -> consumes bids


@dataclass
class Book:
    bids: Dict[int, float]
    asks: Dict[int, float]
    best_bid: int
    best_ask: int


def clamp(x: float, lo: float, hi: float) -> float:
    return lo if x < lo else hi if x > hi else x


def init_book(mid: int, spread_ticks: int = 2, levels: int = 30) -> Book:
    best_bid = mid - spread_ticks // 2
    best_ask = mid + spread_ticks // 2 + (spread_ticks % 2)

    bids: Dict[int, float] = {}
    asks: Dict[int, float] = {}

    # Depth: decreasing sizes away from top
    for i in range(levels):
        bids[best_bid - i] = max(1.0, 80.0 - 2.0 * i + random.random() * 5.0)
        asks[best_ask + i] = max(1.0, 80.0 - 2.0 * i + random.random() * 5.0)

    return Book(bids=bids, asks=asks, best_bid=best_bid, best_ask=best_ask)


def maybe_shift_mid(book: Book, p_shift: float = 0.002) -> None:
    # Occasionally move the whole book up/down by 1 tick (drift)
    if random.random() >= p_shift:
        return
    direction = random.choice([-1, +1])

    book.bids = {px + direction: sz for px, sz in book.bids.items()}
    book.asks = {px + direction: sz for px, sz in book.asks.items()}
    book.best_bid += direction
    book.best_ask += direction


def pick_level_near_top(best: int, side: str, max_depth: int = 8) -> int:
    # Choose a price level near the top (more updates close to top)
    d = int((random.random() ** 2) * max_depth)  # bias to smaller depths
    return best - d if side == SIDE_BID else best + d


def write_sample_data(
    out_dir: str = ".",
    n_market_events: int = 200_000,
    n_simulated_events: int = 400,
    seed: int = 7,
) -> Tuple[Path, Path]:
    """
    Writes:
      - market_events.csv: book updates + trades
      - simulated_events.csv: simulated passive order placements ("queries")
    """
    random.seed(seed)
    outp = Path(out_dir)
    outp.mkdir(parents=True, exist_ok=True)

    market_path = outp / "market_events.csv"
    sim_path = outp / "simulated_events.csv"

    # timestamps in ms
    ts = 0
    dt_min, dt_max = 1, 5

    book = init_book(mid=10_000, spread_ticks=2, levels=40)

    # Choose simulated placement times roughly throughout the stream.
    # Convert event-index to approximate time using avg dt ~3ms.
    sim_event_indices = sorted(random.sample(range(1, n_market_events - 1), k=n_simulated_events))
    sim_rows: List[Tuple[int, str, float, int, str, int]] = []

    for ei in sim_event_indices:
        t0 = ei * 3
        side = random.choice(["buy", "sell"])
        qty = random.choice([1.0, 2.0, 5.0])
        horizon = random.choice([250, 500, 1000, 2000])  # ms
        if random.random() < 0.75:
            price_mode = "join_best"
            price = 0
        else:
            price_mode = "fixed_price"
            # near mid
            price = 10_000 + random.choice([-2, -1, 0, 1, 2])
        sim_rows.append((t0, side, qty, horizon, price_mode, price))

    # Write market events
    with market_path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["ts", "type", "side", "price", "size", "trade_aggr_side"])

        for _ in range(n_market_events):
            ts += random.randint(dt_min, dt_max)

            maybe_shift_mid(book)

            r = random.random()
            if r < 0.70:
                # book update
                side = SIDE_BID if random.random() < 0.5 else SIDE_ASK
                px = pick_level_near_top(book.best_bid if side == SIDE_BID else book.best_ask, side)
                levels = book.bids if side == SIDE_BID else book.asks

                old = levels.get(px, 0.0)

                # update: sometimes cancel/remove, sometimes add
                if random.random() < 0.08:
                    new = 0.0
                else:
                    delta = random.gauss(0, 8)
                    new = clamp(old + delta, 0.0, 250.0)
                    if new < 0.5:
                        new = 0.0

                if new == 0.0:
                    levels.pop(px, None)
                else:
                    levels[px] = new

                # ensure top exists (simple stability)
                if side == SIDE_BID and book.best_bid not in book.bids:
                    book.bids[book.best_bid] = 50.0 + random.random() * 20.0
                if side == SIDE_ASK and book.best_ask not in book.asks:
                    book.asks[book.best_ask] = 50.0 + random.random() * 20.0

                w.writerow([ts, ETYPE_BOOK, side, px, f"{new:.6f}", ""])
            else:
                # trade near top; aggressor determines which side is consumed
                aggr = AGGR_SELL if random.random() < 0.5 else AGGR_BUY
                px = book.best_bid if aggr == AGGR_SELL else book.best_ask
                trade_sz = max(0.1, random.random() * 20.0)
                w.writerow([ts, ETYPE_TRADE, "T", px, f"{trade_sz:.6f}", aggr])

    # Write simulated order placements
    with sim_path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["t0", "side", "qty", "horizon_ms", "price_mode", "price"])
        for row in sim_rows:
            w.writerow(row)

    print(f"Wrote {market_path} ({n_market_events} rows)")
    print(f"Wrote {sim_path} ({n_simulated_events} rows)")
    return market_path, sim_path


if __name__ == "__main__":
    write_sample_data()