#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from typing import Dict, Iterator, List, Optional, Tuple


# =============================================================================
# Market tape CSV schema
# =============================================================================
# market_events.csv columns:
#   ts,type,side,price,size,trade_aggr_side
#
# type: "book" | "trade"
# side:
#   - for book: "B" (bid) or "A" (ask)
#   - for trade: "T"
# trade_aggr_side (trades only): "B" (buyer-initiated) or "S" (seller-initiated) or ""


ETYPE_BOOK = "book"
ETYPE_TRADE = "trade"

SIDE_BID = "B"
SIDE_ASK = "A"

AGGR_BUY = "B"   # buyer-initiated -> consumes asks
AGGR_SELL = "S"  # seller-initiated -> consumes bids


# =============================================================================
# Simulated placements CSV schema
# =============================================================================
# simulated_events.csv columns:
#   t0,side,qty,horizon_ms,price_mode,price
#
# side: "buy" | "sell"
# price_mode: "join_best" | "fixed_price"
# price: ignored for join_best, integer ticks for fixed_price


@dataclass(frozen=True)
class MarketEvent:
    ts: int
    etype: str
    side: str
    price: int
    size: float
    trade_aggr_side: str


@dataclass(frozen=True)
class SimulatedEvent:
    t0: int
    side: str
    qty: float
    horizon_ms: int
    price_mode: str
    price: int

# Example structure
@dataclass
class Result:
    t0: int
    side: str
    price: int
    qty: float
    horizon_ms: int
    queue_ahead0: float
    queue_ahead_end: float
    p_fill: float
    expected_fill_qty: float


# =============================================================================
# Streaming IO
# =============================================================================

def stream_market_events_csv(path: str, batch_rows: int = 100_000) -> Iterator[list[MarketEvent]]:
    batch: list[MarketEvent] = []
    with open(path, newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            batch.append(MarketEvent(
                ts=int(row["ts"]),
                etype=row["type"],
                side=row["side"],
                price=int(row["price"]),
                size=float(row["size"]),
                trade_aggr_side=row.get("trade_aggr_side", "") or "",
            ))
            if len(batch) >= batch_rows:
                yield batch
                batch = []
    if batch:
        yield batch


def load_simulated_events_csv(path: str) -> list[SimulatedEvent]:
    out: list[SimulatedEvent] = []
    with open(path, newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            out.append(SimulatedEvent(
                t0=int(row["t0"]),
                side=row["side"],
                qty=float(row["qty"]),
                horizon_ms=int(row["horizon_ms"]),
                price_mode=row["price_mode"],
                price=int(row["price"]),
            ))
    out.sort(key=lambda x: x.t0)
    return out


# =============================================================================
# Interfaces candidate must implement
# =============================================================================

class OrderBook:
    """
    Candidate TODO:
      - Maintain per-side price->size levels efficiently.
      - Support fast updates.
      - Support best_bid/best_ask without scanning all levels each time.
      - Provide level_size(side, price).

    Book updates are ABSOLUTE sizes at that price level.
    size=0 means remove the level.
    """

    def __init__(self) -> None:
        # TODO
        raise NotImplementedError

    def update_level(self, side: str, price: int, size: float) -> None:
        # TODO
        raise NotImplementedError

    def best_bid(self) -> Optional[Tuple[int, float]]:
        # TODO: (price, size) or None
        raise NotImplementedError

    def best_ask(self) -> Optional[Tuple[int, float]]:
        # TODO: (price, size) or None
        raise NotImplementedError

    def level_size(self, side: str, price: int) -> float:
        # TODO
        raise NotImplementedError


class FillModel:
    """
    Candidate TODO: implement a probabilistic queue/fill model.

    Required behavior:
      - On start_order:
          * compute queue_ahead0 from displayed size at the order's price on the relevant book side
          * set queue_ahead = queue_ahead0 (mutable)
      - On updates until t_end = t0 + horizon_ms:
          * trades at that price should advance queue_ahead depending on aggressor:
              BUY order rests on BID and is consumed by seller-initiated trades (aggr="S")
              SELL order rests on ASK and is consumed by buyer-initiated trades (aggr="B")
          * book size decreases at that level can be treated as cancels ahead and advance queue_ahead
      - finalize() returns:
          * p_fill: probabilistic estimate of full fill within horizon (NOT just deterministic)
          * expected_fill_qty: simple is p_fill * qty (or more nuanced)

    Suggested parameters:
      - alpha (0..1): trade advancement factor
      - gamma (0..1): cancel advancement factor
      - beta (>0): softness/slope for sigmoid
    """

    def __init__(self, alpha: float = 0.7, gamma: float = 0.3, beta: float = 0.01) -> None:
        self.alpha = float(alpha)
        self.gamma = float(gamma)
        self.beta = float(beta)

    def start_order(self, book: OrderBook, s: SimulatedEvent, price: int) -> dict:
        """
        Return an opaque mutable state dict. Must include:
          t0, t_end, side, price, qty, queue_ahead0, queue_ahead
        """
        # TODO
        raise NotImplementedError

    def on_book_update(self, st: dict, ev: MarketEvent, prev_level_size: float) -> None:
        """
        Called when a book update occurs at (relevant side, price).
        prev_level_size is the size before applying the update.
        """
        # TODO
        raise NotImplementedError

    def on_trade(self, st: dict, ev: MarketEvent) -> None:
        """
        Called when a trade occurs at st["price"].
        Use ev.trade_aggr_side ("S"/"B"/"") to decide if it consumes.
        """
        # TODO
        raise NotImplementedError

    def finalize(self, st: dict) -> Tuple[float, float]:
        """
        Return (p_fill, expected_fill_qty).
        """
        # TODO
        raise NotImplementedError


# =============================================================================
# Single-pass engine
# =============================================================================

class Engine:
    """
    Single pass over market_events.csv while activating simulated placements from simulated_events.csv.

    Active orders are keyed by (order_side, price) where order_side in {"buy","sell"}
    so we can route relevant book/trade events efficiently.
    """

    def __init__(self, book: OrderBook, model: FillModel) -> None:
        self.book = book
        self.model = model
        self.active: Dict[Tuple[str, int], List[dict]] = {}
        self.results: List[Result] = []

    def _activate(self, s: SimulatedEvent) -> None:
        if s.price_mode == "join_best":
            if s.side == "buy":
                bb = self.book.best_bid()
                if bb is None:
                    return
                price = bb[0]
            else:
                ba = self.book.best_ask()
                if ba is None:
                    return
                price = ba[0]
        elif s.price_mode == "fixed_price":
            price = s.price
        else:
            return

        st = self.model.start_order(self.book, s, price)
        self.active.setdefault((s.side, price), []).append(st)

    def _expire(self, now_ts: int) -> None:
        dead: list[Tuple[str, int]] = []
        for key, states in self.active.items():
            keep: list[dict] = []
            for st in states:
                if now_ts >= st["t_end"]:
                    p, exp_qty = self.model.finalize(st)
                    self.results.append(Result(
                        t0=int(st["t0"]),
                        side=str(st["side"]),
                        price=int(st["price"]),
                        qty=float(st["qty"]),
                        horizon_ms=int(st["t_end"] - st["t0"]),
                        queue_ahead0=float(st["queue_ahead0"]),
                        queue_ahead_end=float(st["queue_ahead"]),
                        p_fill=float(p),
                        expected_fill_qty=float(exp_qty),
                    ))
                else:
                    keep.append(st)
            if keep:
                self.active[key] = keep
            else:
                dead.append(key)
        for k in dead:
            self.active.pop(k, None)

    def run(
        self,
        market_batches: Iterator[list[MarketEvent]],
        simulated: list[SimulatedEvent],
    ) -> List[Result]:
        si = 0
        sn = len(simulated)

        for batch in market_batches:
            for ev in batch:
                while si < sn and simulated[si].t0 <= ev.ts:
                    self._activate(simulated[si])
                    si += 1

                self._expire(ev.ts)

                if ev.etype == ETYPE_BOOK:
                    prev = self.book.level_size(ev.side, ev.price)
                    self.book.update_level(ev.side, ev.price, ev.size)

                    # buy orders rest on BID, sell orders rest on ASK
                    order_side = "buy" if ev.side == SIDE_BID else "sell"
                    key = (order_side, ev.price)
                    if key in self.active:
                        for st in self.active[key]:
                            self.model.on_book_update(st, ev, prev_level_size=prev)

                elif ev.etype == ETYPE_TRADE:
                    # trades may impact both buy/sell orders at that price
                    for order_side in ("buy", "sell"):
                        key = (order_side, ev.price)
                        if key in self.active:
                            for st in self.active[key]:
                                self.model.on_trade(st, ev)

        # flush remaining
        self._expire(2**63 - 1)
        return self.results


# =============================================================================
# Output
# =============================================================================

def write_results_csv(path: str, results: List[Result]) -> None:
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow([
            "t0", "side", "price", "qty", "horizon_ms",
            "queue_ahead0", "queue_ahead_end", "p_fill", "expected_fill_qty",
        ])
        for r in results:
            w.writerow([
                r.t0, r.side, r.price, f"{r.qty:.6f}", r.horizon_ms,
                f"{r.queue_ahead0:.6f}", f"{r.queue_ahead_end:.6f}",
                f"{r.p_fill:.6f}", f"{r.expected_fill_qty:.6f}",
            ])


# =============================================================================
# CLI
# =============================================================================

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Onsite: probabilistic queue/fill model challenge")
    p.add_argument("--market-events", required=True, help="market_events.csv (book + trade)")
    p.add_argument("--simulated-events", required=True, help="simulated_events.csv (our placements)")
    p.add_argument("--out", required=True, help="results.csv")
    p.add_argument("--batch-rows", type=int, default=100_000, help="Market stream batch size")
    p.add_argument("--alpha", type=float, default=0.7)
    p.add_argument("--gamma", type=float, default=0.3)
    p.add_argument("--beta", type=float, default=0.01)
    return p.parse_args()


def main() -> int:
    args = parse_args()

    # Candidate implements these:
    book = OrderBook()
    model = FillModel(alpha=args.alpha, gamma=args.gamma, beta=args.beta)

    simulated = load_simulated_events_csv(args.simulated_events)
    market_batches = stream_market_events_csv(args.market_events, batch_rows=args.batch_rows)

    engine = Engine(book, model)
    results = engine.run(market_batches, simulated)

    write_results_csv(args.out, results)
    print(f"Wrote {len(results)} rows to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())