#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from typing import Dict, Iterator, List, Optional, Tuple


# =============================================================================
# Data types
# =============================================================================

ETYPE_BOOK = "book"
ETYPE_TRADE = "trade"

SIDE_BID = "B"
SIDE_ASK = "A"

AGGR_BUY = "B"   # buyer-initiated -> consumes asks
AGGR_SELL = "S"  # seller-initiated -> consumes bids


@dataclass(frozen=True)
class MarketEvent:
    ts: int
    etype: str          # "book" | "trade"
    side: str           # "B"/"A" for book, "T" for trade
    price: int
    size: float
    trade_aggr_side: str  # "B"/"S" or ""


@dataclass(frozen=True)
class SimulatedEvent:
    # simulated passive order placement
    t0: int
    side: str           # "buy" | "sell"
    qty: float
    horizon_ms: int
    price_mode: str     # "join_best" | "fixed_price"
    price: int          # used for fixed_price


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
# Streaming IO (CSV)
# =============================================================================

def stream_market_events_csv(path: str, batch_rows: int = 65536) -> Iterator[list[MarketEvent]]:
    """
    Stream market events from CSV in batches.
    """
    batch: list[MarketEvent] = []
    with open(path, newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            ev = MarketEvent(
                ts=int(row["ts"]),
                etype=row["type"],
                side=row["side"],
                price=int(row["price"]),
                size=float(row["size"]),
                trade_aggr_side=row.get("trade_aggr_side", "") or "",
            )
            batch.append(ev)
            if len(batch) >= batch_rows:
                yield batch
                batch = []
    if batch:
        yield batch


def load_simulated_events_csv(path: str) -> list[SimulatedEvent]:
    """
    Load simulated events (order placements) and sort by t0.
    """
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
    out.sort(key=lambda q: q.t0)
    return out


# =============================================================================
# Interfaces candidate should implement
# =============================================================================

class OrderBook:
    """
    Candidate TODO:
      - Maintain per-side price->size levels efficiently.
      - Support fast updates.
      - Support best_bid/best_ask without scanning all levels each time.
      - Provide level_size(side, price).
    """

    def __init__(self) -> None:
        # TODO: implement
        raise NotImplementedError

    def update_level(self, side: str, price: int, size: float) -> None:
        # TODO: implement
        raise NotImplementedError

    def best_bid(self) -> Optional[Tuple[int, float]]:
        # TODO: return (price, size) or None
        raise NotImplementedError

    def best_ask(self) -> Optional[Tuple[int, float]]:
        # TODO: return (price, size) or None
        raise NotImplementedError

    def level_size(self, side: str, price: int) -> float:
        # TODO: implement
        raise NotImplementedError


class FillModel:
    """
    Candidate TODO:
      Implement a probabilistic queue/fill model.

    Requirements:
      - Estimate queue_ahead0 from displayed size at placement price.
      - Update queue position over time using:
          * trades at the order price
          * book size decreases at the order price (proxy for cancels ahead)
      - Output a probabilistic fill estimate (not just deterministic).

    Trade consumption rules:
      - BUY order rests on BID and is consumed by seller-initiated trades (aggr="S") at that price.
      - SELL order rests on ASK and is consumed by buyer-initiated trades (aggr="B") at that price.
    """

    def __init__(self, alpha: float = 0.7, gamma: float = 0.3, beta: float = 0.01) -> None:
        self.alpha = float(alpha)  # trade advancement
        self.gamma = float(gamma)  # cancel advancement
        self.beta = float(beta)    # sigmoid slope / softness

    def start_order(
        self,
        book: OrderBook,
        t0: int,
        side: str,      # "buy"|"sell"
        price: int,
        qty: float,
        horizon_ms: int,
    ) -> dict:
        """
        Return an opaque order-state dict.
        Must include at least:
          - t0, t_end
          - side, price, qty
          - queue_ahead0
          - queue_ahead (mutable)
        """
        # TODO: implement
        raise NotImplementedError

    def on_book_update(self, order_state: dict, event: MarketEvent, prev_level_size: float) -> None:
        """
        Called when a book update happens at the order's (book side, price).
        prev_level_size is the size BEFORE applying the update.
        """
        # TODO: implement
        raise NotImplementedError

    def on_trade(self, order_state: dict, event: MarketEvent) -> None:
        """
        Called when a trade happens at the order price.
        Use event.trade_aggr_side ("S" or "B") to decide whether it consumes this order's queue.
        """
        # TODO: implement
        raise NotImplementedError

    def finalize(self, order_state: dict) -> Tuple[float, float]:
        """
        Return (p_fill, expected_fill_qty)
        """
        # TODO: implement
        raise NotImplementedError


class Engine:
    """
    Single pass over market events while handling many simulated order placements.
    """

    def __init__(self, book: OrderBook, fill_model: FillModel) -> None:
        self.book = book
        self.fill_model = fill_model

        # Active orders keyed by (order_side, price) where order_side in {"buy","sell"}
        self.active: Dict[Tuple[str, int], List[dict]] = {}
        self.results: List[Result] = []

    def _activate_simulated(self, s: SimulatedEvent) -> None:
        # Determine price
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
        else:
            price = s.price

        st = self.fill_model.start_order(
            book=self.book,
            t0=s.t0,
            side=s.side,
            price=price,
            qty=s.qty,
            horizon_ms=s.horizon_ms,
        )
        self.active.setdefault((s.side, price), []).append(st)

    def _expire_orders(self, now_ts: int) -> None:
        dead_keys: list[Tuple[str, int]] = []
        for key, orders in self.active.items():
            alive: list[dict] = []
            for st in orders:
                if now_ts >= st["t_end"]:
                    p_fill, exp_qty = self.fill_model.finalize(st)
                    self.results.append(Result(
                        t0=int(st["t0"]),
                        side=str(st["side"]),
                        price=int(st["price"]),
                        qty=float(st["qty"]),
                        horizon_ms=int(st["t_end"] - st["t0"]),
                        queue_ahead0=float(st["queue_ahead0"]),
                        queue_ahead_end=float(st["queue_ahead"]),
                        p_fill=float(p_fill),
                        expected_fill_qty=float(exp_qty),
                    ))
                else:
                    alive.append(st)

            if alive:
                self.active[key] = alive
            else:
                dead_keys.append(key)

        for k in dead_keys:
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
                # Activate simulated placements whose t0 <= current market timestamp
                while si < sn and simulated[si].t0 <= ev.ts:
                    self._activate_simulated(simulated[si])
                    si += 1

                # Expire orders that have passed horizon
                self._expire_orders(ev.ts)

                if ev.etype == ETYPE_BOOK:
                    # capture prev size for cancel inference
                    prev = self.book.level_size(ev.side, ev.price)
                    self.book.update_level(ev.side, ev.price, ev.size)

                    # Which order side rests on this book side?
                    # buy orders rest on BID; sell orders rest on ASK
                    order_side = "buy" if ev.side == SIDE_BID else "sell"
                    key = (order_side, ev.price)
                    if key in self.active:
                        for st in self.active[key]:
                            self.fill_model.on_book_update(st, ev, prev_level_size=prev)

                elif ev.etype == ETYPE_TRADE:
                    # Trades can impact orders at that price; FillModel decides if it consumes.
                    for order_side in ("buy", "sell"):
                        key = (order_side, ev.price)
                        if key in self.active:
                            for st in self.active[key]:
                                self.fill_model.on_trade(st, ev)

        # Flush remaining orders
        self._expire_orders(now_ts=2**63 - 1)
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
    p = argparse.ArgumentParser(description="Probabilistic queue/fill onsite challenge (CSV)")
    p.add_argument("--market-events", required=True, help="Path to market_events.csv (book + trades)")
    p.add_argument("--simulated-events", required=True, help="Path to simulated_events.csv (simulated placements)")
    p.add_argument("--out", required=True, help="Path to results.csv")
    p.add_argument("--batch-rows", type=int, default=65536, help="Streaming batch size")
    p.add_argument("--alpha", type=float, default=0.7)
    p.add_argument("--gamma", type=float, default=0.3)
    p.add_argument("--beta", type=float, default=0.01)
    return p.parse_args()


def main() -> int:
    args = parse_args()

    # Candidate implements these
    book = OrderBook()
    model = FillModel(alpha=args.alpha, gamma=args.gamma, beta=args.beta)

    simulated = load_simulated_events_csv(args.simulated_events)
    market_batches = stream_market_events_csv(args.market_events, batch_rows=args.batch_rows)

    engine = Engine(book=book, fill_model=model)
    results = engine.run(market_batches, simulated)

    write_results_csv(args.out, results)
    print(f"Wrote {len(results)} results to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
