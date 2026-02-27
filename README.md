# Probabilistic Queue / Fill Model — Onsite Python Challenge (60–90 min)

## What you are building
You are given:
- `market_events.csv`: market tape derived from a real capture (depth + trades)
- `simulated_events.csv`: **our simulated passive order placements** we want to evaluate against the tape

Your task is to implement:
1) An efficient `OrderBook` data structure.
2) A probabilistic `FillModel` that estimates queue position and outputs a rough fill probability.

You will then present your solution and reasoning.

---

## Evaluation criteria
We are testing:
1) **Python code quality**: structure, readability, correctness, edge cases.
2) **Algorithmic complexity**: you should choose data structures that can handle millions of events without scanning the whole book each update.
3) **Microstructure reasoning**: estimate rough queue position at a price level and compute a **probabilistic** fill estimate.

---

## Inputs

### `market_events.csv`
Columns:
- `ts` (int): timestamp (ms or ns depending on generation)
- `type` (str): `book` or `trade`
- `side` (str):
  - book: `B` (bid) or `A` (ask)
  - trade: `T`
- `price` (int): integer ticks
- `size` (float):
  - book: **absolute size at that level after update** (0 means remove)
  - trade: trade quantity
- `trade_aggr_side` (str, trades only): `S` (seller-initiated) or `B` (buyer-initiated) or empty if unknown

### `simulated_events.csv`
Columns:
- `t0` (int): placement time
- `side` (str): `buy` or `sell`
- `qty` (float): order quantity
- `horizon_ms` (int): how long the order rests before we stop
- `price_mode` (str): `join_best` or `fixed_price`
- `price` (int): ignored when `join_best`, otherwise fixed tick price

---

## Output

Write `results.csv` containing one row per simulated placement:
- `queue_ahead0`: queue-ahead estimate at placement time
- `queue_ahead_end`: queue-ahead estimate after processing until horizon
- `p_fill`: **probabilistic** estimate of being filled within horizon
- `expected_fill_qty`: expected fill quantity (simple is `p_fill * qty`)

---

## How to run

python main.py \
  --market-events market_events.csv \
  --simulated-events simulated_events.csv \
  --out results.csv

The scaffold already:
* streams market events in batches
* activates simulated placements when t0 <= current_ts
* expires them when current_ts >= t_end
* routes only relevant updates/trades to active orders by (side, price)

You should not rewrite the whole engine unless you want to improve it.

## Required implementation details (what you must code)

### Part A — Implement OrderBook

You must implement the following methods in OrderBook:

* update_level(side, price, size)
* best_bid() -> Optional[(price, size)]
* best_ask() -> Optional[(price, size)]
* level_size(side, price) -> float

Constraints:

* Must handle large event streams efficiently (millions of updates).
* Avoid scanning all price levels every time you want best bid/ask.
* Must correctly handle removing levels (size == 0).

What we’ll look for:

* Use of appropriate structures (e.g., dict + heap, or sorted structure, etc.)
* Clear explanation of tradeoffs (average-case vs worst-case)

### Part B — Implement FillModel (probabilistic queue / fill estimation)

You must implement:

* start_order(book, simulated_event, price) -> dict
* on_book_update(order_state, market_event, prev_level_size)
* on_trade(order_state, market_event)
* finalize(order_state) -> (p_fill, expected_fill_qty)

Required queue logic

At placement (start_order):

Determine the relevant book side:

buy order rests on the bid book

sell order rests on the ask book

Estimate initial queue ahead:

queue_ahead0 = book.level_size(relevant_side, price)

store queue_ahead = queue_ahead0 (mutable)

During the order’s life (on_book_update, on_trade) until t_end = t0 + horizon_ms:

Trades at the order price should advance queue position depending on aggressor:

Buy order is consumed by seller-initiated trades (trade_aggr_side="S")

Sell order is consumed by buyer-initiated trades (trade_aggr_side="B")

Book size decreases at the order’s level can be treated as cancellations “ahead of you” and reduce queue_ahead.

You must incorporate probability:

p_fill cannot be purely deterministic unless you explicitly turn it into a probability curve.

Acceptable: logistic/sigmoid mapping, hazard model, or another defensible probabilistic approach.

Suggested parameterization (use or propose your own)

alpha ∈ [0,1]: trade advancement factor
Example: queue_ahead -= alpha * trade_size when the trade consumes your side.

gamma ∈ [0,1]: cancel advancement factor
Example: if level size drops by d, then queue_ahead -= gamma * d.

beta > 0: sigmoid slope/softness
Example: p_fill = sigmoid(beta * (-queue_ahead_end))

Expected fill quantity:

simplest: expected_fill_qty = p_fill * qty

more nuanced is okay if explained.

Performance requirements / constraints

Assume market_events.csv may contain millions of rows.

Avoid any approach that replays the entire market tape per simulated order (O(N·Q)).

Your book operations should be efficient; talk about complexity of:

per update

best bid/ask retrieval

memory usage

### What to present (15–30 minutes)

Be ready to walk through:

OrderBook design

data structures chosen

time complexity

handling of best bid/ask updates and removals

Fill model design

what queue_ahead represents

how trades/cancels advance the queue

why your probability mapping makes sense

role of alpha/gamma/beta (or your parameters)

Sanity checks

show a few example outputs from results.csv

explain why they’re plausible

note obvious limitations / assumptions

Extensions

what would improve with richer data (L3 / matching engine events)

how latency or priority rules would change the model