# Probabilistic Queue / Fill Model — Onsite Python Challenge (60–90 min)

## Goal
Implement an efficient order book + a probabilistic queue/fill estimator, then present your design.

We evaluate:
1) Python code quality (structure, readability, correctness).
2) Algorithmic complexity awareness in data-structure choices for high event rates.
3) Queue position estimation and rough probabilistic fill estimates for passive orders.

---

## Provided files
- `main.py` — scaffold + interfaces you must implement (`OrderBook`, `FillModel`)
- `generate_sample_data.py` — generates `market_events.csv` and `simulated_events.csv`

---

## Quick start

Generate sample data:
```bash
python main.py \
  --market-events market_events.csv \
  --simulated-events simulated_events.csv \
  --out results.csv
```