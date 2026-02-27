#!/usr/bin/env bash
set -e

# Set up Python virtual environment
python3 -m venv .venv
source .venv/bin/activate

pip install -r requirements.txt


python3 src/main.py --market-events data/canned/market_events.csv --simulated-events data/canned/simulated_events.csv --out results.csv