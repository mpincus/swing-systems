# swing-systems

Automated daily data build + four scanners, with **per-strategy watchlists** built from a seed universe.
- Seed universe: `configs/seed_universe.txt` (one ticker per line).
- Watchlists are generated daily into `configs/generated/*.txt`.
- Data is downloaded only for those watchlist tickers.
- Outputs in `outputs/**`, ledgers in `state/**`.