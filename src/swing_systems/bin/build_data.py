import argparse, sys
from datetime import date
from pathlib import Path
import pandas as pd
import yfinance as yf
import yaml

def load_cfg(p):
    with open(p, "r") as f:
        return yaml.safe_load(f) or {}

def get_universe(mode: str, tickers: list[str]) -> list[str]:
    mode = (mode or "list").lower()
    if mode == "sp500":
        # Wikipedia S&P 500 constituents
        tables = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")
        df = tables[0]
        return sorted(df["Symbol"].astype(str).str.replace(".", "-", regex=False).unique().tolist())
    if mode == "r1000":
        # Wikipedia Russell 1000 (current list page)
        tables = pd.read_html("https://en.wikipedia.org/wiki/Russell_1000_Index")
        # first table with symbols
        df = max(tables, key=lambda t: t.shape[0])
        # try common symbol column names
        for col in ["Ticker", "Symbol", "Ticker symbol"]:
            if col in df.columns:
                return sorted(df[col].astype(str).str.replace(".", "-", regex=False).unique().tolist())
        return []
    # default: user-specified list
    return tickers or []

def dl(t, start, end):
    df = yf.download(t, start=start, end=end, interval="1d", auto_adjust=False, progress=False)
    if df.empty:
        df = yf.download(t, period="10y", interval="1d", auto_adjust=False, progress=False)
    return df

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--universe", default="configs/universe.yaml")
    ap.add_argument("--start", default="2015-01-01")
    ap.add_argument("--end", default=str(date.today()))
    ap.add_argument("--dst", default=None)
    args = ap.parse_args()

    cfg = load_cfg(args.universe)
    mode = cfg.get("universe_mode", "list")
    tickers = get_universe(mode, cfg.get("universe", []))
    if not tickers:
        print("Universe empty. Set universe_mode to sp500/r1000 or provide tickers.", file=sys.stderr)
        sys.exit(1)
    print(f"Universe size: {len(tickers)}")

    out_path = Path(args.dst) if args.dst else Path(cfg.get("data_path", "data/combined.csv"))
    out_path.parent.mkdir(parents=True, exist_ok=True)

    frames = []
    for t in tickers:
        df = dl(t, args.start, args.end)
        if df.empty:
            continue
        df = df.reset_index().rename(columns={
            "Date":"Date","Open":"Open","High":"High","Low":"Low","Close":"Close","Volume":"Volume"
        })
        df["Ticker"] = t
        frames.append(df[["Date","Ticker","Open","High","Low","Close","Volume"]])

    if not frames:
        print("No data downloaded.", file=sys.stderr); sys.exit(1)

    out = pd.concat(frames, ignore_index=True).sort_values(["Ticker","Date"])
    out.to_csv(out_path, index=False)
    print(f"Wrote {len(out):,} rows -> {out_path}")

if __name__ == "__main__":
    main()