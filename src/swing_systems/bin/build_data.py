import argparse, sys, io
from datetime import date
from pathlib import Path
import pandas as pd
import yfinance as yf
import requests, yaml

def load_cfg(p):
    with open(p, "r") as f:
        return yaml.safe_load(f) or {}

def get_sp500_list() -> list[str]:
    url = "https://datahub.io/core/s-and-p-500-companies/r/constituents.csv"
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        df = pd.read_csv(io.StringIO(r.text))
        syms = (
            df["Symbol"]
            .astype(str)
            .str.strip()
            .str.replace(".", "-", regex=False)  # Yahoo-style tickers
            .unique()
            .tolist()
        )
        return sorted(syms)
    except Exception as e:
        print(f"Failed to fetch S&P 500 list: {e}", file=sys.stderr)
        return ["SPY"]  # safe fallback so the job still runs

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
    tickers = cfg.get("universe", [])
    if "__SP500__" in tickers:
        tickers = get_sp500_list()
    if not tickers:
        print("Universe empty. Check configs/universe.yaml.", file=sys.stderr)
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