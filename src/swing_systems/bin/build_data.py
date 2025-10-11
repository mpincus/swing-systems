import argparse, sys
from datetime import date
from pathlib import Path
import pandas as pd
import yfinance as yf
import yaml


def load_cfg(p):
    with open(p, "r") as f:
        return yaml.safe_load(f)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--universe", default="configs/universe.yaml")
    ap.add_argument("--start", default="2015-01-01")
    ap.add_argument("--end", default=str(date.today()))
    ap.add_argument("--dst", default=None)
    args = ap.parse_args()

    uni = load_cfg(args.universe)
    tickers = uni.get("universe", [])
    out_path = Path(args.dst) if args.dst else Path(uni.get("data_path", "data/combined.csv"))
    out_path.parent.mkdir(parents=True, exist_ok=True)

    frames = []
    for t in tickers:
        print(f"Downloading {t} ...")
        df = yf.download(t, start=args.start, end=args.end, interval="1d", auto_adjust=False, progress=False)
        if df.empty:
            print(f"Warning: no data for {t}")
            continue

        df = df.reset_index().rename(columns={
            "Date": "Date",
            "Open": "Open",
            "High": "High",
            "Low": "Low",
            "Close": "Close",
            "Volume": "Volume"
        })

        df["Ticker"] = t
        frames.append(df[["Date", "Ticker", "Open", "High", "Low", "Close", "Volume"]])

    if not frames:
        print("No data downloaded. Check tickers or dates.", file=sys.stderr)
        sys.exit(1)

    out = pd.concat(frames, ignore_index=True).sort_values(["Ticker", "Date"])
    out.to_csv(out_path, index=False)
    print(f"Wrote {len(out):,} rows -> {out_path}")


if __name__ == "__main__":
    main()