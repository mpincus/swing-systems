import argparse
import sys
import io
import time
import datetime as dt
from datetime import date
from pathlib import Path
import pandas as pd
import yfinance as yf
import requests
import yaml


# ---------- HELPERS ----------

def load_cfg(p):
    with open(p, "r") as f:
        return yaml.safe_load(f) or {}


def get_sp500_list():
    url = "https://datahub.io/core/s-and-p-500-companies/r/constituents.csv"
    r = requests.get(url, timeout=15)
    r.raise_for_status()
    df = pd.read_csv(io.StringIO(r.text))
    return sorted(
        df["Symbol"].astype(str).str.replace(".", "-", regex=False).str.strip().unique().tolist()
    )


def last_trading_day(d: dt.date) -> dt.date:
    wd = d.weekday()
    if wd == 5:  # Saturday
        return d - dt.timedelta(days=1)
    if wd == 6:  # Sunday
        return d - dt.timedelta(days=2)
    return d


def quick_vol(tickers):
    """5-day average volume prefilter."""
    out = []
    for t in tickers:
        df = yf.download(t, period="5d", interval="1d", progress=False)
        if df.empty:
            continue
        out.append((t, float(df["Volume"].tail(5).mean())))
    out.sort(key=lambda x: x[1], reverse=True)
    return [t for t, _ in out]


def dl_chunk_multi(tickers, start, end):
    """Batch download multiple tickers at once for speed."""
    data = yf.download(" ".join(tickers), start=start, end=end, interval="1d",
                       auto_adjust=False, progress=False, group_by="ticker")
    if data.empty:
        return pd.DataFrame(columns=["Date", "Ticker", "Open", "High", "Low", "Close", "Volume"])
    if isinstance(data.columns, pd.MultiIndex):
        frames = []
        for t in tickers:
            if t not in data.columns.levels[0]:
                continue
            g = data[t].reset_index()
            g["Ticker"] = t
            frames.append(g[["Date", "Ticker", "Open", "High", "Low", "Close", "Volume"]])
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    g = data.reset_index()
    g["Ticker"] = tickers[0]
    return g[["Date", "Ticker", "Open", "High", "Low", "Close", "Volume"]]


# ---------- MAIN ----------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--universe", default="configs/universe.yaml")
    ap.add_argument("--start", default="2015-01-01")
    ap.add_argument("--end", default=str(date.today()))
    ap.add_argument("--dst", default=None)
    ap.add_argument("--batch", type=int, default=100)
    ap.add_argument("--sleep", type=float, default=0.10)
    ap.add_argument("--top", type=int, default=0, help="keep top-N by 5d avg volume")
    ap.add_argument("--multi", action="store_true", help="use multi-ticker downloads")
    args = ap.parse_args()

    cfg = load_cfg(args.universe)
    tickers = cfg.get("universe", [])
    if "__SP500__" in tickers:
        tickers = get_sp500_list()
    if not tickers:
        print("Universe empty.", file=sys.stderr)
        sys.exit(1)

    if args.top and len(tickers) > args.top:
        print(f"Prefiltering top {args.top} by 5-day avg volume …")
        tickers = quick_vol(tickers)[:args.top]
        print(f"Using top {len(tickers)} liquid tickers.")

    end_day = last_trading_day(dt.date.fromisoformat(args.end))
    out_path = Path(args.dst) if args.dst else Path(cfg.get("data_path", "data/combined.csv"))
    out_path.parent.mkdir(parents=True, exist_ok=True)

    have = pd.DataFrame()
    if out_path.exists():
        try:
            have = pd.read_csv(out_path, parse_dates=["Date"], low_memory=False, dtype={"Ticker": "string"})
        except Exception as e:
            print(f"Warning: could not read existing combined.csv: {e}", file=sys.stderr)
            have = pd.DataFrame()

    frames = []
    for i in range(0, len(tickers), args.batch):
        chunk = tickers[i:i + args.batch]
        print(f"Batch {i // args.batch + 1}/{(len(tickers) + args.batch - 1) // args.batch} — {len(chunk)} tickers")

        if args.multi:
            dfc = dl_chunk_multi(chunk, args.start, end_day.isoformat())
            if not dfc.empty:
                frames.append(dfc)
            time.sleep(args.sleep)
        else:
            for t in chunk:
                df = yf.download(t, start=args.start, end=end_day.isoformat(),
                                 interval="1d", auto_adjust=False, progress=False)
                if df.empty:
                    time.sleep(args.sleep)
                    continue
                g = df.reset_index()
                g["Ticker"] = t
                frames.append(g[["Date", "Ticker", "Open", "High", "Low", "Close", "Volume"]])
                time.sleep(args.sleep)

        if frames:
            new = pd.concat(frames, ignore_index=True)
            frames = []
            if not have.empty:
                combined = pd.concat([have, new], ignore_index=True)
            else:
                combined = new
            combined = (
                combined.sort_values(["Ticker", "Date"])
                        .drop_duplicates(subset=["Ticker", "Date"], keep="last")
            )
            combined.to_csv(out_path, index=False)
            have = combined
            print(f"Saved -> {out_path} rows={len(combined)}")

    if not out_path.exists() or out_path.stat().st_size == 0:
        print("No data downloaded.", file=sys.stderr)
        sys.exit(1)

    print(f"Done. Final rows={len(have)} -> {out_path}")


if __name__ == "__main__":
    main()