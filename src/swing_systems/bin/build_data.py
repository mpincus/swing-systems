import argparse, sys, io, time, datetime as dt
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
    r = requests.get(url, timeout=15); r.raise_for_status()
    df = pd.read_csv(io.StringIO(r.text))
    return sorted(df["Symbol"].astype(str).str.replace(".", "-", regex=False).str.strip().unique().tolist())

def last_trading_day(utc_today: dt.date) -> dt.date:
    # simple clamp: Mon=0..Sun=6 → map Sat/Sun to prior Friday
    wd = utc_today.weekday()
    if wd == 5:  # Sat
        return utc_today - dt.timedelta(days=1)
    if wd == 6:  # Sun
        return utc_today - dt.timedelta(days=2)
    return utc_today

def dl_one(t, start, end):
    try:
        df = yf.download(t, start=start, end=end, interval="1d", auto_adjust=False, progress=False)
        if df.empty and start <= end:
            # fallback small period to tolerate exact-end boundary
            df = yf.download(t, period="5d", interval="1d", auto_adjust=False, progress=False)
        return df
    except Exception as e:
        print(f"Download error {t}: {e}", file=sys.stderr)
        return pd.DataFrame()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--universe", default="configs/universe.yaml")
    ap.add_argument("--start", default="2015-01-01")
    ap.add_argument("--end", default=str(date.today()))
    ap.add_argument("--dst", default=None)
    ap.add_argument("--batch", type=int, default=80)
    ap.add_argument("--sleep", type=float, default=0.25)
    args = ap.parse_args()

    cfg = load_cfg(args.universe)
    tickers = cfg.get("universe", [])
    if "__SP500__" in tickers:
        tickers = get_sp500_list()
    if not tickers:
        print("Universe empty.", file=sys.stderr); sys.exit(1)

    # clamp end to last trading day (UTC)
    utc_today = dt.date.fromisoformat(args.end) if args.end else date.today()
    end_day = last_trading_day(utc_today)

    out_path = Path(args.dst) if args.dst else Path(cfg.get("data_path", "data/combined.csv"))
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # load existing combined, robust dtypes
    have = pd.DataFrame()
    if out_path.exists():
        try:
            have = pd.read_csv(out_path, parse_dates=["Date"], low_memory=False,
                               dtype={"Ticker":"string"})
        except Exception as e:
            print(f"Warning: could not read existing combined.csv: {e}", file=sys.stderr)
            have = pd.DataFrame()
    have_syms = set(have["Ticker"].astype(str).unique()) if not have.empty else set()

    frames = []
    for i in range(0, len(tickers), args.batch):
        chunk = tickers[i:i+args.batch]
        print(f"Chunk {i//args.batch+1}/{(len(tickers)+args.batch-1)//args.batch}: {len(chunk)} tickers")
        for t in chunk:
            # compute incremental start
            start_day = dt.date.fromisoformat(args.start)
            if not have.empty and t in have_syms:
                last = have.loc[have["Ticker"] == t, "Date"].max()
                if pd.notna(last):
                    start_day = (pd.to_datetime(last).date() + dt.timedelta(days=1))
            if start_day > end_day:
                continue  # nothing to update
            df = dl_one(t, start_day.isoformat(), end_day.isoformat())
            time.sleep(args.sleep)
            if df.empty:
                continue
            df = df.reset_index().rename(columns={
                "Date":"Date","Open":"Open","High":"High","Low":"Low","Close":"Close","Volume":"Volume"
            })
            df["Ticker"] = t
            frames.append(df[["Date","Ticker","Open","High","Low","Close","Volume"]])

        # flush chunk
        if frames:
            new = pd.concat(frames, ignore_index=True)
            frames = []
            out = pd.concat([have, new], ignore_index=True) if not have.empty else new
            out = out.sort_values(["Ticker","Date"])
            out.to_csv(out_path, index=False)
            have = out
            print(f"Saved progress -> {out_path} rows={len(out)}")

    if have.empty:
        print("No data downloaded.", file=sys.stderr); sys.exit(1)
    print(f"Done. Final rows -> {len(have)} -> {out_path}")

if __name__ == "__main__":
    main()