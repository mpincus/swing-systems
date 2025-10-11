import argparse, sys, io, time
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

def dl_one(t, start, end):
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
    ap.add_argument("--batch", type=int, default=80)     # tickers per chunk
    ap.add_argument("--sleep", type=float, default=0.25) # seconds between calls
    args = ap.parse_args()

    cfg = load_cfg(args.universe)
    tickers = cfg.get("universe", [])
    if "__SP500__" in tickers:
        tickers = get_sp500_list()
    if not tickers:
        print("Universe empty.", file=sys.stderr); sys.exit(1)

    out_path = Path(args.dst) if args.dst else Path(cfg.get("data_path", "data/combined.csv"))
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Resume: load existing combined.csv to avoid re-downloading
    have = pd.DataFrame()
    if out_path.exists():
        try:
            have = pd.read_csv(out_path, parse_dates=["Date"])
        except Exception:
            have = pd.DataFrame()
    have_syms = set(have["Ticker"].unique()) if not have.empty else set()

    frames = []
    for i in range(0, len(tickers), args.batch):
        chunk = tickers[i:i+args.batch]
        print(f"Chunk {i//args.batch+1}: {len(chunk)} tickers")
        for t in chunk:
            # incremental: fetch only missing range if we already have history
            start = args.start
            if not have.empty and t in have_syms:
                last = have.loc[have["Ticker"]==t, "Date"].max()
                if pd.notna(last):
                    start = (pd.to_datetime(last) + pd.Timedelta(days=1)).date().isoformat()
            df = dl_one(t, start, args.end)
            time.sleep(args.sleep)
            if df.empty: 
                continue
            df = df.reset_index().rename(columns={"Date":"Date","Open":"Open","High":"High","Low":"Low","Close":"Close","Volume":"Volume"})
            df["Ticker"] = t
            frames.append(df[["Date","Ticker","Open","High","Low","Close","Volume"]])

        # flush each chunk to reduce memory and preserve progress
        if frames:
            new = pd.concat(frames, ignore_index=True)
            frames = []
            if not have.empty:
                out = pd.concat([have, new], ignore_index=True)
            else:
                out = new
            out = out.sort_values(["Ticker","Date"])
            out.to_csv(out_path, index=False)
            have = out  # update baseline
            print(f"Saved progress -> {out_path} rows={len(out)}")

    if have.empty:
        print("No data downloaded.", file=sys.stderr); sys.exit(1)
    print(f"Done. Final rows -> {len(have)} -> {out_path}")

if __name__ == "__main__":
    main()