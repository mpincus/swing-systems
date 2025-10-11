import argparse
from pathlib import Path
from datetime import date
import pandas as pd
import yfinance as yf

def load_tickers(path: str) -> list[str]:
    p = Path(path)
    if not p.exists():
        return []
    return [l.strip().upper() for l in p.read_text().splitlines() if l.strip()]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tickers-file", default="configs/generated/all_tickers.txt")
    ap.add_argument("--start", default="2015-01-01")
    ap.add_argument("--end", default=str(date.today()))
    ap.add_argument("--dst", default="data/combined.csv")
    args = ap.parse_args()

    tickers = load_tickers(args.tickers_file)
    Path(args.dst).parent.mkdir(parents=True, exist_ok=True)

    frames = []
    for t in tickers:
        df = yf.download(t, start=args.start, end=args.end, interval="1d", auto_adjust=False, progress=False)
        if df.empty: 
            continue
        df = df.reset_index().rename(columns={
            "Date":"Date","Open":"Open","High":"High","Low":"Low","Close":"Close","Adj Close":"Adj Close","Volume":"Volume"
        })
        df["Ticker"] = t
        frames.append(df[["Date","Ticker","Open","High","Low","Close","Adj Close","Volume"]])
    if not frames:
        pd.DataFrame(columns=["Date","Ticker","Open","High","Low","Close","Adj Close","Volume"]).to_csv(args.dst, index=False)
        print("No tickers to download. Wrote empty combined.csv")
        return
    out = pd.concat(frames, ignore_index=True).sort_values(["Ticker","Date"])
    out.to_csv(args.dst, index=False)
    print(f"Wrote {len(out):,} rows to {args.dst} for {len(tickers)} tickers.")

if __name__ == "__main__":
    main()