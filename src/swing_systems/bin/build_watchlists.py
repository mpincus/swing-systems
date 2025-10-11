import argparse
from pathlib import Path
import pandas as pd
import yfinance as yf
from datetime import date, timedelta
from ..common.indicators import rsi, sma, rolling_high, rolling_low

def load_seed(path: Path) -> list[str]:
    return [l.strip().upper() for l in path.read_text().splitlines() if l.strip()]

def fetch_recent(tickers: list[str], lookback_days: int = 260) -> pd.DataFrame:
    end = date.today()
    start = end - timedelta(days=int(lookback_days*1.5))
    frames = []
    for t in tickers:
        df = yf.download(t, start=str(start), end=str(end), interval="1d", progress=False)
        if df.empty: 
            continue
        df = df.reset_index().rename(columns={
            "Date": "Date","Open":"Open","High":"High","Low":"Low","Close":"Close","Adj Close":"Adj Close","Volume":"Volume"
        })
        df["Ticker"] = t
        frames.append(df[["Date","Ticker","Open","High","Low","Close","Adj Close","Volume"]])
    if not frames:
        return pd.DataFrame(columns=["Date","Ticker","Open","High","Low","Close","Adj Close","Volume"])
    return pd.concat(frames, ignore_index=True).sort_values(["Ticker","Date"])

def compute_feats(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return df
    out = df.copy()
    out["Date"] = pd.to_datetime(out["Date"])
    out = out.sort_values(["Ticker","Date"])
    out["RSI2"] = out.groupby("Ticker")["Close"].transform(lambda s: rsi(s, 2))
    out["MA200"] = out.groupby("Ticker")["Close"].transform(lambda s: sma(s, 200))
    out["DMA5"] = out.groupby("Ticker")["Close"].transform(lambda s: sma(s, 5))
    out["L7"] = out.groupby("Ticker")["Close"].transform(lambda s: rolling_low(s, 7))
    out["H7"] = out.groupby("Ticker")["Close"].transform(lambda s: rolling_high(s, 7))
    def down_streak(g):
        hh = g["High"]<g["High"].shift(1)
        ll = g["Low"]<g["Low"].shift(1)
        d = (hh & ll).astype(int)
        return (d * (d.groupby((d==0).cumsum()).cumcount()+1))
    out["DownStreak"] = out.groupby("Ticker", group_keys=False).apply(down_streak).reset_index(level=0, drop=True)
    return out

def select_lists(df: pd.DataFrame) -> dict[str, list[str]]:
    if df.empty:
        return {k: [] for k in ["double_seven","rsi2_us","rsi2_5_70_sso","connors_3d_hl","all_tickers"]}
    today = df["Date"].max()
    dft = df[df["Date"]==today]
    d7   = dft[(dft["Close"]>dft["MA200"]) & (dft["Close"]<=dft["L7"])]['Ticker'].unique()
    rsiu = dft[(dft["Close"]>dft["MA200"]) & (dft["RSI2"]<=10)]['Ticker'].unique()
    sso  = dft[(dft["Ticker"]=="SSO") & (dft["RSI2"]<=5)]['Ticker'].unique()
    c3   = dft[(dft["DownStreak"]>=3) & (dft["Close"]<dft["DMA5"]) & (dft["Close"]>dft["MA200"])]['Ticker'].unique()
    allu = sorted(set(d7)|set(rsiu)|set(sso)|set(c3))
    return {
        "double_seven": sorted(d7),
        "rsi2_us": sorted(rsiu),
        "rsi2_5_70_sso": sorted(sso),
        "connors_3d_hl": sorted(c3),
        "all_tickers": allu,
    }

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", default="configs/seed_universe.txt")
    ap.add_argument("--outdir", default="configs/generated")
    args = ap.parse_args()

    tickers = load_seed(Path(args.seed))
    recent = fetch_recent(tickers, 260)
    feats = compute_feats(recent)
    lists = select_lists(feats)

    outdir = Path(args.outdir); outdir.mkdir(parents=True, exist_ok=True)
    for name, arr in lists.items():
        (outdir/f"{name}.txt").write_text("\n".join(arr))
    print({k: len(v) for k,v in lists.items()})

if __name__ == "__main__":
    main()