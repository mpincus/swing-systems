import argparse
from pathlib import Path
import pandas as pd
import yaml
import numpy as np

def atr(df: pd.DataFrame, n: int = 14) -> pd.Series:
    h, l, c = df["High"], df["Low"], df["Close"]
    prev_c = c.shift(1)
    tr = pd.concat([(h - l), (h - prev_c).abs(), (l - prev_c).abs()], axis=1).max(axis=1)
    return tr.rolling(n, min_periods=n).mean()

def sma(s: pd.Series, n: int) -> pd.Series:
    return s.rolling(n, min_periods=n).mean()

def last_snapshot(df: pd.DataFrame) -> pd.DataFrame:
    # assume df has Date,Ticker,...
    ix = df.groupby("Ticker")["Date"].idxmax()
    return df.loc[ix].reset_index(drop=True)

def write_yaml(path: Path, tickers: list[str]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        yaml.safe_dump({"universe": sorted(tickers)}, f, sort_keys=False)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--combined", default="data/combined.csv")
    ap.add_argument("--outdir", default="configs/watchlists")
    ap.add_argument("--lookback", type=int, default=90)   # last ~90 trading days
    args = ap.parse_args()

    df = pd.read_csv(args.combined, parse_dates=["Date"])
    df = df.sort_values(["Ticker","Date"])

    # limit to recent window for speed
    cutoff = df["Date"].max() - pd.Timedelta(days=args.lookback*2)  # buffer for indicators
    df = df[df["Date"] >= cutoff].copy()

    # indicators per ticker
    df["ATR14"] = df.groupby("Ticker", group_keys=False).apply(lambda g: atr(g, 14))
    df["MA50"]  = df.groupby("Ticker")["Close"].transform(lambda s: sma(s, 50))
    df["MA200"] = df.groupby("Ticker")["Close"].transform(lambda s: sma(s, 200))
    df["Vol30"] = df.groupby("Ticker")["Volume"].transform(lambda s: s.rolling(30, min_periods=10).mean())
    df["ATRp"]  = (df["ATR14"] / df["Close"]) * 100.0

    snap = last_snapshot(df)
    snap = snap.replace([np.inf, -np.inf], np.nan).dropna(subset=["Close","Vol30","ATR14","MA50","MA200","ATRp"])

    # base liquidity/quality filter
    base = snap[
        (snap["Close"] >= 20) &
        (snap["Vol30"]  >= 2_000_000)
    ]

    # Strategy universes
    # 1) RSI2-US: mean-reversion in liquid, stable names, prefer uptrend
    rsi2_us = base[
        (snap["ATRp"] >= 1.0) & (snap["ATRp"] <= 4.0) &
        (snap["Close"] > snap["MA200"])
    ]["Ticker"].unique().tolist()

    # 2) RSI2 5/70 SSO: leveraged index ETFs (fixed list)
    rsi2_570 = ["SSO","QLD","TQQQ","SPXL","UPRO"]

    # 3) Double Seven: trending symbols with reasonable volatility
    dbl7 = base[
        (snap["Close"] > snap["MA50"]) & (snap["MA50"] > snap["MA200"]) &
        (snap["ATRp"] >= 1.0) & (snap["ATRp"] <= 6.0)
    ]["Ticker"].unique().tolist()

    # 4) Connors 3DHL: likes a bit more vol, still above long-term trend
    c3dhl = base[
        (snap["ATRp"] >= 2.0) &
        (snap["Close"] > snap["MA200"])
    ]["Ticker"].unique().tolist()

    outdir = Path(args.outdir)
    write_yaml(outdir / "rsi2_us.yaml", {"universe": rsi2_us} if isinstance(rsi2_us, dict) else rsi2_us)
    write_yaml(outdir / "rsi2_5_70_sso.yaml", rsi2_570)
    write_yaml(outdir / "double_seven.yaml", dbl7)
    write_yaml(outdir / "connors_3d_hl.yaml", c3dhl)

    print(f"Watchlists written to {outdir}")

if __name__ == "__main__":
    main()