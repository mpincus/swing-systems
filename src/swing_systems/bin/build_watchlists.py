import argparse
from pathlib import Path
import pandas as pd
import numpy as np
import yaml

# ---------- helpers ----------

def atr(df: pd.DataFrame, n: int = 14) -> pd.Series:
    h = pd.to_numeric(df["High"], errors="coerce")
    l = pd.to_numeric(df["Low"], errors="coerce")
    c = pd.to_numeric(df["Close"], errors="coerce")
    prev_c = c.shift(1)
    tr = pd.concat([(h - l), (h - prev_c).abs(), (l - prev_c).abs()], axis=1).max(axis=1)
    return tr.rolling(n, min_periods=n).mean()

def sma(s: pd.Series, n: int) -> pd.Series:
    s = pd.to_numeric(s, errors="coerce")
    return s.rolling(n, min_periods=n).mean()

def last_snapshot(df: pd.DataFrame) -> pd.DataFrame:
    ix = df.groupby("Ticker")["Date"].idxmax()
    return df.loc[ix].reset_index(drop=True)

def write_yaml(path: Path, tickers: list[str]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        yaml.safe_dump({"universe": sorted({t for t in tickers if isinstance(t, str) and t.strip()})}, f, sort_keys=False)

# ---------- main ----------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--combined", default="data/combined.csv")
    ap.add_argument("--outdir", default="configs/watchlists")
    ap.add_argument("--lookback", type=int, default=90)
    args = ap.parse_args()

    df = pd.read_csv(args.combined, parse_dates=["Date"], low_memory=False, dtype={"Ticker": "string"})
    # enforce numerics up-front
    for col in ["Open","High","Low","Close","Volume"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["Date","Open","High","Low","Close"])
    df = df.sort_values(["Ticker","Date"])

    # limit window (buffer for indicators)
    cutoff = df["Date"].max() - pd.Timedelta(days=args.lookback*2)
    df = df[df["Date"] >= cutoff].copy()

    # indicators
    df["ATR14"] = df.groupby("Ticker", group_keys=False).apply(lambda g: atr(g, 14)).reset_index(drop=True)
    df["MA50"]  = df.groupby("Ticker")["Close"].transform(lambda s: sma(s, 50))
    df["MA200"] = df.groupby("Ticker")["Close"].transform(lambda s: sma(s, 200))
    df["Vol30"] = df.groupby("Ticker")["Volume"].transform(lambda s: s.rolling(30, min_periods=10).mean())
    df["ATRp"]  = (df["ATR14"] / df["Close"]) * 100.0

    snap = last_snapshot(df)
    snap = snap.replace([np.inf, -np.inf], np.nan).dropna(subset=["Close","Vol30","ATR14","MA50","MA200","ATRp"])

    # base liquidity filter
    base = snap[(snap["Close"] >= 20) & (snap["Vol30"] >= 2_000_000)]

    # per-strategy universes
    rsi2_us = base[(base["ATRp"] >= 1.0) & (base["ATRp"] <= 4.0) & (base["Close"] > base["MA200"])]["Ticker"].tolist()
    rsi2_570 = ["SSO","QLD","TQQQ","SPXL","UPRO"]
    dbl7 = base[(base["Close"] > base["MA50"]) & (base["MA50"] > base["MA200"]) & (base["ATRp"] >= 1.0) & (base["ATRp"] <= 6.0)]["Ticker"].tolist()
    c3dhl = base[(base["ATRp"] >= 2.0) & (base["Close"] > base["MA200"])]["Ticker"].tolist()

    outdir = Path(args.outdir)
    write_yaml(outdir / "rsi2_us.yaml", rsi2_us)
    write_yaml(outdir / "rsi2_5_70_sso.yaml", rsi2_570)
    write_yaml(outdir / "double_seven.yaml", dbl7)
    write_yaml(outdir / "connors_3d_hl.yaml", c3dhl)

    print(f"Watchlists written to {outdir}")

if __name__ == "__main__":
    main()