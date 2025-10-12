import argparse, pandas as pd, yaml
from pathlib import Path

def sma(s, n): return s.rolling(n, min_periods=n).mean()
def rsi(close, n=2):
    d = close.diff()
    up = d.clip(lower=0).rolling(n, min_periods=n).mean()
    dn = (-d.clip(upper=0)).rolling(n, min_periods=n).mean()
    rs = up / dn
    return 100 - (100 / (1 + rs))

def percent_rank(close, n):
    roll_min = close.rolling(n, min_periods=n).min()
    roll_max = close.rolling(n, min_periods=n).max()
    rng = (roll_max - roll_min).replace(0, pd.NA)
    return 100 * (close - roll_min) / rng

def load_data(universe_yaml: str) -> pd.DataFrame:
    with open(universe_yaml, "r") as f:
        u = yaml.safe_load(f)
    path = Path(u["data_path"])
    df = pd.read_csv(path, low_memory=False, parse_dates=["Date"])
    for c in ["Open","High","Low","Close","Volume"]:
        if c in df.columns: df[c] = pd.to_numeric(df[c], errors="coerce")
    return df.sort_values(["Ticker","Date"]).dropna(subset=["Date","Ticker","Close"])

def snapshot(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.Timestamp]:
    last = df["Date"].max()
    return df[df["Date"] == last].copy(), last

def build_indicators(df: pd.DataFrame) -> pd.DataFrame:
    g = df.groupby("Ticker", group_keys=False)
    # adapt windows if history is short; guarantee columns exist
    df["RSI2"]  = g["Close"].transform(lambda s: rsi(s, 2))
    df["RSI3"]  = g["Close"].transform(lambda s: rsi(s, 3))
    df["MA50"]  = g["Close"].transform(lambda s: sma(s, min(50,  max(5, len(s)))))
    df["MA200"] = g["Close"].transform(lambda s: sma(s, min(200, max(20, len(s)))))
    if "MA200" not in df.columns: df["MA200"] = pd.NA
    df["L7"]    = g["Close"].transform(lambda s: s.rolling(7,  min_periods=7).min())
    df["H7"]    = g["Close"].transform(lambda s: s.rolling(7,  min_periods=7).max())
    df["HLRank100"] = g["Close"].transform(lambda s: percent_rank(s, min(100, max(10, len(s)))))
    return df

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--universe", required=True)   # e.g. configs/universe.yaml
    ap.add_argument("--outdir", default="docs")
    args = ap.parse_args()

    df = build_indicators(load_data(args.universe))
    snap, last = snapshot(df)

    rows = []

    # RSI2_US: RSI2<5 and Close>MA200
    rsi2_us = snap.dropna(subset=["RSI2","MA200","Close"])
    m = (rsi2_us["RSI2"] < 5) & (rsi2_us["Close"] > rsi2_us["MA200"])
    for t in rsi2_us.loc[m, "Ticker"].unique():
        rows.append({"Strategy":"RSI2_US","Ticker":t,"Reason":"RSI2<5 & Close>MA200"})

    # RSI2_5_70_SSO: same rule limited to leveraged ETFs
    sso = rsi2_us
    m = (sso["RSI2"] < 5) & (sso["Close"] > sso["MA200"]) & (sso["Ticker"].isin(["SSO","UPRO","QLD","TQQQ","SPXL"]))
    for t in sso.loc[m, "Ticker"].unique():
        rows.append({"Strategy":"RSI2_5_70_SSO","Ticker":t,"Reason":"RSI2<5 & Close>MA200"})

    # Double Seven: enter Close<=L7 & >MA200; exit Close>=H7
    d7 = snap.dropna(subset=["L7","H7","MA200","Close"])
    for t in d7.loc[(d7["Close"] <= d7["L7"]) & (d7["Close"] > d7["MA200"]), "Ticker"].unique():
        rows.append({"Strategy":"Double_Seven","Ticker":t,"Reason":"Close<=L7 & >MA200"})
    for t in d7.loc[(d7["Close"] >= d7["H7"]), "Ticker"].unique():
        rows.append({"Strategy":"Double_Seven_EXIT","Ticker":t,"Reason":"Close>=H7"})

    # Connors 3D HL: RSI3<20, 2 down days, HLRank<40, >MA200
    c3 = snap.dropna(subset=["RSI3","HLRank100","MA200","Close"])
    # simple 2-day down streak on full DF
    dn2 = set()
    for t, g in df.groupby("Ticker"):
        cl = g["Close"].tail(3).to_list()
        if len(cl) >= 3 and (cl[-1] < cl[-2] < cl[-3]):
            dn2.add(t)
    m = (c3["RSI3"] < 20) & (c3["HLRank100"] < 40) & (c3["Close"] > c3["MA200"]) & (c3["Ticker"].isin(dn2))
    for t in c3.loc[m, "Ticker"].unique():
        rows.append({"Strategy":"Connors_3D_HL","Ticker":t,"Reason":"RSI3<20 & 2-down & HLRank<40 & >MA200"})

    out = pd.DataFrame(rows).sort_values(["Strategy","Ticker"])
    Path(args.outdir).mkdir(parents=True, exist_ok=True)
    (Path(args.outdir) / "diagnostics.csv").write_text(out.to_csv(index=False), encoding="utf-8")
    html = f"<h2>Diagnostics {last.date()}</h2><p>{len(out)} candidates</p>" + out.to_html(index=False)
    (Path(args.outdir) / "diagnostics.html").write_text(html, encoding="utf-8")
    print("Diagnostics complete:", len(out), "candidates")

if __name__ == "__main__":
    main()
