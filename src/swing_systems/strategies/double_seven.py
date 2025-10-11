import pandas as pd
from ..common.indicators import sma

def prepare(df: pd.DataFrame):
    out = df.copy().sort_values(["Ticker","Date"])
    # numeric safety
    for c in ["Open","High","Low","Close","Volume"]:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")
    # needed features
    out["MA200"] = out.groupby("Ticker")["Close"].transform(lambda s: sma(s, 200))
    out["L7"]    = out.groupby("Ticker")["Low"]  .transform(lambda s: s.rolling(7, min_periods=7).min())
    return out.dropna(subset=["MA200","L7"])

def signals(ctx, state, dft):
    dft = prepare(dft)
    entries, exits = [], []
    open_set = set(state.loc[state["Status"]=="open","Ticker"]) if not state.empty else set()

    today_rows = dft.groupby("Ticker").tail(1)  # last bar per ticker
    for _, r in today_rows.iterrows():
        t = r["Ticker"]
        c = float(r["Close"])
        l7 = float(r["L7"])
        ma200 = float(r["MA200"])
        if (c <= l7) and (c > ma200) and (t not in open_set):
            entries.append({"Ticker": t, "EntryDate": ctx.today, "EntryPrice": c, "Notes": "DoubleSeven entry"})

    # simple exits: price above previous 7-day high or time stop handled by engine if applicable
    if not state.empty:
        prev_high7 = dft.groupby("Ticker")["High"].apply(lambda s: s.shift(1).rolling(7, min_periods=7).max())
        dft = dft.assign(H7=prev_high7)
        merged = dft.groupby("Ticker").tail(1).merge(
            state[state["Status"]=="open"][["Ticker","EntryDate","EntryPrice"]],
            on="Ticker", how="inner"
        )
        for _, r in merged.iterrows():
            if pd.notna(r.get("H7")) and float(r["Close"]) >= float(r["H7"]):
                exits.append({"Ticker": r["Ticker"], "ExitPrice": float(r["Close"]), "Notes": "DoubleSeven exit"})

    return entries, exits