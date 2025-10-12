import pandas as pd
from ..common.indicators import rsi, sma

RSI_PERIOD = 2
RSI_BUY = 5
RSI_SELL = 70
MA_LONG = 200

def prepare(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy().sort_values(["Ticker", "Date"])
    # numeric coercion
    for c in ["Open", "High", "Low", "Close", "Volume"]:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")
    # indicators
    out["RSI2"]  = out.groupby("Ticker")["Close"].transform(lambda s: rsi(s, RSI_PERIOD))
    out["MA200"] = out.groupby("Ticker")["Close"].transform(lambda s: sma(s, MA_LONG))
    return out

def signals(ctx, state, dft: pd.DataFrame):
    dft = prepare(dft)

    last_day = pd.to_datetime(dft["Date"].dropna().max()).normalize()
    today_df = dft[dft["Date"] == last_day].copy()
    # drop NaNs and enforce uptrend
    today_df = today_df.dropna(subset=["Close", "MA200", "RSI2"])
    today_df = today_df[today_df["Close"] > today_df["MA200"]]

    open_set = set(state.loc[state["Status"] == "open", "Ticker"]) if not state.empty else set()

    entries, exits = [], []

    # ----- ENTRIES: one per ticker, today only -----
    ent = today_df[today_df["RSI2"] < RSI_BUY][["Ticker", "Date", "Close"]].drop_duplicates(subset=["Ticker"], keep="last")
    for _, r in ent.iterrows():
        t = r["Ticker"]
        if t in open_set:
            continue
        entries.append({
            "Ticker": t,
            "EntryDate": pd.to_datetime(r["Date"]).normalize(),
            "EntryPrice": float(r["Close"]),
            "Notes": f"RSI2<{RSI_BUY}"
        })

    # ----- EXITS: one per ticker, today only -----
    if open_set:
        ex = today_df[
            (today_df["Ticker"].isin(open_set)) &
            (today_df["RSI2"] > RSI_SELL)
        ][["Ticker", "Date", "Close"]].drop_duplicates(subset=["Ticker"], keep="last")

        for _, r in ex.iterrows():
            exits.append({
                "Ticker": r["Ticker"],
                "ExitDate": pd.to_datetime(r["Date"]).normalize(),
                "ExitPrice": float(r["Close"]),
                "Notes": f"RSI2>{RSI_SELL}"
            })

    return entries, exits, dft