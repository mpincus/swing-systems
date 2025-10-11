import pandas as pd
from ..common.indicators import rolling_low, rolling_high, sma

def prepare(df: pd.DataFrame, entry_lookback=7, exit_lookback=7, ma_len=200):
    out = df.copy().sort_values(["Ticker","Date"])
    out["L7"] = out.groupby("Ticker")["Close"].transform(lambda s: rolling_low(s, entry_lookback))
    out["H7"] = out.groupby("Ticker")["Close"].transform(lambda s: rolling_high(s, exit_lookback))
    out["MA200"] = out.groupby("Ticker")["Close"].transform(lambda s: sma(s, ma_len))
    return out

def signals(ctx, state, dft, time_stop_days=20):
    entries, exits = [], []
    open_set = set(state.loc[state["Status"]=="open","Ticker"]) if not state.empty else set()
    for _, row in dft.iterrows():
        t = row["Ticker"]; c = row["Close"]; l7 = row["L7"]; h7 = row["H7"]; ma200 = row["MA200"]
        if pd.notna(l7) and pd.notna(ma200) and (c <= l7) and (c > ma200) and (t not in open_set):
            entries.append({"Ticker": t, "EntryDate": ctx.today, "EntryPrice": c, "Notes": "Double7 entry"})
    if not state.empty:
        merged = dft.merge(state[state["Status"]=="open"][['Ticker','EntryDate','EntryPrice']], on='Ticker', how='inner')
        for _, r in merged.iterrows():
            exit_signal = pd.notna(r['H7']) and (r['Close'] >= r['H7'])
            if not exit_signal and time_stop_days:
                held = (ctx.today - pd.to_datetime(r['EntryDate'])).days
                exit_signal = held >= time_stop_days
            if exit_signal:
                exits.append({"Ticker": r['Ticker'], "ExitPrice": r['Close'], "Notes": "Double7 exit"})
    return entries, exits