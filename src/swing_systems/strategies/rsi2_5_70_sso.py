import pandas as pd
from ..common.indicators import rsi

def prepare(df: pd.DataFrame, rsi_len=2):
    out = df.copy().sort_values(["Ticker","Date"])
    out[f"RSI{rsi_len}"] = out.groupby("Ticker")["Close"].transform(lambda s: rsi(s, rsi_len))
    return out

def signals(ctx, state, dft, buy_thr=5, rsi_exit=70, time_stop_days=20):
    entries, exits = [], []
    open_set = set(state.loc[state['Status']=='open','Ticker']) if not state.empty else set()
    dft = dft[dft['Ticker']=='SSO']
    for _, row in dft.iterrows():
        t = row['Ticker']; c = row['Close']; rv = row.filter(like='RSI').values[0]
        if pd.notna(rv) and (rv <= buy_thr) and (t not in open_set):
            entries.append({"Ticker": t, "EntryDate": ctx.today, "EntryPrice": c, "Notes": f"RSI2<={buy_thr}"})
    if not state.empty:
        merged = dft.merge(state[state['Status']=='open'][['Ticker','EntryDate','EntryPrice']], on='Ticker', how='inner')
        for _, r in merged.iterrows():
            rv = r.filter(like='RSI').values[0]
            exit_signal = pd.notna(rv) and (rv >= rsi_exit)
            if not exit_signal and time_stop_days:
                held = (ctx.today - pd.to_datetime(r['EntryDate'])).days
                exit_signal = held >= time_stop_days
            if exit_signal:
                exits.append({"Ticker": r['Ticker'], "ExitPrice": r['Close'], "Notes": "RSI2 5/70 exit"})
    return entries, exits