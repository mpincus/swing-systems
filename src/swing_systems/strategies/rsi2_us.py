import pandas as pd
from ..common.indicators import rsi, sma

def prepare(df: pd.DataFrame, rsi_len=2, ma_filter=200, dma5=5):
    out = df.copy().sort_values(["Ticker","Date"])
    out[f"RSI{rsi_len}"] = out.groupby("Ticker")["Close"].transform(lambda s: rsi(s, rsi_len))
    out["MA200"] = out.groupby("Ticker")["Close"].transform(lambda s: sma(s, ma_filter))
    out["DMA5"] = out.groupby("Ticker")["Close"].transform(lambda s: sma(s, dma5))
    return out

def signals(ctx, state, dft, buy_thr=10, exit_on="rsi_cross", rsi_exit=70, time_stop_days=10):
    entries, exits = [], []
    open_set = set(state.loc[state["Status"]=="open","Ticker"]) if not state.empty else set()
    for _, row in dft.iterrows():
        t = row['Ticker']; c = row['Close']; rv = row.filter(like='RSI').values[0]; ma200 = row['MA200']
        if pd.notna(rv) and pd.notna(ma200) and (rv <= buy_thr) and (c > ma200) and (t not in open_set):
            entries.append({"Ticker": t, "EntryDate": ctx.today, "EntryPrice": c, "Notes": f"RSI2<= {buy_thr}"})
    if not state.empty:
        merged = dft.merge(state[state['Status']=='open'][['Ticker','EntryDate','EntryPrice']], on='Ticker', how='inner')
        for _, r in merged.iterrows():
            exit_signal = False
            if exit_on == 'rsi_cross':
                rv = r.filter(like='RSI').values[0]
                exit_signal = pd.notna(rv) and (rv >= rsi_exit)
            else:
                exit_signal = r['Close'] > r['DMA5']
            if not exit_signal and time_stop_days:
                held = (ctx.today - pd.to_datetime(r['EntryDate'])).days
                exit_signal = held >= time_stop_days
            if exit_signal:
                exits.append({"Ticker": r['Ticker'], "ExitPrice": r['Close'], "Notes": "RSI2 exit"})
    return entries, exits