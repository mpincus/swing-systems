import pandas as pd
from ..common.indicators import sma

def prepare(df: pd.DataFrame, ma_len=200, dma_len=5):
    out = df.copy().sort_values(["Ticker","Date"])
    out['MA200'] = out.groupby('Ticker')['Close'].transform(lambda s: sma(s, ma_len))
    out['DMA5'] = out.groupby('Ticker')['Close'].transform(lambda s: sma(s, dma_len))
    def down_streak(g):
        hh = g['High']<g['High'].shift(1)
        ll = g['Low']<g['Low'].shift(1)
        down = (hh & ll).astype(int)
        streak = (down * (down.groupby((down==0).cumsum()).cumcount()+1))
        return streak
    out['DownStreak'] = out.groupby('Ticker', group_keys=False).apply(down_streak).reset_index(level=0, drop=True)
    return out

def signals(ctx, state, dft, time_stop_days=10):
    entries, exits = [], []
    open_set = set(state.loc[state['Status']=='open','Ticker']) if not state.empty else set()
    for _, row in dft.iterrows():
        t = row['Ticker']; c = row['Close']
        if (row['DownStreak']>=3) and (c < row['DMA5']) and (c > row['MA200']) and (t not in open_set):
            entries.append({"Ticker": t, "EntryDate": ctx.today, "EntryPrice": c, "Notes": "3DHL entry"})
    if not state.empty:
        merged = dft.merge(state[state['Status']=='open'][['Ticker','EntryDate','EntryPrice']], on='Ticker', how='inner')
        for _, r in merged.iterrows():
            exit_signal = r['Close'] > r['DMA5']
            if not exit_signal and time_stop_days:
                held = (ctx.today - pd.to_datetime(r['EntryDate'])).days
                exit_signal = held >= time_stop_days
            if exit_signal:
                exits.append({"Ticker": r['Ticker'], "ExitPrice": r['Close'], "Notes": "3DHL exit"})
    return entries, exits