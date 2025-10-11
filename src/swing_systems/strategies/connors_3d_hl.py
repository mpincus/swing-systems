import pandas as pd
from ..common.indicators import sma

def _down_streak_group(g: pd.DataFrame) -> pd.Series:
    down = (g["High"] < g["High"].shift(1)) & (g["Low"] < g["Low"].shift(1))
    # run-length counter of consecutive True values
    groups = (~down).cumsum()
    return down.astype(int).groupby(groups).cumsum()

def prepare(df: pd.DataFrame, ma_len=200, dma_len=5):
    out = df.copy().sort_values(["Ticker", "Date"])
    out["MA200"] = out.groupby("Ticker")["Close"].transform(lambda s: sma(s, ma_len))
    out["DMA5"]  = out.groupby("Ticker")["Close"].transform(lambda s: sma(s, dma_len))
    # returns a Series, aligns to rows; avoids DataFrame assignment error
    out["DownStreak"] = out.groupby("Ticker", group_keys=False).apply(_down_streak_group).reset_index(drop=True)
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