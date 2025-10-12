import pandas as pd
from ..common.indicators import sma

def _to_numeric(df: pd.DataFrame) -> pd.DataFrame:
    for c in ("Open", "High", "Low", "Close"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def _down_streak(group: pd.DataFrame) -> pd.Series:
    """
    Consecutive down days (Close < prior Close). Resets to 0 on an up/flat day.
    Returns a Series aligned to group's index.
    """
    close = group["Close"]
    down = (close < close.shift(1)).astype(int)
    # run-length encoding within equal segments
    seg = (down != down.shift(1)).cumsum()
    streak = down.groupby(seg).cumsum()
    # zero out non-down days explicitly
    streak = streak.where(down.eq(1), 0)
    return streak.astype("int64")

def prepare(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds:
      - MA200: 200-day SMA of Close
      - DMA5:  5-day SMA of Close, shifted by 1 to avoid look-ahead
      - DownStreak: consecutive down-day count
    """
    if df is None or df.empty:
        return df

    df = df.copy()
    df = _to_numeric(df)
    df = df.sort_values(["Ticker", "Date"])

    # groupwise rolling means; return Series aligned to original index
    df["MA200"] = df.groupby("Ticker", group_keys=False)["Close"].apply(lambda s: sma(s, 200))
    ma5 = df.groupby("Ticker", group_keys=False)["Close"].apply(lambda s: sma(s, 5))
    df["DMA5"] = ma5.shift(1)  # 1-day displacement to prevent look-ahead

    # consecutive down days per ticker
    df["DownStreak"] = df.groupby("Ticker", group_keys=False).apply(_down_streak).reset_index(level=0, drop=True)

    return df

def signals(ctx, state: pd.DataFrame, df: pd.DataFrame):
    """
    Entry (long):
      DownStreak >= 3 AND Close < DMA5 AND Close > MA200
    Exit:
      Close >= DMA5
    Evaluated on ctx.today only. State is passed through unchanged.
    """
    dft = prepare(df)
    if dft is None or dft.empty:
        return pd.DataFrame(), pd.DataFrame(), state

    today = pd.to_datetime(ctx.today).normalize()
    snap = dft[dft["Date"] == today].dropna(subset=["Close", "DMA5", "MA200", "DownStreak"])

    # determine currently open tickers from state, if available
    open_set = set()
    if isinstance(state, pd.DataFrame) and not state.empty:
        if "ExitDate" in state.columns:
            # open positions: ExitDate is NaN
            open_set = set(state[state["ExitDate"].isna()]["Ticker"].astype(str))
        elif "Status" in state.columns:
            open_set = set(state[state["Status"].astype(str).str.lower().eq("open")]["Ticker"].astype(str))
        elif "Ticker" in state.columns:
            # fallback: treat all listed as open
            open_set = set(state["Ticker"].astype(str))

    entries = []
    exits = []

    for _, row in snap.iterrows():
        t = str(row["Ticker"])
        c = float(row["Close"])
        dma5 = float(row["DMA5"])
        ma200 = float(row["MA200"])
        ds = int(row["DownStreak"])

        # entry rule
        if (ds >= 3) and (c < dma5) and (c > ma200) and (t not in open_set):
            entries.append({
                "Date": today.date(),
                "Ticker": t,
                "Close": c,
                "DownStreak": ds,
                "DMA5": dma5,
                "MA200": ma200,
                "Rule": "enter_long_ds>=3 AND Close<DMA5 AND Close>MA200"
            })

        # exit rule (only for open)
        if (t in open_set) and (c >= dma5):
            exits.append({
                "Date": today.date(),
                "Ticker": t,
                "Close": c,
                "DMA5": dma5,
                "Rule": "exit Close>=DMA5"
            })

    return pd.DataFrame(entries), pd.DataFrame(exits), state