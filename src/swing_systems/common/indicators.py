from __future__ import annotations
import pandas as pd
import numpy as np

def rsi(series: pd.Series, length: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1/length, min_periods=length, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/length, min_periods=length, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    out = 100 - (100 / (1 + rs))
    return out.fillna(method="bfill")

def rolling_low(series: pd.Series, n: int) -> pd.Series:
    return series.rolling(n).min()

def rolling_high(series: pd.Series, n: int) -> pd.Series:
    return series.rolling(n).max()

def sma(series: pd.Series, n: int) -> pd.Series:
    return series.rolling(n).mean()