import pandas as pd
import numpy as np

def _to_num(s: pd.Series) -> pd.Series:
    """Convert series to numeric, coercing invalid entries to NaN."""
    return pd.to_numeric(s, errors="coerce")

def sma(s: pd.Series, n: int) -> pd.Series:
    """Simple moving average."""
    s = _to_num(s)
    out = s.rolling(n, min_periods=n).mean()
    return out.bfill()

def ema(s: pd.Series, n: int) -> pd.Series:
    """Exponential moving average."""
    s = _to_num(s)
    out = s.ewm(span=n, adjust=False, min_periods=n).mean()
    return out.bfill()

def rsi_wilder(close: pd.Series, n: int = 14) -> pd.Series:
    """Wilder’s RSI (relative strength index)."""
    c = _to_num(close)
    diff = c.diff()
    up = diff.clip(lower=0)
    dn = -diff.clip(upper=0)
    alpha = 1.0 / n
    avg_gain = up.ewm(alpha=alpha, adjust=False).mean()
    avg_loss = dn.ewm(alpha=alpha, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100.0 - (100.0 / (1.0 + rs))
    return rsi.bfill()

def atr_wilder(df: pd.DataFrame, n: int = 14) -> pd.Series:
    """Wilder’s Average True Range."""
    h = _to_num(df["High"])
    l = _to_num(df["Low"])
    c = _to_num(df["Close"])
    prev_c = c.shift(1)
    tr = pd.concat([(h - l), (h - prev_c).abs(), (l - prev_c).abs()], axis=1).max(axis=1)
    atr = tr.ewm(alpha=1.0 / n, adjust=False).mean()
    return atr.bfill()

# ---- Aliases expected by strategy modules ----
def rsi(close: pd.Series, n: int = 14) -> pd.Series:
    """Alias for RSI Wilder to maintain backward compatibility."""
    return rsi_wilder(close, n)

def atr(df: pd.DataFrame, n: int = 14) -> pd.Series:
    """Alias for ATR Wilder to maintain backward compatibility."""
    return atr_wilder(df, n)