# src/swing_systems/common/engine.py
import os
import pandas as pd
from datetime import datetime, timezone

REQUIRED_COLS = ["Ticker","EntryDate","EntryPrice","Status","ExitDate","ExitPrice","Notes"]

def _naive_today_from(df: pd.DataFrame) -> pd.Timestamp:
    if isinstance(df, pd.DataFrame) and "Date" in df.columns and not df["Date"].isna().all():
        return pd.to_datetime(df["Date"].max()).normalize()
    return pd.Timestamp(datetime.now(timezone.utc).date())

class Ctx:
    def __init__(self, df: pd.DataFrame):
        self.df = df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame()
        self.today = _naive_today_from(self.df)

def _ensure_state_columns(state: pd.DataFrame) -> pd.DataFrame:
    if state is None or (isinstance(state, pd.DataFrame) and state.empty):
        out = pd.DataFrame(columns=REQUIRED_COLS)
    else:
        out = state.copy()

    for c in REQUIRED_COLS:
        if c not in out.columns:
            out[c] = pd.NA

    for c in ["EntryDate","ExitDate"]:
        out[c] = pd.to_datetime(out[c], errors="coerce")

    mask_missing = out["Status"].isna()
    if mask_missing.any():
        out.loc[mask_missing & out["ExitDate"].notna(), "Status"] = "closed"
        out.loc[mask_missing & out["ExitDate"].isna(),  "Status"] = "open"

    out["EntryPrice"] = pd.to_numeric(out["EntryPrice"], errors="coerce")
    out["ExitPrice"]  = pd.to_numeric(out["ExitPrice"],  errors="coerce")
    out["Ticker"]     = out["Ticker"].astype("string")

    return out[REQUIRED_COLS]

def load_state(path: str) -> pd.DataFrame:
    if os.path.exists(path):
        try:
            raw = pd.read_csv(path)
        except Exception:
            raw = pd.DataFrame(columns=REQUIRED_COLS)
    else:
        raw = pd.DataFrame(columns=REQUIRED_COLS)
    return _ensure_state_columns(raw)

def save_state(path: str, state: pd.DataFrame) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    _ensure_state_columns(state).to_csv(path, index=False)

def _as_df(obj, cols=("Ticker","Date","Close")) -> pd.DataFrame:
    """Coerce entries/exits to DataFrame with at least Ticker/Date/Close."""
    if obj is None:
        return pd.DataFrame(columns=list(cols))
    if isinstance(obj, pd.DataFrame):
        df = obj.copy()
    elif isinstance(obj, pd.Series):
        df = obj.to_frame().T
    elif isinstance(obj, dict):
        df = pd.DataFrame([obj])
    elif isinstance(obj, list):
        if not obj:
            return pd.DataFrame(columns=list(cols))
        if isinstance(obj[0], dict):
            df = pd.DataFrame(obj)
        elif isinstance(obj[0], (list, tuple)):
            df = pd.DataFrame(obj, columns=list(cols)[:len(obj[0])])
        else:
            # single list of scalar -> wrap
            df = pd.DataFrame([obj], columns=list(cols)[:len(obj)])
    else:
        # unknown -> empty
        return pd.DataFrame(columns=list(cols))

    # Ensure required basic columns exist
    for c in cols:
        if c not in df.columns:
            df[c] = pd.NA

    # Type coercions
    if "Date" in df.columns:
        df["Date"]  = pd.to_datetime(df["Date"], errors="coerce")
    if "Close" in df.columns:
        df["Close"] = pd.to_numeric(df["Close"], errors="coerce")
    if "Ticker" in df.columns:
        df["Ticker"] = df["Ticker"].astype("string")

    # Keep only known + pass-through extras
    return df

def _normalize_entries_exits(entries, exits):
    e = _as_df(entries, cols=("Ticker","Date","Close","Rule"))
    x = _as_df(exits,   cols=("Ticker","Date","Close","Rule"))
    return e, x

def run_strategy(ctx: Ctx, state_path: str, out_dir: str, signal_fn):
    os.makedirs(out_dir, exist_ok=True)
    state = load_state(state_path)

    # Accept strategies that return (entries, exits) or (entries, exits, dft)
    result = signal_fn(ctx, state, ctx.df)
    if not isinstance(result, tuple):
        raise ValueError("signal_fn must return a tuple")
    if len(result) == 2:
        entries, exits = result
        dft = None
    elif len(result) == 3:
        entries, exits, dft = result
    else:
        raise ValueError("signal_fn must return (entries, exits) or (entries, exits, dft)")

    entries, exits = _normalize_entries_exits(entries, exits)

    today_str = str(ctx.today.date())
    os.makedirs(out_dir, exist_ok=True)
    e_path = os.path.join(out_dir, f"entries_{today_str}.csv")
    x_path = os.path.join(out_dir, f"exits_{today_str}.csv")

    (entries if not entries.empty else pd.DataFrame(columns=["Ticker","Date","Close"])).to_csv(e_path, index=False)
    (exits   if not exits.empty   else pd.DataFrame(columns=["Ticker","Date","Close"])).to_csv(x_path, index=False)

    # Update portfolio state
    s = state.copy()

    if not exits.empty:
        for _, r in exits.iterrows():
            t = r.get("Ticker")
            exit_dt = r.get("Date", ctx.today)
            exit_px = r.get("Close", pd.NA)
            idx = s.index[s["Ticker"] == t]
            target_idx = None
            if len(idx) > 0:
                open_idx = idx[s.loc[idx, "Status"] == "open"]
                target_idx = open_idx[-1] if len(open_idx) > 0 else idx[-1]
            if target_idx is not None:
                s.loc[target_idx, "ExitDate"]  = pd.to_datetime(exit_dt, errors="coerce") if pd.notna(exit_dt) else ctx.today
                s.loc[target_idx, "ExitPrice"] = pd.to_numeric(exit_px, errors="coerce")
                s.loc[target_idx, "Status"]    = "closed"

    if not entries.empty:
        new_rows = []
        for _, r in entries.iterrows():
            new_rows.append({
                "Ticker":     r.get("Ticker"),
                "EntryDate":  pd.to_datetime(r.get("Date"), errors="coerce"),
                "EntryPrice": pd.to_numeric(r.get("Close"), errors="coerce"),
                "Status":     "open",
                "ExitDate":   pd.NaT,
                "ExitPrice":  pd.NA,
                "Notes":      r.get("Rule", pd.NA),
            })
        if new_rows:
            s = pd.concat([s, pd.DataFrame(new_rows)], ignore_index=True)

    s = _ensure_state_columns(s)
    save_state(state_path, s)

    open_df = s.loc[s["Status"] == "open", ["Ticker","EntryDate","EntryPrice","Status"]].copy()
    open_path = os.path.join(out_dir, f"open_positions_{today_str}.csv")
    open_df.to_csv(open_path, index=False)

    print(f"Today: {today_str}")
    print(f"Entries: {len(entries)} -> {e_path}")
    print(f"Exits:   {len(exits)} -> {x_path}")
    print(f"Open:    {len(open_df)} -> {open_path}")
    print(f"State:   {state_path}")
    return entries, exits, dft