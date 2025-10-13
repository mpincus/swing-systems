# src/swing_systems/common/engine.py
import os
import pandas as pd
from datetime import datetime, timezone

REQUIRED_COLS = [
    "Ticker","EntryDate","EntryPrice","Status","ExitDate","ExitPrice","Notes"
]

def _naive_today_from(df: pd.DataFrame) -> pd.Timestamp:
    if "Date" in df.columns and not df["Date"].isna().all():
        return pd.to_datetime(df["Date"].max()).normalize()
    # fallback to UTC today if df has no Date
    return pd.Timestamp(datetime.now(timezone.utc).date())

class Ctx:
    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()
        self.today = _naive_today_from(self.df)

def _ensure_state_columns(state: pd.DataFrame) -> pd.DataFrame:
    """Guarantee REQUIRED_COLS exist and infer Status if missing."""
    if state is None or state.empty:
        out = pd.DataFrame(columns=REQUIRED_COLS)
    else:
        out = state.copy()

    for c in REQUIRED_COLS:
        if c not in out.columns:
            out[c] = pd.NA

    # Normalize dates
    for c in ["EntryDate","ExitDate"]:
        if c in out.columns:
            out[c] = pd.to_datetime(out[c], errors="coerce")

    # Infer Status if missing or partially null
    # rule: closed if ExitDate present, else open
    mask_missing = out["Status"].isna()
    if mask_missing.any():
        out.loc[mask_missing & out["ExitDate"].notna(), "Status"] = "closed"
        out.loc[mask_missing & out["ExitDate"].isna(), "Status"] = "open"

    # Types
    if "EntryPrice" in out.columns:
        out["EntryPrice"] = pd.to_numeric(out["EntryPrice"], errors="coerce")
    if "ExitPrice" in out.columns:
        out["ExitPrice"] = pd.to_numeric(out["ExitPrice"], errors="coerce")
    if "Ticker" in out.columns:
        out["Ticker"] = out["Ticker"].astype("string")

    # Keep only required columns, in order
    out = out[REQUIRED_COLS]
    return out

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

def _normalize_entries_exits(entries: pd.DataFrame, exits: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    e = (entries.copy() if entries is not None else pd.DataFrame(columns=["Ticker","Date","Close"]))
    x = (exits.copy()   if exits   is not None else pd.DataFrame(columns=["Ticker","Date","Close"]))

    for c in ["Date"]:
        if c in e.columns: e[c] = pd.to_datetime(e[c], errors="coerce")
        if c in x.columns: x[c] = pd.to_datetime(x[c], errors="coerce")
    if "Close" in e.columns: e["Close"] = pd.to_numeric(e["Close"], errors="coerce")
    if "Close" in x.columns: x["Close"] = pd.to_numeric(x["Close"], errors="coerce")
    if "Ticker" in e.columns: e["Ticker"] = e["Ticker"].astype("string")
    if "Ticker" in x.columns: x["Ticker"] = x["Ticker"].astype("string")
    return e, x

def run_strategy(ctx: Ctx, state_path: str, out_dir: str, signal_fn):
    os.makedirs(out_dir, exist_ok=True)
    state = load_state(state_path)

    # Call strategy. Accept (entries, exits) or (entries, exits, dft)
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

    # Persist entries/exits with today stamp
    today_str = str(ctx.today.date())
    e_path = os.path.join(out_dir, f"entries_{today_str}.csv")
    x_path = os.path.join(out_dir, f"exits_{today_str}.csv")
    if not entries.empty:
        entries.to_csv(e_path, index=False)
    else:
        pd.DataFrame(columns=["Ticker","Date","Close"]).to_csv(e_path, index=False)
    if not exits.empty:
        exits.to_csv(x_path, index=False)
    else:
        pd.DataFrame(columns=["Ticker","Date","Close"]).to_csv(x_path, index=False)

    # Update state
    s = state.copy()

    # Close positions listed in exits
    if not exits.empty:
        for _, r in exits.iterrows():
            t = r.get("Ticker")
            exit_dt = r.get("Date", ctx.today)
            exit_px = r.get("Close", pd.NA)
            idx = s.index[s["Ticker"] == t]
            if len(idx) > 0:
                # close the most recent open row for this ticker
                open_idx = idx[s.loc[idx, "Status"] == "open"]
                if len(open_idx) == 0:
                    open_idx = [idx[-1]]
                s.loc[open_idx, "ExitDate"] = pd.to_datetime(exit_dt, errors="coerce") if pd.notna(exit_dt) else ctx.today
                s.loc[open_idx, "ExitPrice"] = pd.to_numeric(exit_px, errors="coerce")
                s.loc[open_idx, "Status"] = "closed"

    # Add new entries
    if not entries.empty:
        new_rows = []
        for _, r in entries.iterrows():
            new_rows.append({
                "Ticker": r.get("Ticker"),
                "EntryDate": pd.to_datetime(r.get("Date"), errors="coerce"),
                "EntryPrice": pd.to_numeric(r.get("Close"), errors="coerce"),
                "Status": "open",
                "ExitDate": pd.NaT,
                "ExitPrice": pd.NA,
                "Notes": r.get("Rule", pd.NA)
            })
        if new_rows:
            s = pd.concat([s, pd.DataFrame(new_rows)], ignore_index=True)

    s = _ensure_state_columns(s)
    save_state(state_path, s)

    # Open positions snapshot
    open_df = s.loc[s["Status"] == "open", ["Ticker","EntryDate","EntryPrice","Status"]].copy()
    open_path = os.path.join(out_dir, f"open_positions_{today_str}.csv")
    open_df.to_csv(open_path, index=False)

    print(f"Today: {today_str}")
    print(f"Entries: {len(entries)} -> {e_path}")
    print(f"Exits:   {len(exits)} -> {x_path}")
    print(f"Open:    {len(open_df)} -> {open_path}")
    print(f"State:   {state_path}")
    return entries, exits, dft