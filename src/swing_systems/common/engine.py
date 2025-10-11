import pandas as pd
from pathlib import Path

def _last_business_day(ts: pd.Timestamp) -> pd.Timestamp:
    # ensure weekday 0–4
    while ts.weekday() > 4:
        ts -= pd.Timedelta(days=1)
    return ts.normalize()

class Ctx:
    def __init__(self, df: pd.DataFrame):
        # robust “today”: use last date in df; if NaT/empty, fallback to UTC now -> last business day
        last = pd.to_datetime(df["Date"].dropna().max()) if (df is not None and "Date" in df.columns) else pd.NaT
        if pd.isna(last):
            last = _last_business_day(pd.Timestamp.utcnow())
        else:
            last = _last_business_day(last)
        self.today = last  # pandas Timestamp

def _load_state(path: Path) -> pd.DataFrame:
    p = Path(path)
    if p.exists():
        df = pd.read_csv(p, low_memory=False)
    else:
        cols = ["Ticker","EntryDate","EntryPrice","Stop","Target","Status","Notes","ExitDate","ExitPrice"]
        df = pd.DataFrame(columns=cols)
    # enforce dtypes
    for c in ["EntryDate","ExitDate"]:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")
    return df

def _ensure_cols(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c not in df.columns:
            df[c] = pd.NA
    return df

def _save_csv(df: pd.DataFrame, path: Path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)

def run_strategy(ctx: Ctx,
                 state_path: str | Path,
                 out_dir: str | Path,
                 signal_fn):
    state_path = Path(state_path)
    out_dir = Path(out_dir)

    state = _load_state(state_path)

    # signal_fn(ctx, state, df_or_none) -> (entries:list[dict], exits:list[dict], df_full:pd.DataFrame|None)
    entries, exits, _ = signal_fn(ctx, state, None)

    # ---- add entries ----
    if entries:
        add = pd.DataFrame(entries)
        add = _ensure_cols(add, ["Ticker","EntryDate","EntryPrice","Stop","Target","Status","Notes"])
        add["EntryDate"] = pd.to_datetime(add["EntryDate"], errors="coerce")
        add.loc[add["EntryDate"].isna(), "EntryDate"] = ctx.today
        add["EntryDate"] = add["EntryDate"].dt.normalize()
        add["Status"] = add["Status"].fillna("open")
        state = pd.concat(
            [state, add[["Ticker","EntryDate","EntryPrice","Stop","Target","Status","Notes"]]],
            ignore_index=True
        )

    # ---- apply exits ----
    if exits and not state.empty:
        ex = pd.DataFrame(exits)
        ex = _ensure_cols(ex, ["Ticker","ExitPrice","Notes","ExitDate"])
        ex["ExitDate"] = pd.to_datetime(ex["ExitDate"], errors="coerce")
        ex.loc[ex["ExitDate"].isna(), "ExitDate"] = ctx.today
        ex["ExitDate"] = ex["ExitDate"].dt.normalize()

        # ensure state ExitDate/ExitPrice dtypes before assignment
        if "ExitDate" not in state.columns:
            state["ExitDate"] = pd.NaT
        else:
            state["ExitDate"] = pd.to_datetime(state["ExitDate"], errors="coerce")
        if "ExitPrice" not in state.columns:
            state["ExitPrice"] = pd.NA

        for _, r in ex.iterrows():
            mask = (state["Ticker"] == r["Ticker"]) & (state["Status"] == "open")
            if not mask.any():
                continue
            idx = state.index[mask][0]
            state.loc[idx, "Status"] = "closed"
            state.loc[idx, "ExitDate"] = r["ExitDate"]
            if pd.notna(r.get("ExitPrice", pd.NA)):
                state.loc[idx, "ExitPrice"] = r["ExitPrice"]
            prev = str(state.loc[idx, "Notes"]) if "Notes" in state.columns else ""
            addn = str(r.get("Notes", ""))
            state.loc[idx, "Notes"] = (prev + ("; " if prev and addn else "") + addn).strip()

    # ---- outputs ----
    day = ctx.today.date().isoformat()
    entries_df = pd.DataFrame(entries)
    exits_df = pd.DataFrame(exits)
    open_df = state[state.get("Status", pd.Series(dtype=str)) == "open"].copy()

    _save_csv(entries_df, out_dir / f"entries_{day}.csv")
    _save_csv(exits_df,   out_dir / f"exits_{day}.csv")
    _save_csv(open_df,    out_dir / f"open_positions_{day}.csv")
    _save_csv(state,      state_path)

    print(f"Today: {day}")
    print(f"Entries: {len(entries_df)} -> {out_dir / f'entries_{day}.csv'}")
    print(f"Exits:   {len(exits_df)} -> {out_dir / f'exits_{day}.csv'}")
    print(f"Open:    {len(open_df)} -> {out_dir / f'open_positions_{day}.csv'}")
    print(f"State:   {state_path}")