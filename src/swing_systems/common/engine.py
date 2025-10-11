import pandas as pd
from pathlib import Path

class Ctx:
    def __init__(self, df: pd.DataFrame):
        # Use pandas Timestamp for arithmetic compatibility
        self.today = pd.to_datetime(df["Date"].max()).normalize()  # Timestamp @ 00:00 UTC

def _load_state(path: Path) -> pd.DataFrame:
    if Path(path).exists():
        return pd.read_csv(path, parse_dates=["EntryDate"], low_memory=False)
    return pd.DataFrame(columns=["Ticker","EntryDate","EntryPrice","Stop","Target","Status","Notes"])

def _ensure_cols(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c not in df.columns:
            df[c] = pd.NA
    return df

def _save_csv(df: pd.DataFrame, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)

def run_strategy(ctx: Ctx,
                 state_path: str | Path,
                 out_dir: str | Path,
                 signal_fn):
    state_path = Path(state_path)
    out_dir = Path(out_dir)

    state = _load_state(state_path)

    # signal_fn must return (entries:list[dict], exits:list[dict], df_full:pd.DataFrame)
    entries, exits, _ = signal_fn(ctx, state, None)

    # ---- update state ----
    if entries:
        add = pd.DataFrame(entries)
        add = _ensure_cols(add, ["Ticker","EntryDate","EntryPrice","Stop","Target","Status","Notes"])
        if add["EntryDate"].isna().any():
            add["EntryDate"] = ctx.today  # Timestamp
        add["Status"] = add["Status"].fillna("open")
        state = pd.concat([state, add[["Ticker","EntryDate","EntryPrice","Stop","Target","Status","Notes"]]], ignore_index=True)

    if exits and not state.empty:
        ex = pd.DataFrame(exits)
        ex["ExitDate"] = ctx.today  # Timestamp
        for _, r in ex.iterrows():
            mask = (state["Ticker"] == r["Ticker"]) & (state["Status"] == "open")
            if not mask.any():
                continue
            idx = state.index[mask][0]
            state.loc[idx, "Status"] = "closed"
            state.loc[idx, "ExitDate"] = r.get("ExitDate", ctx.today)
            if "ExitPrice" in r:
                state.loc[idx, "ExitPrice"] = r["ExitPrice"]
            prev = str(state.loc[idx, "Notes"]) if "Notes" in state.columns else ""
            addn = str(r.get("Notes", ""))
            state.loc[idx, "Notes"] = (prev + ("; " if prev and addn else "") + addn).strip()

    # ---- outputs ----
    day = ctx.today.date().isoformat()  # YYYY-MM-DD
    entries_df = pd.DataFrame(entries)
    exits_df = pd.DataFrame(exits)
    open_df = state[state["Status"] == "open"].copy()

    _save_csv(entries_df, out_dir / f"entries_{day}.csv")
    _save_csv(exits_df,   out_dir / f"exits_{day}.csv")
    _save_csv(open_df,    out_dir / f"open_positions_{day}.csv")
    _save_csv(state,      state_path)

    print(f"Today: {day}")
    print(f"Entries: {len(entries_df)} -> {out_dir / f'entries_{day}.csv'}")
    print(f"Exits:   {len(exits_df)} -> {out_dir / f'exits_{day}.csv'}")
    print(f"Open:    {len(open_df)} -> {out_dir / f'open_positions_{day}.csv'}")
    print(f"State:   {state_path}")