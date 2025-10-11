from dataclasses import dataclass
from pathlib import Path
import pandas as pd

STATE_COLS = ["Ticker","EntryDate","EntryPrice","Status","ExitDate","ExitPrice","Notes"]

@dataclass
class RunContext:
    df: pd.DataFrame
    include: list[str]
    out_dir: Path
    state_path: Path
    today: pd.Timestamp

def load_state(path: Path) -> pd.DataFrame:
    if path.exists():
        st = pd.read_csv(path, parse_dates=["EntryDate","ExitDate"])
    else:
        st = pd.DataFrame(columns=STATE_COLS)
    return st

def save_state(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    out = df.copy()
    if not out.empty:
        out["EntryDate"] = pd.to_datetime(out["EntryDate"]).dt.date
        out["ExitDate"] = pd.to_datetime(out["ExitDate"]).dt.date
    out.to_csv(path, index=False)

def run_strategy(ctx: RunContext, signal_fn):
    ctx.out_dir.mkdir(parents=True, exist_ok=True)
    state = load_state(ctx.state_path)
    dft = ctx.df[ctx.df["Date"] == ctx.today]

    entries, exits = signal_fn(ctx, state, dft)

    if exits:
        ex_df = pd.DataFrame(exits)
        for _, ex in ex_df.iterrows():
            m = (state["Ticker"] == ex["Ticker"]) & (state["Status"] == "open")
            state.loc[m, ["Status","ExitDate","ExitPrice","Notes"]] = [
                "closed", ctx.today, ex["ExitPrice"], ex.get("Notes","")
            ]
    else:
        ex_df = pd.DataFrame(columns=["Ticker","ExitPrice","Notes"])

    if entries:
        en_df = pd.DataFrame(entries)
        en_df["Status"] = "open"
        en_df["ExitDate"] = pd.NaT
        en_df["ExitPrice"] = pd.NA
        en_df["Notes"] = en_df.get("Notes", "")
        state = pd.concat([state, en_df[STATE_COLS]], ignore_index=True)
    else:
        en_df = pd.DataFrame(columns=["Ticker","EntryDate","EntryPrice","Notes"])

    entries_path = ctx.out_dir / f"entries_{ctx.today.date()}.csv"
    exits_path = ctx.out_dir / f"exits_{ctx.today.date()}.csv"
    open_path = ctx.out_dir / f"open_positions_{ctx.today.date()}.csv"

    en_df.to_csv(entries_path, index=False)
    ex_df.to_csv(exits_path, index=False)
    state[state["Status"]=="open"].to_csv(open_path, index=False)
    save_state(state, ctx.state_path)

    print(f"Today: {ctx.today.date()}")
    print(f"Entries: {len(en_df)} -> {entries_path}")
    print(f"Exits:   {len(ex_df)} -> {exits_path}")
    print(f"Open:    {state['Status'].eq('open').sum()} -> {open_path}")
    print(f"State:   {ctx.state_path}")