import pandas as pd
from pathlib import Path

def run_strategy(ctx, signal_fn):
    data_path = ctx["data_path"]
    df = pd.read_csv(data_path, parse_dates=["Date"])

    state_path = ctx["state_path"]
    state = pd.read_csv(state_path) if Path(state_path).exists() else pd.DataFrame()

    entries, exits = signal_fn(ctx, state, df)

    if not entries.empty:
        entries.to_csv(ctx["out_entries"], index=False)
    if not exits.empty:
        exits.to_csv(ctx["out_exits"], index=False)

    open_pos = pd.concat([entries, exits]).sort_values("Date")
    open_pos.to_csv(ctx["out_open"], index=False)

    add = pd.concat([entries, exits])
    if "EntryDate" not in add.columns:
        add["EntryDate"] = pd.NaT

    add.to_csv(state_path, index=False)

def Ctx(universe_file, include_file, strategy_name):
    return {
        "data_path": "data/combined.csv",
        "state_path": f"state/{strategy_name}_state.csv",
        "out_entries": f"outputs/{strategy_name}/entries.csv",
        "out_exits": f"outputs/{strategy_name}/exits.csv",
        "out_open": f"outputs/{strategy_name}/open_positions.csv",
        "strategy": strategy_name,
    }