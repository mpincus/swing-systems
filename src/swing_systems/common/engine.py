import pandas as pd
from pathlib import Path

class Ctx:
    def __init__(self, df: pd.DataFrame):
        self.today = pd.to_datetime(df["Date"].max()).date()

def _load_state(path: Path) -> pd.DataFrame:
    if path.exists():
        return pd.read_csv(path, parse_dates=["EntryDate"], low_memory=False)
    return pd.DataFrame(columns=["Ticker","EntryDate","EntryPrice","Stop","Target","Status","Notes"])

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

    # signal_fn(ctx, state, dft) must return (entries, exits)
    # entries/exits are lists of dicts with at least Ticker, EntryPrice/ExitPrice, Notes
    dft = signal_fn.__self__ if hasattr(signal_fn, "__self__") else None  # not used, but kept for API symmetry
    entries, exits, dft_full = signal_fn(ctx, state, None) if dft is None else signal_fn(ctx, state, dft)

    # Update state
    if entries:
        add = pd.DataFrame(entries)
        if "EntryDate" not in add.columns: