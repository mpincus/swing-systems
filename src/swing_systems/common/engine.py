import pandas as pd
from pathlib import Path

class Ctx:
    def __init__(self, df, include_file=None, strategy_name=None):
        self.df = df
        self.include_file = include_file
        self.strategy_name = strategy_name
        # robust "today"
        if df is None or df.empty or pd.isna(df["Date"].max()):
            self.today = pd.Timestamp.utcnow().normalize()
        else:
            self.today = pd.to_datetime(df["Date"].max(), utc=True).normalize()

def _ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

def _as_df(x):
    if x is None:
        return pd.DataFrame()
    if isinstance(x, pd.DataFrame):
        return x
    return pd.DataFrame(x)

def run_strategy(ctx: Ctx, state_path: Path, out_dir: Path, signal_fn):
    _ensure_dir(out_dir)
    _ensure_dir(state_path.parent)

    # load prior state safely
    if state_path.exists():
        try:
            state = pd.read_csv(state_path, low_memory=False)
        except Exception:
            state = pd.DataFrame()
    else:
        state = pd.DataFrame()

    # call strategy; accept 2-tuple or 3-tuple
    result = signal_fn(ctx, state, ctx.df)
    if not isinstance(result, tuple):
        raise ValueError("signal_fn must return a tuple")
    if len(result) == 3:
        entries, exits, new_state = result
    elif len(result) == 2:
        entries, exits = result
        new_state = state
    else:
        raise ValueError(f"signal_fn returned {len(result)} values; expected 2 or 3")

    entries = _as_df(entries)
    exits   = _as_df(exits)
    new_state = _as_df(new_state)

    day = str(ctx.today.date())
    # write outputs
    (out_dir / f"entries_{day}.csv").write_text(entries.to_csv(index=False))
    (out_dir / f"exits_{day}.csv").write_text(exits.to_csv(index=False))
    new_state.to_csv(state_path, index=False)

    print(f"Today: {day}")
    print(f"Entries: {len(entries)} -> {out_dir}/entries_{day}.csv")
    print(f"Exits:   {len(exits)} -> {out_dir}/exits_{day}.csv")
    print(f"State:   {state_path}")

    return entries, exits, new_state