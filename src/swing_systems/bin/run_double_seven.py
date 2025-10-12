import argparse, pandas as pd, yaml
from pathlib import Path
from ..common.engine import Ctx, run_strategy
from ..strategies.double_seven import signals as st_signals

DEF_DATA = "data/combined.csv"
STRAT = "double_seven"

def load_df(universe_yaml: str, include_file: str | None) -> pd.DataFrame:
    with open(universe_yaml, "r") as f:
        uni = yaml.safe_load(f) or {}
    data_path = Path(uni.get("data_path", DEF_DATA))
    df = pd.read_csv(data_path, parse_dates=["Date"], low_memory=False)
    if include_file:
        with open(include_file, "r") as f:
            inc = yaml.safe_load(f) or {}
        incl = set(inc.get("universe", []))
        if incl:
            df = df[df["Ticker"].isin(incl)]
    return df

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--universe", required=True)
    ap.add_argument("--include-file", default=None)
    args = ap.parse_args()

    df = load_df(args.universe, args.include_file)
    ctx = Ctx(df)
    out_dir = Path("outputs") / STRAT
    state_path = Path("state") / f"{STRAT}_state.csv"

    def adapter(context, state, _unused):
        return st_signals(context, state, df)

    run_strategy(ctx, state_path, out_dir, adapter)

if __name__ == "__main__":
    main()
