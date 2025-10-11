import pandas as pd
from pathlib import Path
from ._runner_common import load_config, load_data, read_include_file
from ..common.engine import Ctx, run_strategy
from ..strategies.double_seven import signals as ds_signals, prepare as ds_prepare

cfg = load_config("configs/universe.yaml")
data_path = cfg["data_path"]
out_dir = Path(cfg.get("out_root", "outputs")) / "double_seven"
state_path = Path(cfg.get("state_root", "state")) / "double_seven_state.csv"

import argparse
ap = argparse.ArgumentParser()
ap.add_argument("--universe", default="configs/universe.yaml")
ap.add_argument("--include-file", default=None)
args = ap.parse_args()

include = read_include_file(args.include_file) if args.include_file else []
df = load_data(data_path, include)
ctx = Ctx(df)

# strategy adapter to match engine API
def adapter(context, state, _):
    dfp = ds_prepare(df)
    e, x = ds_signals(context, state, dfp)
    return e, x, dfp

run_strategy(ctx, state_path, out_dir, adapter)