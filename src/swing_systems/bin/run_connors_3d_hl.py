import pandas as pd
from pathlib import Path
from ._runner_common import load_config, load_data, read_include_file
from ..common.engine import Ctx, run_strategy
from ..strategies.connors_3d_hl import signals as st_signals, prepare as st_prepare

cfg = load_config("configs/universe.yaml")
data_path = cfg["data_path"]
out_dir = Path(cfg.get("out_root", "outputs")) / "connors_3d_hl"
state_path = Path(cfg.get("state_root", "state")) / "connors_3d_hl_state.csv"

import argparse
ap = argparse.ArgumentParser()
ap.add_argument("--universe", default="configs/universe.yaml")
ap.add_argument("--include-file", default=None)
args = ap.parse_args()

include = read_include_file(args.include_file) if args.include_file else []
df = load_data(data_path, include)
ctx = Ctx(df)

def adapter(context, state, _):
    dfp = st_prepare(df)
    e, x = st_signals(context, state, dfp)
    return e, x, dfp

run_strategy(ctx, state_path, out_dir, adapter)