import argparse
from pathlib import Path
from ..common.io import load_config
from ..common.engine import RunContext, run_strategy
from ..strategies.rsi2_5_70_sso import prepare, signals
from ._runner_common import load_data, read_include_file

if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--universe', default='configs/universe.yaml')
    ap.add_argument('--include-file', default='configs/generated/rsi2_5_70_sso.txt')
    args = ap.parse_args()

    uni = load_config(args.universe)
    include = read_include_file(args.include_file)
    df = load_data(uni['data_path'], include)
    dfp = prepare(df)
    today = dfp['Date'].max()

    ctx = RunContext(dfp, include, Path(uni['out_root'])/'rsi2_5_70_sso', Path(uni['state_root'])/'rsi2_5_70_sso_state.csv', today)
    run_strategy(ctx, lambda c,s,d: signals(c,s,d))