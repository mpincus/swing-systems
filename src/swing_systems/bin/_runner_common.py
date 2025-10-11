import pandas as pd
from pathlib import Path
from ..common.io import load_config

def load_data(data_path: str | Path, include: list[str]) -> pd.DataFrame:
    df = pd.read_csv(data_path, parse_dates=["Date"], low_memory=False, dtype={"Ticker": "string"})
    if include:
        df = df[df["Ticker"].isin(include)]
    return df.sort_values(["Ticker", "Date"]).reset_index(drop=True)