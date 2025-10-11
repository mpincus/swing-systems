import pandas as pd
from pathlib import Path

def load_data(data_path: str | Path, include: list[str]) -> pd.DataFrame:
    df = pd.read_csv(data_path, parse_dates=['Date'])
    if include:
        df = df[df['Ticker'].isin(include)]
    return df.sort_values(['Ticker','Date']).reset_index(drop=True)

def read_include_file(path: str | None) -> list[str]:
    p = Path(path)
    if p.exists():
        return [l.strip().upper() for l in p.read_text().splitlines() if l.strip()]
    return []