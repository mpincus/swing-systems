import pandas as pd
from pathlib import Path
import yaml

def load_config(p: str | Path) -> dict:
    with open(p, "r") as f:
        return yaml.safe_load(f) or {}

def load_data(data_path: str | Path, include: list[str] | None) -> pd.DataFrame:
    df = pd.read_csv(data_path, parse_dates=["Date"], low_memory=False, dtype={"Ticker": "string"})
    # force numerics
    for col in ["Open", "High", "Low", "Close", "Volume"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["Date", "Open", "High", "Low", "Close"])  # keep NaN Volume if any
    if include:
        df = df[df["Ticker"].isin(include)]
    return df.sort_values(["Ticker", "Date"]).reset_index(drop=True)

def read_include_file(p: str | Path) -> list[str]:
    p = Path(p)
    if not p.exists():
        return []
    if p.suffix.lower() in {".yml", ".yaml"}:
        with open(p, "r") as f:
            cfg = yaml.safe_load(f) or {}
        u = cfg.get("universe", [])
        return sorted({str(t).strip().upper() for t in u if str(t).strip()})
    out = []
    for line in p.read_text().splitlines():
        s = line.split("#", 1)[0].strip()
        if s:
            out.append(s.upper())
    return sorted({*out})