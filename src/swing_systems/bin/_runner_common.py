import pandas as pd
from pathlib import Path
import yaml

def load_config(p: str | Path) -> dict:
    with open(p, "r") as f:
        return yaml.safe_load(f) or {}

def load_data(data_path: str | Path, include: list[str] | None) -> pd.DataFrame:
    df = pd.read_csv(data_path, parse_dates=["Date"], low_memory=False, dtype={"Ticker": "string"})
    if include:
        df = df[df["Ticker"].isin(include)]
    return df.sort_values(["Ticker", "Date"]).reset_index(drop=True)

def read_include_file(p: str | Path) -> list[str]:
    """
    Accepts YAML or plain text.
    YAML: expects {"universe": ["AAPL","MSFT",...]}
    TXT: one ticker per line (comments with # allowed)
    """
    p = Path(p)
    if not p.exists():
        return []
    if p.suffix.lower() in {".yml", ".yaml"}:
        cfg = load_config(p)
        u = cfg.get("universe", [])
        # normalize to upper and strip
        return sorted({str(t).strip().upper() for t in u if str(t).strip()})
    # text fallback
    out = []
    for line in p.read_text().splitlines():
        s = line.split("#", 1)[0].strip()
        if s:
            out.append(s.upper())
    return sorted({*out})