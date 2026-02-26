from __future__ import annotations

import re
import pandas as pd


def clean_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def as_hs(value: object, length: int = 10) -> str:
    raw = re.sub(r"\D", "", clean_text(value))
    return raw.zfill(length)[:length]


def parse_periodo(periodo: str) -> tuple[int, int]:
    match = re.search(r"(\d{4})\s*/\s*(\d{1,2})", clean_text(periodo))
    if not match:
        return 0, 0
    return int(match.group(1)), int(match.group(2))


def detect_header_row(path: str, sheet_name: str, key_columns: list[str], search_rows: int = 30) -> int:
    probe = pd.read_excel(path, sheet_name=sheet_name, header=None, nrows=search_rows)
    lowered_keys = {k.lower() for k in key_columns}
    for idx in range(len(probe)):
        row_values = {clean_text(v).lower() for v in probe.iloc[idx].tolist()}
        if lowered_keys.issubset(row_values):
            return idx
    return 0
