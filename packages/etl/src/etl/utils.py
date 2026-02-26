from __future__ import annotations

import re
import unicodedata


def clean_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def normalize_column_name(value: object) -> str:
    text = clean_text(value).lower()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(c for c in text if not unicodedata.combining(c))
    text = re.sub(r"[^a-z0-9]+", "_", text)
    return text.strip("_")


def as_hs(value: object, length: int = 10) -> str:
    raw = re.sub(r"\D", "", clean_text(value))
    return raw.zfill(length)[:length]


def parse_periodo(periodo: str) -> tuple[int, int]:
    match = re.search(r"(\d{4})\s*/\s*(\d{1,2})", clean_text(periodo))
    if not match:
        return 0, 0
    return int(match.group(1)), int(match.group(2))


def detect_header_row(path: str, sheet_name: str, key_columns: list[str], search_rows: int = 40) -> int:
    import pandas as pd

    probe = pd.read_excel(path, sheet_name=sheet_name, header=None, nrows=search_rows)
    lowered_keys = {normalize_column_name(k) for k in key_columns}
    for idx in range(len(probe)):
        row_values = {normalize_column_name(v) for v in probe.iloc[idx].tolist()}
        if lowered_keys.issubset(row_values):
            return idx
    return 0


def first_sheet_with_headers(path: str, key_columns: list[str]) -> tuple[str, int]:
    import pandas as pd

    xls = pd.ExcelFile(path)
    for sheet in xls.sheet_names:
        idx = detect_header_row(path, sheet, key_columns)
        probe = pd.read_excel(path, sheet_name=sheet, header=idx, nrows=1)
        normalized = {normalize_column_name(c) for c in probe.columns}
        if {normalize_column_name(k) for k in key_columns}.issubset(normalized):
            return sheet, idx
    if xls.sheet_names:
        return xls.sheet_names[0], 0
    raise ValueError(f"No sheets found in workbook: {path}")
