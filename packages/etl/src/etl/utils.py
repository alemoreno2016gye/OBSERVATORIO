from __future__ import annotations

import logging
import re
import unicodedata
from typing import Iterable

logger = logging.getLogger(__name__)

DASH_TRANSLATION = str.maketrans({"–": "-", "—": "-", "−": "-", "‒": "-"})


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


def normalize_text_value(value: object) -> str:
    text = clean_text(value)
    text = text.replace("\u200b", "").replace("\ufeff", "")
    text = text.translate(DASH_TRANSLATION)
    text = re.sub(r"^[-\s]+", "", text)
    text = re.sub(r"[\.;:,\s]+$", "", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def as_hs(value: object, length: int = 10) -> str:
    raw = re.sub(r"\D", "", clean_text(value))
    return raw.zfill(length)[:length]


def parse_periodo(periodo: str) -> tuple[int, int]:
    text = normalize_text_value(periodo)
    match = re.search(r"(19\d{2}|20\d{2})\s*/\s*(1[0-2]|0?[1-9])", text)
    if not match:
        match = re.search(r"(19\d{2}|20\d{2})[-_\s]+(1[0-2]|0?[1-9])", text)
    if not match:
        logger.warning("Periodo no parseable: %s", periodo)
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


def resolve_column(df_columns: Iterable[object], aliases: list[str]) -> str | None:
    cols = {normalize_column_name(c): str(c) for c in df_columns}
    for alias in aliases:
        key = normalize_column_name(alias)
        if key in cols:
            return cols[key]
    return None


def expand_chapter_token(token: str) -> list[str]:
    text = normalize_text_value(token).replace(" ", "")
    if not text:
        return []
    text = text.translate(DASH_TRANSLATION)
    if "-" in text:
        parts = [p for p in text.split("-") if p]
        if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
            start, end = int(parts[0]), int(parts[1])
            if start <= end:
                return [str(v).zfill(2) for v in range(start, end + 1)]
            logger.warning("Rango de capítulos invertido: %s", token)
            return []
        logger.warning("Token de rango inválido en capítulos: %s", token)
        return []
    if text.isdigit():
        return [str(int(text)).zfill(2)]
    logger.warning("Token de capítulo inválido: %s", token)
    return []
