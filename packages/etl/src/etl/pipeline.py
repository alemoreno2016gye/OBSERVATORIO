from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import pandas as pd
import duckdb

from .utils import (
    as_hs,
    parse_periodo,
    detect_header_row,
    clean_text,
    normalize_column_name,
    first_sheet_with_headers,
)


@dataclass
class ETLConfig:
    raw_dir: Path
    processed_dir: Path
    duckdb_path: Path


class ObservatorioETL:
    def __init__(self, config: ETLConfig):
        self.config = config
        self.config.processed_dir.mkdir(parents=True, exist_ok=True)

    def run(self) -> None:
        dim_hs = self._build_dim_hs()
        dim_sector = self._build_dim_sector()
        fact_exports = self._build_fact_trade(is_import=False, dim_hs=dim_hs)
        fact_imports = self._build_fact_trade(is_import=True, dim_hs=dim_hs)
        fact_trademap = self._build_fact_trademap()

        self._save_parquet("dim_hs", dim_hs)
        self._save_parquet("dim_sector", dim_sector)
        self._save_parquet("fact_exports", fact_exports)
        self._save_parquet("fact_imports", fact_imports)
        self._save_parquet("fact_trademap", fact_trademap)
        self._materialize_duckdb()

    def _find_excel_by_keyword(self, keyword: str) -> Path:
        matches = sorted(
            [p for p in self.config.raw_dir.glob("*.xlsx") if keyword in p.stem.lower()],
            key=lambda p: p.name.lower(),
        )
        if not matches:
            raise FileNotFoundError(f"No se encontró archivo Excel con keyword '{keyword}' en {self.config.raw_dir}")
        return matches[0]

    def _find_trade_files(self, keyword: str) -> list[Path]:
        files = [p for p in self.config.raw_dir.rglob("*.xlsx") if keyword in p.as_posix().lower()]
        files = [p for p in files if p.is_file()]
        return sorted(files, key=lambda p: p.as_posix().lower())

    def _build_dim_hs(self) -> pd.DataFrame:
        path = self._find_excel_by_keyword("diccionario")
        sheet, header_idx = first_sheet_with_headers(str(path), ["hs10", "descripcion_final"])
        df = pd.read_excel(path, sheet_name=sheet, header=header_idx)
        cols = {normalize_column_name(c): c for c in df.columns}
        hs_col = cols.get("hs10") or cols.get("codigo_subpartida_10")
        if hs_col is None:
            raise ValueError(f"No se encontró columna hs10 en {path}")

        out = pd.DataFrame()
        out["hs10"] = df[hs_col].map(lambda v: as_hs(v, 10))
        out["hs8"] = out["hs10"].str[:8]
        out["hs6"] = out["hs10"].str[:6]
        out["hs4"] = out["hs10"].str[:4]
        out["hs2"] = out["hs10"].str[:2]
        desc_col = cols.get("descripcion_final") or cols.get("descripcion") or hs_col
        type_col = cols.get("tipo_elemento") or desc_col
        out["descripcion_final"] = df[desc_col].map(clean_text)
        out["tipo_elemento"] = df[type_col].map(clean_text)
        return out.drop_duplicates(subset=["hs10"])

    def _build_dim_sector(self) -> pd.DataFrame:
        path = self._find_excel_by_keyword("sector")
        df = pd.read_excel(path)
        cols = {normalize_column_name(c): c for c in df.columns}
        cap_col = cols.get("capitulos") or cols.get("capitulo") or list(df.columns)[1]
        sec_col = cols.get("seccion") or list(df.columns)[0]
        sector_col = cols.get("sector") or list(df.columns)[-1]

        rows = []
        for _, row in df.iterrows():
            raw_caps = clean_text(row[cap_col]).replace(" ", "")
            for part in raw_caps.split(","):
                if "-" in part:
                    start, end = part.split("-", 1)
                    for c in range(int(start), int(end) + 1):
                        rows.append((str(c).zfill(2), clean_text(row[sec_col]), clean_text(row[sector_col])))
                elif part:
                    rows.append((str(int(part)).zfill(2), clean_text(row[sec_col]), clean_text(row[sector_col])))
        return pd.DataFrame(rows, columns=["hs2", "seccion", "sector_industria"]).drop_duplicates()

    def _build_fact_trade(self, is_import: bool, dim_hs: pd.DataFrame) -> pd.DataFrame:
        keyword = "import" if is_import else "export"
        paths = self._find_trade_files(keyword)
        if not paths:
            raise FileNotFoundError(f"No se encontraron archivos {keyword}*.xlsx en {self.config.raw_dir}")

        chunks: list[pd.DataFrame] = []
        for path in paths:
            chunks.append(self._read_trade_file(path, is_import=is_import, dim_hs=dim_hs))
        return pd.concat(chunks, ignore_index=True)

    def _read_trade_file(self, path: Path, is_import: bool, dim_hs: pd.DataFrame) -> pd.DataFrame:
        sheet, header_idx = first_sheet_with_headers(str(path), ["Periodo", "Codigo_Subpartida_10"])
        _ = detect_header_row(str(path), sheet, ["Periodo", "Codigo_Subpartida_10"])
        df = pd.read_excel(path, sheet_name=sheet, header=header_idx)
        c = {normalize_column_name(col): col for col in df.columns}

        hs = df[c["codigo_subpartida_10"]].map(lambda v: as_hs(v, 10))
        year_month = df[c["periodo"]].map(lambda v: parse_periodo(clean_text(v)))

        out = pd.DataFrame()
        out["year"] = year_month.map(lambda x: x[0])
        out["month"] = year_month.map(lambda x: x[1])
        out["periodo_raw"] = df[c["periodo"]].map(clean_text)
        out["hs10"] = hs
        out["hs6"] = hs.str[:6]
        out["hs4"] = hs.str[:4]
        out["hs2"] = hs.str[:2]

        code_key = "codigo_pais_origen" if is_import else "codigo_pais_destino"
        name_key = "pais_origen" if is_import else "pais_destino"

        out["country_code"] = df[c.get(code_key)].map(clean_text)
        out["country_name"] = df[c.get(name_key)].map(clean_text)
        out["tm_peso_neto"] = pd.to_numeric(df[c.get("tm_peso_neto")], errors="coerce").fillna(0)
        out["fob"] = pd.to_numeric(df[c.get("fob")], errors="coerce").fillna(0)
        if is_import:
            out["cif"] = pd.to_numeric(df[c.get("cif")], errors="coerce").fillna(0)

        enriched = out.merge(dim_hs[["hs10", "descripcion_final"]], on="hs10", how="left")
        enriched["source_file"] = path.name
        return enriched

    def _build_fact_trademap(self) -> pd.DataFrame:
        path = self._find_excel_by_keyword("trademap")
        df = pd.read_excel(path, sheet_name=0)
        c = {normalize_column_name(col): col for col in df.columns}
        out = pd.DataFrame()
        out["producto"] = df[c.get("producto")].map(clean_text)
        out["pais"] = df[c.get("pais")].map(clean_text)
        out["year"] = pd.to_numeric(df[c.get("year")], errors="coerce").fillna(0).astype(int)
        out["value"] = pd.to_numeric(df[c.get("value")], errors="coerce").fillna(0)
        return out

    def _save_parquet(self, name: str, df: pd.DataFrame) -> None:
        df.to_parquet(self.config.processed_dir / f"{name}.parquet", index=False)

    def _materialize_duckdb(self) -> None:
        conn = duckdb.connect(str(self.config.duckdb_path))
        try:
            for table in ["dim_hs", "dim_sector", "fact_exports", "fact_imports", "fact_trademap"]:
                parquet_path = self.config.processed_dir / f"{table}.parquet"
                conn.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM read_parquet('{parquet_path}')")
        finally:
            conn.close()
