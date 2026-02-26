from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import pandas as pd
import duckdb

from .utils import as_hs, parse_periodo, detect_header_row, clean_text


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
        fact_exports = self._build_fact_trade("exportaciones", is_import=False, dim_hs=dim_hs)
        fact_imports = self._build_fact_trade("importaciones", is_import=True, dim_hs=dim_hs)
        fact_trademap = self._build_fact_trademap()

        self._save_parquet("dim_hs", dim_hs)
        self._save_parquet("dim_sector", dim_sector)
        self._save_parquet("fact_exports", fact_exports)
        self._save_parquet("fact_imports", fact_imports)
        self._save_parquet("fact_trademap", fact_trademap)
        self._materialize_duckdb()

    def _build_dim_hs(self) -> pd.DataFrame:
        path = self.config.raw_dir / "diccionario_ecuador.xlsx"
        df = pd.read_excel(path, sheet_name="diccionario")
        cols = {c.lower(): c for c in df.columns}
        out = pd.DataFrame()
        out["hs10"] = df[cols.get("hs10", "hs10")].map(lambda v: as_hs(v, 10))
        out["hs8"] = out["hs10"].str[:8]
        out["hs6"] = out["hs10"].str[:6]
        out["hs4"] = out["hs10"].str[:4]
        out["hs2"] = out["hs10"].str[:2]
        desc_col = cols.get("descripcion_final", next(iter(df.columns)))
        type_col = cols.get("tipo_elemento", desc_col)
        out["descripcion_final"] = df[desc_col].map(clean_text)
        out["tipo_elemento"] = df[type_col].map(clean_text)
        return out.drop_duplicates(subset=["hs10"])

    def _build_dim_sector(self) -> pd.DataFrame:
        path = self.config.raw_dir / "SECTORES.xlsx"
        df = pd.read_excel(path)
        cols = {c.lower(): c for c in df.columns}
        cap_col = cols.get("capítulos", list(df.columns)[1])
        sec_col = cols.get("sección", list(df.columns)[0])
        sector_col = cols.get("sector", list(df.columns)[-1])

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

    def _build_fact_trade(self, kind: str, is_import: bool, dim_hs: pd.DataFrame) -> pd.DataFrame:
        path = self.config.raw_dir / f"{kind}.xlsx"
        header_idx = detect_header_row(str(path), "Columnas", ["Periodo", "Codigo_Subpartida_10"])
        df = pd.read_excel(path, sheet_name="Columnas", header=header_idx)
        c = {col.lower(): col for col in df.columns}

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

        code_col = c.get("codigo_pais_origen") if is_import else c.get("codigo_pais_destino")
        name_col = c.get("pais_origen") if is_import else c.get("pais_destino")
        out["country_code"] = df[code_col].map(clean_text)
        out["country_name"] = df[name_col].map(clean_text)
        out["tm_peso_neto"] = pd.to_numeric(df[c.get("tm_peso_neto")], errors="coerce").fillna(0)
        out["fob"] = pd.to_numeric(df[c.get("fob")], errors="coerce").fillna(0)
        if is_import:
            out["cif"] = pd.to_numeric(df[c.get("cif")], errors="coerce").fillna(0)

        enriched = out.merge(dim_hs[["hs10", "descripcion_final"]], on="hs10", how="left")
        return enriched

    def _build_fact_trademap(self) -> pd.DataFrame:
        path = self.config.raw_dir / "panel_trademap.xlsx"
        df = pd.read_excel(path, sheet_name=0)
        c = {col.lower(): col for col in df.columns}
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
