from __future__ import annotations

from dataclasses import dataclass
import logging
from pathlib import Path

import duckdb
import pandas as pd

from .utils import (
    as_hs,
    clean_text,
    expand_chapter_token,
    first_sheet_with_headers,
    normalize_column_name,
    normalize_text_value,
    parse_periodo,
    resolve_column,
)

logger = logging.getLogger(__name__)


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
        logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")

        dim_hs = self._safe_build("dim_hs", self._build_dim_hs, self._empty_dim_hs)
        dim_sector = self._safe_build("dim_sector", self._build_dim_sector, self._empty_dim_sector)
        fact_exports = self._safe_build(
            "fact_exports",
            lambda: self._build_fact_trade(is_import=False, dim_hs=dim_hs),
            self._empty_fact_exports,
        )
        fact_imports = self._safe_build(
            "fact_imports",
            lambda: self._build_fact_trade(is_import=True, dim_hs=dim_hs),
            self._empty_fact_imports,
        )
        fact_trademap = self._safe_build("fact_trademap", self._build_fact_trademap, self._empty_fact_trademap)

        self._validate_before_kpis(dim_hs, dim_sector, fact_exports, fact_imports)

        self._save_parquet("dim_hs", dim_hs)
        self._save_parquet("dim_sector", dim_sector)
        self._save_parquet("fact_exports", fact_exports)
        self._save_parquet("fact_imports", fact_imports)
        self._save_parquet("fact_trademap", fact_trademap)
        self._materialize_duckdb()

    def _safe_build(self, name: str, builder, fallback):
        try:
            df = builder()
            logger.info("Tabla %s construida: %s filas", name, len(df))
            return df
        except Exception as exc:  # noqa: BLE001
            logger.warning("Fallo construyendo %s: %s", name, exc)
            return fallback()

    def _find_excel_by_keyword(self, keyword: str) -> Path:
        matches = sorted(
            [p for p in self.config.raw_dir.glob("*.xlsx") if keyword in p.stem.lower()],
            key=lambda p: p.name.lower(),
        )
        if not matches:
            raise FileNotFoundError(f"No se encontró archivo Excel con keyword '{keyword}' en {self.config.raw_dir}")
        return matches[0]

    def _find_trade_files(self, keyword: str) -> list[Path]:
        files = [p for p in self.config.raw_dir.rglob("*.xlsx") if keyword in p.as_posix().lower() and p.is_file()]
        return sorted(files, key=lambda p: p.as_posix().lower())

    def _build_dim_hs(self) -> pd.DataFrame:
        path = self._find_excel_by_keyword("diccionario")
        sheet, header_idx = first_sheet_with_headers(str(path), ["hs10", "descripcion_final"])
        df = pd.read_excel(path, sheet_name=sheet, header=header_idx)
        hs_col = resolve_column(df.columns, ["hs10", "codigo_subpartida_10"])
        desc_col = resolve_column(df.columns, ["descripcion_final", "descripcion"])
        type_col = resolve_column(df.columns, ["tipo_elemento", "tipo"])

        if hs_col is None:
            logger.warning("dim_hs sin columna hs10; devolviendo tabla vacía")
            return self._empty_dim_hs()

        out = pd.DataFrame()
        out["hs10"] = df[hs_col].map(lambda v: as_hs(v, 10))
        out["hs8"] = out["hs10"].str[:8]
        out["hs6"] = out["hs10"].str[:6]
        out["hs4"] = out["hs10"].str[:4]
        out["hs2"] = out["hs10"].str[:2]
        out["descripcion_final"] = df[desc_col].map(normalize_text_value) if desc_col else ""
        out["tipo_elemento"] = df[type_col].map(normalize_text_value) if type_col else ""

        invalid = out[~out["hs10"].str.match(r"^\d{10}$", na=False)]
        if not invalid.empty:
            logger.warning("dim_hs contiene %s HS10 inválidos; serán descartados", len(invalid))
        out = out[out["hs10"].str.match(r"^\d{10}$", na=False)]
        return out.drop_duplicates(subset=["hs10"])

    def _build_dim_sector(self) -> pd.DataFrame:
        path = self._find_excel_by_keyword("sector")
        df = pd.read_excel(path)

        cap_col = resolve_column(df.columns, ["capitulos", "capítulos", "capitulo", "capítulo"])
        sec_col = resolve_column(df.columns, ["seccion", "sección"])
        sector_col = resolve_column(df.columns, ["sector", "sector_industria"])

        if cap_col is None:
            logger.warning("dim_sector sin columna capítulos/capitulos")
            return self._empty_dim_sector()

        rows: list[tuple[str, str, str]] = []
        for _, row in df.iterrows():
            raw_caps = normalize_text_value(row.get(cap_col, ""))
            sec = normalize_text_value(row.get(sec_col, "")) if sec_col else ""
            sector = normalize_text_value(row.get(sector_col, "")) if sector_col else ""

            for token in [t for t in raw_caps.split(",") if clean_text(t)]:
                expanded = expand_chapter_token(token)
                if not expanded:
                    logger.warning("Capítulo no parseable en dim_sector: '%s'", token)
                    continue
                for hs2 in expanded:
                    rows.append((hs2, sec, sector))

        out = pd.DataFrame(rows, columns=["hs2", "seccion", "sector_industria"]).drop_duplicates()
        if out.empty:
            logger.warning("dim_sector quedó vacío tras parseo")
        return out

    def _build_fact_trade(self, is_import: bool, dim_hs: pd.DataFrame) -> pd.DataFrame:
        keyword = "import" if is_import else "export"
        paths = self._find_trade_files(keyword)
        if not paths:
            logger.warning("No se encontraron archivos %s*.xlsx en %s", keyword, self.config.raw_dir)
            return self._empty_fact_imports() if is_import else self._empty_fact_exports()

        chunks: list[pd.DataFrame] = []
        for path in paths:
            try:
                chunk = self._read_trade_file(path, is_import=is_import, dim_hs=dim_hs)
                if not chunk.empty:
                    chunks.append(chunk)
            except Exception as exc:  # noqa: BLE001
                logger.warning("Error leyendo %s: %s", path, exc)

        if not chunks:
            return self._empty_fact_imports() if is_import else self._empty_fact_exports()
        return pd.concat(chunks, ignore_index=True)

    def _read_trade_file(self, path: Path, is_import: bool, dim_hs: pd.DataFrame) -> pd.DataFrame:
        sheet, header_idx = first_sheet_with_headers(str(path), ["Periodo", "Codigo_Subpartida_10"])
        df = pd.read_excel(path, sheet_name=sheet, header=header_idx)

        periodo_col = resolve_column(df.columns, ["periodo"])
        hs_col = resolve_column(df.columns, ["codigo_subpartida_10", "hs10"])
        fob_col = resolve_column(df.columns, ["fob", "valor_fob"])
        cif_col = resolve_column(df.columns, ["cif", "valor_cif"])
        tm_col = resolve_column(df.columns, ["tm_peso_neto", "peso_neto", "tm"])

        code_col = resolve_column(df.columns, ["codigo_pais_origen"] if is_import else ["codigo_pais_destino"])
        name_col = resolve_column(df.columns, ["pais_origen", "pais"] if is_import else ["pais_destino", "pais"])

        missing = [
            req
            for req, val in {
                "periodo": periodo_col,
                "hs10": hs_col,
                "fob": fob_col,
                "pais": name_col,
            }.items()
            if val is None
        ]
        if missing:
            logger.warning("Archivo %s sin columnas requeridas %s. Se omite.", path.name, missing)
            return self._empty_fact_imports() if is_import else self._empty_fact_exports()

        hs = df[hs_col].map(lambda v: as_hs(v, 10))
        year_month = df[periodo_col].map(lambda v: parse_periodo(clean_text(v)))

        out = pd.DataFrame()
        out["year"] = year_month.map(lambda x: x[0])
        out["month"] = year_month.map(lambda x: x[1])
        out["periodo_raw"] = df[periodo_col].map(normalize_text_value)
        out["hs10"] = hs
        out["hs8"] = hs.str[:8]
        out["hs6"] = hs.str[:6]
        out["hs4"] = hs.str[:4]
        out["hs2"] = hs.str[:2]
        out["country_code"] = df[code_col].map(normalize_text_value) if code_col else ""
        out["country_name"] = df[name_col].map(normalize_text_value)
        out["tm_peso_neto"] = pd.to_numeric(df[tm_col], errors="coerce").fillna(0) if tm_col else 0
        out["fob"] = pd.to_numeric(df[fob_col], errors="coerce").fillna(0)
        if is_import:
            out["cif"] = pd.to_numeric(df[cif_col], errors="coerce").fillna(0) if cif_col else 0

        invalid_hs = ~out["hs10"].str.match(r"^\d{10}$", na=False)
        if invalid_hs.any():
            logger.warning("%s: %s filas con HS10 inválido", path.name, int(invalid_hs.sum()))
        out = out[~invalid_hs]

        out = out[(out["fob"] >= 0) & (out["tm_peso_neto"] >= 0)]
        if is_import and "cif" in out.columns:
            out = out[out["cif"] >= 0]

        enriched = out.merge(dim_hs[["hs10", "descripcion_final"]], on="hs10", how="left")
        enriched["source_file"] = path.name
        return enriched

    def _build_fact_trademap(self) -> pd.DataFrame:
        path = self._find_excel_by_keyword("trademap")
        df = pd.read_excel(path, sheet_name=0)
        product_col = resolve_column(df.columns, ["producto", "product"])
        country_col = resolve_column(df.columns, ["pais", "country"])
        year_col = resolve_column(df.columns, ["year", "anio", "año"])
        value_col = resolve_column(df.columns, ["value", "valor"])

        if not all([product_col, country_col, year_col, value_col]):
            logger.warning("fact_trademap sin columnas requeridas. Se devuelve vacío")
            return self._empty_fact_trademap()

        out = pd.DataFrame()
        out["producto"] = df[product_col].map(normalize_text_value)
        out["pais"] = df[country_col].map(normalize_text_value)
        out["year"] = pd.to_numeric(df[year_col], errors="coerce").fillna(0).astype(int)
        out["value"] = pd.to_numeric(df[value_col], errors="coerce").fillna(0)
        return out

    def _validate_before_kpis(
        self,
        dim_hs: pd.DataFrame,
        dim_sector: pd.DataFrame,
        fact_exports: pd.DataFrame,
        fact_imports: pd.DataFrame,
    ) -> None:
        duplicated_hs = dim_hs["hs10"].duplicated().sum() if "hs10" in dim_hs.columns else 0
        if duplicated_hs:
            logger.warning("dim_hs tiene %s HS10 duplicados", duplicated_hs)

        hs2_count = dim_sector["hs2"].nunique() if "hs2" in dim_sector.columns else 0
        if hs2_count != 98:
            logger.warning("dim_sector tiene %s capítulos únicos (esperado ~98)", hs2_count)

        if not fact_exports.empty and not dim_sector.empty and "hs2" in fact_exports.columns:
            join_share = fact_exports.merge(dim_sector[["hs2"]].drop_duplicates(), on="hs2", how="left")["hs2"].notna().mean()
            if join_share < 0.8:
                logger.warning("Join HS->sector bajo: %.2f%%", join_share * 100)

        if "fob" in fact_exports.columns and (fact_exports["fob"] < 0).any():
            logger.warning("FOB negativo detectado en exportaciones")
        if "fob" in fact_imports.columns and (fact_imports["fob"] < 0).any():
            logger.warning("FOB negativo detectado en importaciones")
        if "cif" in fact_imports.columns and (fact_imports["cif"] < 0).any():
            logger.warning("CIF negativo detectado en importaciones")

        if "country_name" in fact_exports.columns:
            has_china = fact_exports["country_name"].str.upper().str.contains("CHINA", na=False).any()
            if not has_china:
                logger.warning("No se detectaron registros para China en exportaciones")

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

    @staticmethod
    def _empty_dim_hs() -> pd.DataFrame:
        return pd.DataFrame(columns=["hs2", "hs4", "hs6", "hs8", "hs10", "descripcion_final", "tipo_elemento"])

    @staticmethod
    def _empty_dim_sector() -> pd.DataFrame:
        return pd.DataFrame(columns=["hs2", "seccion", "sector_industria"])

    @staticmethod
    def _empty_fact_exports() -> pd.DataFrame:
        return pd.DataFrame(
            columns=[
                "year",
                "month",
                "periodo_raw",
                "hs10",
                "hs8",
                "hs6",
                "hs4",
                "hs2",
                "country_code",
                "country_name",
                "tm_peso_neto",
                "fob",
                "descripcion_final",
                "source_file",
            ]
        )

    @staticmethod
    def _empty_fact_imports() -> pd.DataFrame:
        return pd.DataFrame(
            columns=[
                "year",
                "month",
                "periodo_raw",
                "hs10",
                "hs8",
                "hs6",
                "hs4",
                "hs2",
                "country_code",
                "country_name",
                "tm_peso_neto",
                "fob",
                "cif",
                "descripcion_final",
                "source_file",
            ]
        )

    @staticmethod
    def _empty_fact_trademap() -> pd.DataFrame:
        return pd.DataFrame(columns=["producto", "pais", "year", "value"])
