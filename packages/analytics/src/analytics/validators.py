from __future__ import annotations

import logging
import pandas as pd

logger = logging.getLogger(__name__)


def validate_kpi_inputs(exports: pd.DataFrame, imports: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    if "fob" in exports.columns:
        negatives = (exports["fob"] < 0).sum()
        if negatives:
            logger.warning("Se removieron %s filas de exportaciones por FOB negativo", int(negatives))
        exports = exports[exports["fob"] >= 0]

    if "fob" in imports.columns:
        negatives = (imports["fob"] < 0).sum()
        if negatives:
            logger.warning("Se removieron %s filas de importaciones por FOB negativo", int(negatives))
        imports = imports[imports["fob"] >= 0]

    if "cif" in imports.columns:
        negatives = (imports["cif"] < 0).sum()
        if negatives:
            logger.warning("Se removieron %s filas de importaciones por CIF negativo", int(negatives))
        imports = imports[imports["cif"] >= 0]

    if "country_name" in exports.columns and not exports["country_name"].str.upper().str.contains("CHINA", na=False).any():
        logger.warning("No hay registros de China para cálculo de dependencia")

    return exports, imports
