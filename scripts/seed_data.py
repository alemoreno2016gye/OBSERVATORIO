from pathlib import Path
import pandas as pd


def ensure_sample_data(raw_dir: Path) -> None:
    raw_dir.mkdir(parents=True, exist_ok=True)
    _seed_dict(raw_dir / "diccionario_ecuador.xlsx")
    _seed_trade(raw_dir / "exportaciones.xlsx", export=True)
    _seed_trade(raw_dir / "importaciones.xlsx", export=False)
    _seed_trademap(raw_dir / "panel_trademap.xlsx")
    _seed_sector(raw_dir / "SECTORES.xlsx")


def _seed_dict(path: Path) -> None:
    if path.exists():
        return
    df = pd.DataFrame([
        {"hs10": "0803901100", "descripcion_final": "Banano", "tipo_elemento": "subpartida"},
        {"hs10": "0306171000", "descripcion_final": "Camarón", "tipo_elemento": "subpartida"},
    ])
    with pd.ExcelWriter(path) as w:
        df.to_excel(w, sheet_name="diccionario", index=False)


def _seed_trade(path: Path, export: bool) -> None:
    if path.exists():
        return
    rows = [
        {
            "Periodo": "2024 / 01 - Enero",
            "Codigo_Subpartida_10": "0803901100",
            "Codigo_Pais_Destino" if export else "Codigo_Pais_Origen": "156",
            "Pais_Destino" if export else "Pais_Origen": "China",
            "TM_Peso_Neto": 100,
            "FOB": 1200,
            **({} if export else {"CIF": 1300}),
        },
        {
            "Periodo": "2024 / 02 - Febrero",
            "Codigo_Subpartida_10": "0306171000",
            "Codigo_Pais_Destino" if export else "Codigo_Pais_Origen": "840",
            "Pais_Destino" if export else "Pais_Origen": "Estados Unidos",
            "TM_Peso_Neto": 50,
            "FOB": 900,
            **({} if export else {"CIF": 980}),
        },
    ]
    df = pd.DataFrame(rows)
    with pd.ExcelWriter(path) as w:
        pd.DataFrame([["metadata"], ["otra fila"]]).to_excel(w, sheet_name="Columnas", index=False, header=False)
        df.to_excel(w, sheet_name="Columnas", index=False, startrow=2)


def _seed_trademap(path: Path) -> None:
    if path.exists():
        return
    panel = pd.DataFrame([
        {"producto": "0803901100", "pais": "China", "year": 2024, "value": 10000},
        {"producto": "0306171000", "pais": "China", "year": 2024, "value": 8000},
    ])
    lookup = pd.DataFrame([{"capitulo": "08"}])
    with pd.ExcelWriter(path) as w:
        panel.to_excel(w, sheet_name="Sheet1", index=False)
        lookup.to_excel(w, sheet_name="Sheet2", index=False)


def _seed_sector(path: Path) -> None:
    if path.exists():
        return
    df = pd.DataFrame([
        {"sección": "II", "capítulos": "01-05", "sector": "Agro"},
        {"sección": "III", "capítulos": "06,07,08", "sector": "Alimentos"},
    ])
    df.to_excel(path, index=False)
