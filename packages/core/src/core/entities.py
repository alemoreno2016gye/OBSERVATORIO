from dataclasses import dataclass


@dataclass(frozen=True)
class HSNode:
    hs2: str
    hs4: str
    hs6: str
    hs8: str
    hs10: str
    descripcion_final: str
    tipo_elemento: str


@dataclass(frozen=True)
class TradeRecord:
    year: int
    month: int
    periodo_raw: str
    hs10: str
    hs6: str
    hs4: str
    hs2: str
    country_code: str
    country_name: str
    tm_peso_neto: float
    fob: float
