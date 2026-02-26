import pandas as pd

from packages.analytics.src.analytics.kpis import dependency_by_product, overview_kpis
from packages.analytics.src.analytics.validators import validate_kpi_inputs


def test_overview_kpis_balance():
    exports = pd.DataFrame([{"year": 2024, "fob": 100, "country_name": "China"}])
    imports = pd.DataFrame([{"year": 2024, "fob": 80, "cif": 90}])
    out = overview_kpis(exports, imports, 2024)
    assert out["trade_balance"] == 20
    assert out["logistics_cost"] == 10


def test_validate_kpi_inputs_filters_negative_values():
    exports = pd.DataFrame([{"year": 2024, "fob": -1, "country_name": "China"}, {"year": 2024, "fob": 10, "country_name": "China"}])
    imports = pd.DataFrame([{"year": 2024, "fob": 5, "cif": -2}, {"year": 2024, "fob": 5, "cif": 7}])
    ex2, im2 = validate_kpi_inputs(exports, imports)
    assert len(ex2) == 1
    assert len(im2) == 1


def test_dependency_empty_columns_safe():
    out = dependency_by_product(pd.DataFrame())
    assert list(out.columns) == ["hs10", "fob_total", "fob_china", "share_china"]
