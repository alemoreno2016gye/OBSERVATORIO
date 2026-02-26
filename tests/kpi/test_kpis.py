import pytest

pd = pytest.importorskip('pandas')
from packages.analytics.src.analytics.kpis import overview_kpis


def test_overview_kpis_balance():
    exports = pd.DataFrame([{"year": 2024, "fob": 100, "country_name": "China"}])
    imports = pd.DataFrame([{"year": 2024, "fob": 80, "cif": 90}])
    out = overview_kpis(exports, imports, 2024)
    assert out["trade_balance"] == 20
    assert out["logistics_cost"] == 10
