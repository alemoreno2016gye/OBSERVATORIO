from __future__ import annotations

from functools import lru_cache
from packages.analytics.src.analytics import overview_kpis, dependency_by_product, validate_kpi_inputs
from ..repositories.duckdb_repository import DuckDBRepository


class AnalyticsService:
    def __init__(self, repository: DuckDBRepository):
        self.repository = repository

    @lru_cache(maxsize=64)
    def overview(self, year: int | None = None) -> dict:
        exports = self.repository.read("fact_exports")
        imports = self.repository.read("fact_imports")
        exports, imports = validate_kpi_inputs(exports, imports)
        return overview_kpis(exports, imports, year)

    @lru_cache(maxsize=64)
    def dependency(self) -> list[dict]:
        exports = self.repository.read("fact_exports")
        dep = dependency_by_product(exports)
        high = dep[dep["share_china"] >= 0.5].sort_values("share_china", ascending=False)
        return high.to_dict(orient="records")
