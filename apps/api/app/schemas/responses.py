from pydantic import BaseModel


class CountryRankingItem(BaseModel):
    country_name: str
    fob: float


class OverviewResponse(BaseModel):
    total_exports_fob: float
    total_imports_fob: float
    trade_balance: float
    logistics_cost: float
    country_ranking: list[CountryRankingItem]
