from fastapi import APIRouter, Depends, Query

from ..core.config import settings
from ..repositories.duckdb_repository import DuckDBRepository
from ..services.analytics_service import AnalyticsService

router = APIRouter()


def get_service() -> AnalyticsService:
    repo = DuckDBRepository(settings.duckdb_path)
    return AnalyticsService(repo)


@router.get('/health')
async def health() -> dict:
    return {"status": "ok"}


@router.get('/kpis/overview')
async def overview(year: int | None = Query(default=None), service: AnalyticsService = Depends(get_service)) -> dict:
    return service.overview(year)


@router.get('/kpis/dependency')
async def dependency(service: AnalyticsService = Depends(get_service)) -> list[dict]:
    return service.dependency()
