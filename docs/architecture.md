# Arquitectura

## Principios

- **SRP**: ETL, dominio, analytics, API y UI están separados.
- **OCP**: KPI nuevos se agregan en `packages/analytics` sin romper adaptadores.
- **LSP/ISP**: contratos de repositorio y servicios en `packages/core`.
- **DIP**: `AnalyticsService` depende de abstracción de acceso a datos (repositorio).

## Capas

1. **Domain (`packages/core`)**
2. **Application (`packages/analytics`, `packages/etl`)**
3. **Interface Adapters (`apps/api`, `apps/web`)**
4. **Infrastructure (DuckDB, Parquet, Docker)**

## Almacenamiento

- `data/processed/*.parquet`: tablas canónicas.
- `data/processed/observatorio.duckdb`: motor OLAP local.

## Escalabilidad

- particionado por año en futuras versiones
- materialized views para KPIs costosos
- cache en capa API
