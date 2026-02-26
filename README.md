# Observatorio Ecuador–China

Plataforma analítica local para analizar comercio exterior (exportaciones/importaciones) por HS10, capítulo, sector y país.

## Arquitectura

- **Clean Architecture + SOLID**
  - `packages/core`: entidades y contratos (dominio puro)
  - `packages/etl`: pipelines de extracción/normalización
  - `packages/analytics`: KPIs y agregaciones
  - `apps/api`: FastAPI (adaptador de entrada)
  - `apps/web`: Next.js dashboard (adaptador de presentación)

## Requisitos

- Python 3.11+
- Node 20+
- npm

## Variables de entorno

Copiar `.env.example` a `.env`.

## Comandos

```bash
make install
make etl
make run
```

- API: http://localhost:8000
- Web: http://localhost:3000

## Flujo de datos

1. Colocar archivos Excel en `data/raw/`.
2. Ejecutar `make etl`.
3. Se generan tablas canónicas en `data/processed/*.parquet` y `data/processed/observatorio.duckdb`.
4. API consulta DuckDB y calcula KPIs desde `packages/analytics`.

## Testing

```bash
make test
```
