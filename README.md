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

## Estructura de datos esperada en `data/raw/`

El ETL soporta tanto archivos sueltos como carpetas anidadas (caso real 1998-2025):

- `EXPORTACION_1998-2025/*.xlsx` (uno o varios Excel)
- `IMPORTACIONES_1998-2025/*.xlsx` (uno o varios Excel)
- `diccionario_hs_caps.xlsx` o cualquier archivo con `diccionario` en el nombre
- `panel_trademap.xlsx` o cualquier archivo con `trademap` en el nombre
- `SECTORES.xlsx` o cualquier archivo con `sector` en el nombre

> El pipeline busca recursivamente `*.xlsx` y detecta encabezados automáticamente por columnas clave.

## Comandos

```bash
make install
make etl
make run
```

- API: http://localhost:8000
- Web: http://localhost:3000

## Flujo de datos

1. Colocar archivos Excel en `data/raw/` (incluyendo carpetas de export/import por años).
2. Ejecutar `make etl`.
3. Se generan tablas canónicas en `data/processed/*.parquet` y `data/processed/observatorio.duckdb`.
4. API consulta DuckDB y calcula KPIs desde `packages/analytics`.

## Demo seed (opcional)

Si no tienes datos todavía, puedes habilitar datos sintéticos:

```bash
ETL_SEED_DEMO=true make etl
```

## Testing

```bash
make test
```
