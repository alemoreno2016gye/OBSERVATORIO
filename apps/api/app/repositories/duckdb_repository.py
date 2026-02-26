from __future__ import annotations

import duckdb
import pandas as pd


class DuckDBRepository:
    def __init__(self, db_path: str):
        self.db_path = db_path

    def read(self, table_name: str) -> pd.DataFrame:
        conn = duckdb.connect(self.db_path, read_only=True)
        try:
            return conn.execute(f"SELECT * FROM {table_name}").df()
        finally:
            conn.close()
