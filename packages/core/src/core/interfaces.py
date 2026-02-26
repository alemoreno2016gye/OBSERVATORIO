from abc import ABC, abstractmethod
import pandas as pd


class TableRepository(ABC):
    @abstractmethod
    def read(self, table_name: str) -> pd.DataFrame:
        raise NotImplementedError


class KPIService(ABC):
    @abstractmethod
    def overview(self, year: int | None = None) -> dict:
        raise NotImplementedError
