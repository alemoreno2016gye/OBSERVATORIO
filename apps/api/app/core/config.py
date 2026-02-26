from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    duckdb_path: str = "./data/processed/observatorio.duckdb"

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


settings = Settings()
