from pathlib import Path
from dotenv import load_dotenv
import os

from packages.etl.src.etl.pipeline import ETLConfig, ObservatorioETL
from scripts.seed_data import ensure_sample_data
from scripts.generate_data_dictionary import generate_data_dictionary


def main() -> None:
    load_dotenv()
    raw = Path(os.getenv("DATA_RAW_DIR", "./data/raw"))
    processed = Path(os.getenv("DATA_PROCESSED_DIR", "./data/processed"))
    db = Path(os.getenv("DUCKDB_PATH", "./data/processed/observatorio.duckdb"))

    if os.getenv("ETL_SEED_DEMO", "false").lower() == "true":
        ensure_sample_data(raw)

    etl = ObservatorioETL(ETLConfig(raw_dir=raw, processed_dir=processed, duckdb_path=db))
    etl.run()
    generate_data_dictionary(processed)
    print("ETL completado")


if __name__ == "__main__":
    main()
