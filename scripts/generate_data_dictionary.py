from pathlib import Path
import pandas as pd


def generate_data_dictionary(processed_dir: Path) -> None:
    lines = ["# Data Dictionary\n"]
    for file in sorted(processed_dir.glob("*.parquet")):
        df = pd.read_parquet(file)
        lines.append(f"## {file.stem}\n")
        for col, dtype in df.dtypes.items():
            lines.append(f"- `{col}`: `{dtype}`")
        lines.append("")
    Path("docs/data_dictionary.md").write_text("\n".join(lines), encoding="utf-8")
