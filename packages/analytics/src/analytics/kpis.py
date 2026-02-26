from __future__ import annotations

import pandas as pd


def overview_kpis(exports: pd.DataFrame, imports: pd.DataFrame, year: int | None = None) -> dict:
    if year:
        exports = exports[exports["year"] == year]
        imports = imports[imports["year"] == year]

    total_exports = float(exports["fob"].sum()) if not exports.empty else 0.0
    total_imports = float(imports["fob"].sum()) if not imports.empty else 0.0
    total_cif = float(imports["cif"].sum()) if "cif" in imports.columns else 0.0
    logistics_cost = total_cif - total_imports

    country_rank = (
        exports.groupby("country_name", as_index=False)["fob"].sum().sort_values("fob", ascending=False).head(10)
    )

    return {
        "total_exports_fob": total_exports,
        "total_imports_fob": total_imports,
        "trade_balance": total_exports - total_imports,
        "logistics_cost": logistics_cost,
        "country_ranking": country_rank.to_dict(orient="records"),
    }


def dependency_by_product(exports: pd.DataFrame, china_name: str = "CHINA") -> pd.DataFrame:
    grp = exports.groupby(["hs10", "country_name"], as_index=False)["fob"].sum()
    totals = grp.groupby("hs10", as_index=False)["fob"].sum().rename(columns={"fob": "fob_total"})
    china = grp[grp["country_name"].str.upper() == china_name].rename(columns={"fob": "fob_china"})
    merged = totals.merge(china[["hs10", "fob_china"]], on="hs10", how="left").fillna({"fob_china": 0})
    merged["share_china"] = merged["fob_china"] / merged["fob_total"].replace(0, 1)
    return merged
