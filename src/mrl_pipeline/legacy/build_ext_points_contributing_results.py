from __future__ import annotations

from importlib.resources import files

import duckdb
import pandas as pd


def build_ext_points_contributing_results(
    df_ext_results: pd.DataFrame,
    df_dim_registrations: pd.DataFrame,
) -> pd.DataFrame:
    pkg = __package__ or __name__.rpartition(".")[0]
    query = (
        files(pkg)
        .joinpath("build_ext_points_contributing_results.sql")
        .read_text(encoding="utf-8")
    )

    with duckdb.connect(database=":memory:") as con:
        con.register("df_ext_results", df_ext_results)
        con.register("df_dim_registrations", df_dim_registrations)
        return con.execute(query).df()
