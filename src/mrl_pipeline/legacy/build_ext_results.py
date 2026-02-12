from __future__ import annotations
from importlib.resources import files


import duckdb
import pandas as pd


def build_df_ext_results_duckdb(
    df_prep_penalty_contributions: pd.DataFrame,
    df_ext_athlete_penalties: pd.DataFrame,
    df_dim_results: pd.DataFrame,
    df_dim_races: pd.DataFrame,
    df_dim_registrations: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build df_ext_results using an in-process DuckDB database and a single CREATE TABLE AS ... WITH ... SELECT ... SQL chain.

    Inputs (registered as DuckDB tables with same names):
      - df_prep_penalty_contributions
      - df_ext_athlete_penalties   (must contain standardized_athlete_penalty_25 and standardized_athlete_penalty_26)
      - df_dim_results
      - df_dim_races
      - df_dim_registrations       (must contain registration_id and is_ghost_registration)

    Output:
      - pandas DataFrame df_ext_results
    """

    con = duckdb.connect(database=":memory:")

    # Register pandas DataFrames as DuckDB tables
    con.register("df_prep_penalty_contributions", df_prep_penalty_contributions)
    con.register("df_ext_athlete_penalties", df_ext_athlete_penalties)
    con.register("df_dim_results", df_dim_results)
    con.register("df_dim_races", df_dim_races)
    con.register("df_dim_registrations", df_dim_registrations)

    query = (
        files(__package__).joinpath("build_ext_results.sql").read_text(encoding="utf-8")
    )

    df_ext_results = con.execute(query).df()

    con.close()
    return df_ext_results
