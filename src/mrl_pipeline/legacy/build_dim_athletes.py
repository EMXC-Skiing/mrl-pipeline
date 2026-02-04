import pandas as pd


def build_dim_athletes(df_dim_registrations: pd.DataFrame) -> pd.DataFrame:
    """
    Input:
      df_dim_registrations: already-fetched dim_registrations table
    Returns:
      df_dim_athletes
    """

    # distill most recent registration data to athletes table
    df_dim_athletes = (
        df_dim_registrations.sort_values(by="race_season", ascending=False)
        .groupby(["athlete_id"], as_index=False)
        .apply(
            lambda grp: (
                grp.bfill().infer_objects(copy=False).reset_index(drop=True).iloc[0]
            )
        )
        .assign(graduation_year=lambda r: r["race_season"] + 12 - r["grade"])
        .drop(
            columns=[
                "grade",
                "race_season",
                "registration_id",
                "usss_age",
                "is_ehs_eligible",
                "is_u16c_eligible",
            ]
        )
    )

    # Pivot registrations to create a boolean table for each athlete-season combination
    df_reg_year_pivot = (
        df_dim_registrations[["athlete_id", "race_season"]]
        .pivot_table(
            index="athlete_id",
            columns="race_season",
            aggfunc=lambda x: True,
            fill_value=False,
        )
        .astype("boolean")
    )

    # Rename columns to indicate registration by race season
    df_reg_year_pivot.columns = [
        f"was_registered_eligible_{y}" for y in df_reg_year_pivot.columns
    ]
    df_reg_year_pivot = df_reg_year_pivot[
        [c for c in sorted(df_reg_year_pivot.columns, reverse=True)]
    ]

    # combine tables at the grain of athlete
    df_dim_athletes = (
        df_dim_athletes.merge(df_reg_year_pivot, on="athlete_id", how="left")
        .astype(
            {
                "athlete_id": "string",
                "athlete_name": "string",
                "city_town": "string",
                "state": "string",
                "school": "string",
                "club": "string",
                "gender": "string",
                "nensa_number": "Int64",  # nullable int
                "usss_number": "Int64",  # nullable int
                "birth_year": "int64",
                "graduation_year": "int64",
            }
        )
        .sort_values(["athlete_name", "gender"])
    )

    return df_dim_athletes
