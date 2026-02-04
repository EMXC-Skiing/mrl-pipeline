import pandas as pd
import warnings


def race_dates_in_windows(race_dates: pd.DataFrame, windows: pd.DataFrame) -> pd.Series:
    return (race_dates["race_date"] >= windows["window_start_date"]) & (
        race_dates["race_date"] < windows["penalty_list_date"]
    )


def calculate_penalty_windows(
    df_prep_penalty_publication_dates: pd.DataFrame,
    df_prep_penalty_contribution_windows: pd.DataFrame,
    df_dim_races: pd.DataFrame,
) -> pd.DataFrame:
    """
    Transform-only (no IO): returns df_penalty_windows.

    Notes:
      - Assumes df_dim_races has at least: race_date, series, race_season
      - Assumes prep inputs have: penalty_list_date, penalty_list_season, series,
        contributing_window_weeks, current_season_supplants_previous_season
    """

    # Cross join publication dates x contribution windows
    df_penalty_windows = pd.merge(
        df_prep_penalty_publication_dates,
        df_prep_penalty_contribution_windows,
        how="cross",
    )

    # Normalize dates and compute window start
    df_penalty_windows["penalty_list_date"] = pd.to_datetime(
        df_penalty_windows["penalty_list_date"]
    )
    df_penalty_windows["window_start_date"] = df_penalty_windows[
        "penalty_list_date"
    ] - pd.to_timedelta(df_penalty_windows["contributing_window_weeks"] * 7, unit="D")
    df_penalty_windows["recent_season_only"] = df_penalty_windows[
        "current_season_supplants_previous_season"
    ].astype(bool)

    # Ensure races have comparable dtype for race_date
    df_races = df_dim_races.copy()
    df_races["race_date"] = pd.to_datetime(df_races["race_date"])

    # Compute most recent race season in each window, per series, without row-wise apply:
    # Join races to windows on series, filter to in-window, then take max season per window-row.
    windows_key = df_penalty_windows.reset_index(names="_window_row_id")

    races_in_windows = windows_key[
        ["_window_row_id", "series", "window_start_date", "penalty_list_date"]
    ].merge(df_races[["series", "race_date", "race_season"]], on="series", how="left")

    races_in_windows = races_in_windows.loc[
        race_dates_in_windows(races_in_windows, races_in_windows)
    ]

    most_recent = (
        races_in_windows.groupby("_window_row_id", as_index=False)["race_season"]
        .max()
        .rename(columns={"race_season": "most_recent_race_season_in_window"})
    )

    df_penalty_windows = windows_key.merge(
        most_recent, on="_window_row_id", how="left"
    ).drop(columns=["_window_row_id"])

    return df_penalty_windows


def build_ext_races_by_penalty_window(
    df_prep_penalty_publication_dates: pd.DataFrame,
    df_prep_penalty_contribution_windows: pd.DataFrame,
    df_dim_races: pd.DataFrame,
    *,
    staleness_months: int = 3,
) -> pd.DataFrame:
    """
    Transform-only (no warehouse IO). Returns df_ext_races_by_penalty_window.

    Warnings:
      1) If race series exist in dim_races that have no corresponding window definition in
         df_prep_penalty_contribution_windows (those races will be excluded).
      2) If penalty_publication_dates appear stale: any race_date is more than `staleness_months`
         after the most recent penalty_list_date.
    """

    # Normalize dtypes needed for checks
    races = df_dim_races.copy()
    races["race_date"] = pd.to_datetime(races["race_date"])

    penalty_dates = df_prep_penalty_publication_dates.copy()
    penalty_dates["penalty_list_date"] = pd.to_datetime(
        penalty_dates["penalty_list_date"]
    )

    # --- Warning 1: series coverage ------------------------------------------------
    race_series = set(races["series"].dropna().unique())
    window_series = set(
        df_prep_penalty_contribution_windows["series"].dropna().unique()
    )

    missing_series = race_series - window_series
    if missing_series:
        warnings.warn(
            "Race series appear in dim_races but have no entries in "
            "df_prep_penalty_contribution_windows and will be excluded from "
            "ext_races_by_penalty_window: "
            f"{sorted(missing_series)}",
            category=UserWarning,
            stacklevel=2,
        )

    # --- Warning 2: stale penalty publication dates --------------------------------
    max_penalty_date = penalty_dates["penalty_list_date"].max()
    if pd.isna(max_penalty_date):
        warnings.warn(
            "df_prep_penalty_publication_dates has no valid penalty_list_date values; "
            "cannot assess staleness.",
            category=UserWarning,
            stacklevel=2,
        )
    else:
        stale_cutoff = max_penalty_date + pd.DateOffset(months=staleness_months)
        late_races = races.loc[
            races["race_date"] > stale_cutoff, ["race_id", "series", "race_date"]
        ]

        if not late_races.empty:
            latest_race_date = late_races["race_date"].max()
            affected_series = sorted(late_races["series"].dropna().unique())
            warnings.warn(
                "Penalty publication dates may be stale: found "
                f"{len(late_races)} race(s) in dim_races with race_date later than "
                f"{staleness_months} month(s) after the most recent penalty_list_date. "
                f"max_penalty_list_date={max_penalty_date.date()}, "
                f"stale_cutoff={stale_cutoff.date()}, "
                f"latest_race_date={latest_race_date.date()}, "
                f"affected_series={affected_series}",
                category=UserWarning,
                stacklevel=2,
            )

    # --- Existing transform ---------------------------------------------------------
    df_penalty_windows = calculate_penalty_windows(
        df_prep_penalty_publication_dates=df_prep_penalty_publication_dates,
        df_prep_penalty_contribution_windows=df_prep_penalty_contribution_windows,
        df_dim_races=df_dim_races,
    )

    df_ext = df_penalty_windows.merge(
        races[["race_date", "series", "race_id", "race_season"]],
        on="series",
        how="left",
    )

    in_window = race_dates_in_windows(df_ext, df_ext)
    recent_season_ok = (~df_ext["recent_season_only"]) | (
        df_ext["recent_season_only"]
        & (df_ext["race_season"] == df_ext["most_recent_race_season_in_window"])
    )

    df_ext = df_ext.loc[in_window & recent_season_ok]

    df_ext = (
        df_ext[
            [
                "penalty_list_season",
                "penalty_list_date",
                "series",
                "race_id",
                "race_date",
                "race_season",
            ]
        ]
        .sort_values(
            by=["penalty_list_season", "penalty_list_date", "series", "race_date"],
            ascending=[False, True, True, True],
        )
        .reset_index(drop=True)
    )

    return df_ext
