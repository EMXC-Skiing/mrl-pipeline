import numpy as np
import pandas as pd
from sklearn import linear_model


# -----------------------------------------------------------------------------
# 1) Build df_log_ratio_times_by_publication_date
# -----------------------------------------------------------------------------


def build_df_log_ratio_times_by_publication_date(
    df_dim_races: pd.DataFrame,  # currently unused, kept for interface consistency
    df_dim_registrations: pd.DataFrame,
    df_dim_results: pd.DataFrame,
    df_ext_races_by_penalty_window: pd.DataFrame,
) -> pd.DataFrame:
    races = df_ext_races_by_penalty_window.copy()
    results = df_dim_results.copy()
    regs = df_dim_registrations.copy()

    # Normalize dtypes needed downstream
    races["penalty_list_date"] = pd.to_datetime(races["penalty_list_date"])
    races["race_date"] = pd.to_datetime(races["race_date"])
    results["race_date"] = pd.to_datetime(results["race_date"])
    regs["race_season"] = pd.to_numeric(regs["race_season"], errors="coerce").astype(
        "Int64"
    )

    # races LEFT JOIN results ON race_id
    df_rc = races.merge(
        results[
            [
                "race_id",
                "athlete_id",
                "athlete_name",
                "race_date",
                "race_season",
                "gender",
                "time_float",
                "series",
            ]
        ],
        on="race_id",
        how="left",
        suffixes=("_race", "_result"),
    )

    # Keep series from races table (authoritative for window membership)
    if "series_race" in df_rc.columns:
        df_rc["series"] = df_rc["series_race"]
    df_rc = df_rc.drop(
        columns=[c for c in ("series_race", "series_result") if c in df_rc.columns]
    )

    # INNER JOIN registrations ON (penalty_list_season=race_season AND athlete_id)
    df_rc = df_rc.merge(
        regs[["race_season", "athlete_id"]],
        left_on=["penalty_list_season", "athlete_id"],
        right_on=["race_season", "athlete_id"],
        how="inner",
    ).drop(columns=["race_season"])

    # WHERE results.time_float IS NOT NULL
    df_rc = df_rc.loc[df_rc["time_float"].notna()].copy()

    # rank() over (partition by penalty_list_date, race_id, gender order by time_float)
    df_rc["ma_athlete_place"] = (
        df_rc.groupby(["penalty_list_date", "race_id", "gender"])["time_float"]
        .rank(method="min", ascending=True)
        .astype("Int64")
    )

    # Top finisher per (penalty_list_date, race_id, gender)
    top_finishers = df_rc.loc[
        df_rc["ma_athlete_place"] == 1,
        ["penalty_list_date", "race_id", "gender", "time_float", "athlete_id"],
    ].rename(
        columns={
            "time_float": "reference_time_float",
            "athlete_id": "reference_athlete_id",
        }
    )

    # Attach top finisher and compute log-ratio features
    df_log = df_rc.merge(
        top_finishers,
        on=["penalty_list_date", "race_id", "gender"],
        how="left",
    )

    df_log["time_log_ratio"] = np.log(
        df_log["time_float"] / df_log["reference_time_float"]
    )
    df_log["days_from_race_to_publication"] = (
        df_log["penalty_list_date"] - df_log["race_date"]
    ).dt.days

    out = (
        df_log[
            [
                "penalty_list_season",
                "penalty_list_date",
                "race_id",
                "race_date",
                "gender",
                "athlete_id",
                "athlete_name",
                "series",
                "reference_athlete_id",
                "time_log_ratio",
                "days_from_race_to_publication",
            ]
        ]
        .sort_values(
            by=["race_date", "race_id", "gender", "athlete_id", "penalty_list_date"]
        )
        .reset_index(drop=True)
    )

    return out


# -----------------------------------------------------------------------------
# 2) Penalty calculation v2025 as an .assign()-friendly function
# -----------------------------------------------------------------------------


def calculate_penalties_2025(
    df: pd.DataFrame,
    *,
    target_mean: float = 200.0,
    robust_pctiles: tuple[float, float] = (0.25, 0.75),
    return_columns: tuple[str, ...] = (
        "athlete_penalty_25",
        "standardized_athlete_penalty_25",
    ),
) -> dict[str, np.ndarray]:
    """
    Returns a dict suitable for:
        df.assign(calculate_penalties_2025)

    Adds (versioned) columns:
      - athlete_penalty_25
      - standardized_athlete_penalty_25

    Requirements on df:
      Must contain at least:
        penalty_list_season, penalty_list_date, gender, athlete_id,
        reference_athlete_id, time_log_ratio
      If present, days_from_race_to_publication is ignored in 2025 version.
    """

    group_cols = ("penalty_list_season", "penalty_list_date", "gender")
    key_cols = (*group_cols, "athlete_id")

    def construct_X_y(group: pd.DataFrame):
        g = group.loc[group["athlete_id"] != group["reference_athlete_id"]].copy()
        if g.empty:
            return None, None

        athletes = pd.Index(
            pd.concat([g["athlete_id"], g["reference_athlete_id"]]).unique()
        )

        X = pd.DataFrame(0.0, index=range(len(g)), columns=athletes)
        for i, r in g.iterrows():
            X.at[i, r["athlete_id"]] = 1.0
            X.at[i, r["reference_athlete_id"]] = -1.0

        y = g["time_log_ratio"].to_numpy()

        # Mean constraint: average coefficient should be ~0 on log scale
        mean_row = pd.DataFrame(
            {c: 1.0 / len(athletes) for c in athletes}, index=[len(X)]
        )
        X = pd.concat([X, mean_row], axis=0)
        y = np.append(y, 0.0)

        return X, y

    def scale_to_target_mean(arr: np.ndarray) -> np.ndarray:
        lo, hi = robust_pctiles
        q_lo, q_hi = np.quantile(arr, lo), np.quantile(arr, hi)
        inner = arr[(arr >= q_lo) & (arr <= q_hi)]
        if inner.size == 0:
            return arr
        inner_mean = inner.mean()
        if not np.isfinite(inner_mean) or inner_mean == 0:
            return arr
        return arr * (target_mean / inner_mean)

    def robust_mean(s: pd.Series) -> float:
        arr = s.dropna().to_numpy()
        if arr.size == 0:
            return np.nan
        lo, hi = robust_pctiles
        q_lo, q_hi = np.quantile(arr, lo), np.quantile(arr, hi)
        inner = arr[(arr >= q_lo) & (arr <= q_hi)]
        return float(inner.mean()) if inner.size else np.nan

    # --- per-group regression ----------------------------------------------------
    dfs = []
    for key, group in df.groupby(list(group_cols), dropna=False):
        X, y = construct_X_y(group)
        if X is None:
            continue

        model = linear_model.LinearRegression()
        model.fit(X, y)

        raw = np.exp(model.coef_)
        athlete_penalty_25 = scale_to_target_mean(raw)

        dfs.append(
            pd.DataFrame(
                {
                    "athlete_id": X.columns.astype("string"),
                    "athlete_penalty_25": athlete_penalty_25,
                    "penalty_list_season": key[0],
                    "penalty_list_date": key[1],
                    "gender": key[2],
                }
            )
        )

    # If no groups produced penalties, return empty aligned outputs
    if not dfs:
        n = len(df)
        out = {
            "athlete_penalty_25": np.full(n, np.nan, dtype="float64"),
            "standardized_athlete_penalty_25": np.full(n, np.nan, dtype="float64"),
        }
        return {k: out[k] for k in return_columns}

    df_pen = pd.concat(dfs, ignore_index=True)

    # --- normalization (per season+gender+date) ----------------------------------
    norms = (
        df_pen.groupby(
            ["penalty_list_season", "gender", "penalty_list_date"], dropna=False
        )["athlete_penalty_25"]
        .apply(robust_mean)
        .rename("robust_mean")
        .reset_index()
    )
    norms["norm_factor"] = target_mean / norms["robust_mean"]

    df_pen = df_pen.merge(
        norms[["penalty_list_season", "gender", "penalty_list_date", "norm_factor"]],
        on=["penalty_list_season", "gender", "penalty_list_date"],
        how="left",
    )
    df_pen["standardized_athlete_penalty_25"] = (
        df_pen["athlete_penalty_25"] * df_pen["norm_factor"]
    )

    # --- align back to input rows for .assign() ----------------------------------
    idx = df.set_index(list(key_cols), drop=False).index
    df_pen = df_pen.set_index(list(key_cols))

    aligned = df_pen.reindex(idx)

    out = {
        "athlete_penalty_25": aligned["athlete_penalty_25"].to_numpy(),
        "standardized_athlete_penalty_25": aligned[
            "standardized_athlete_penalty_25"
        ].to_numpy(),
    }
    return {k: out[k] for k in return_columns}


# -----------------------------------------------------------------------------
# 3) Pipeline stage wrapper
# -----------------------------------------------------------------------------


def build_ext_athlete_penalties(
    df_dim_races: pd.DataFrame,
    df_dim_registrations: pd.DataFrame,
    df_dim_results: pd.DataFrame,
    df_ext_races_by_penalty_window: pd.DataFrame,
) -> pd.DataFrame:
    df_log_ratio_times_by_publication_date = (
        build_df_log_ratio_times_by_publication_date(
            df_dim_races=df_dim_races,
            df_dim_registrations=df_dim_registrations,
            df_dim_results=df_dim_results,
            df_ext_races_by_penalty_window=df_ext_races_by_penalty_window,
        )
    )

    df_ext_athlete_penalties = df_log_ratio_times_by_publication_date.assign(
        calculate_penalties_2025
    )
    return df_ext_athlete_penalties
