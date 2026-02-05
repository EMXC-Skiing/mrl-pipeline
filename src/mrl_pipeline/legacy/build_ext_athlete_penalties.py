import numpy as np
import pandas as pd
from sklearn import linear_model


def build_df_ext_athlete_penalties_prep(
    df_log_ratio_times_by_publication_date: pd.DataFrame,
    df_dim_registrations: pd.DataFrame,
) -> pd.DataFrame:
    key_cols = ["penalty_list_season", "penalty_list_date", "gender", "athlete_id"]

    df_prep = (
        df_log_ratio_times_by_publication_date[key_cols]
        .drop_duplicates()
        .sort_values(key_cols)
        .reset_index(drop=True)
    )

    # Optional: attach registration attributes at season x athlete_id
    reg_cols = [
        "race_season",
        "athlete_id",
        "athlete_name",
        "school",
        "club",
        "city_town",
        "state",
        "usss_age",
        "grade",
        "is_ehs_eligible",
        "is_u16c_eligible",
    ]

    df_prep = df_prep.merge(
        df_dim_registrations[reg_cols],
        left_on=["penalty_list_season", "athlete_id"],
        right_on=["race_season", "athlete_id"],
        how="left",
    ).drop(columns=["race_season"])

    return df_prep


def build_df_log_ratio_times_by_publication_date(
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
    df_rc = races[
        ["penalty_list_season", "penalty_list_date", "race_id", "series"]
    ].merge(
        results[
            [
                "race_id",
                "athlete_id",
                "athlete_name",
                "race_date",
                "race_season",
                "gender",
                "time_float",
            ]
        ],
        on="race_id",
        how="left",
        suffixes=("_window", "_result"),
    )

    # Optionally drop the window copies if present to avoid confusion
    df_rc = df_rc.drop(
        columns=["race_date_window", "race_season_window"],
        errors="ignore",
    )

    # INNER JOIN registrations ON (penalty_list_season=race_season AND athlete_id)
    regs_keys = regs[["race_season", "athlete_id"]].rename(
        columns={"race_season": "_reg_race_season"}
    )

    df_rc = df_rc.merge(
        regs_keys,
        left_on=["penalty_list_season", "athlete_id"],
        right_on=["_reg_race_season", "athlete_id"],
        how="inner",
    ).drop(columns=["_reg_race_season"])

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
                "series",  # from df_ext_races_by_penalty_window (window table)
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


def standardize_penalties_first_date_cohort(
    df_pen: pd.DataFrame,
    *,
    target_mean: float = 200.0,
    robust_pctiles: tuple[float, float] = (0.25, 0.75),
    penalty_col: str = "athlete_penalty_25",
    out_col: str = "standardized_athlete_penalty_25",
) -> pd.DataFrame:
    """
    Matches original logic:
      - cohort = athletes present on the first penalty_list_date for (season, gender)
      - compute robust mean per date using only cohort athletes
      - apply normalization factor to all athletes on that date
    """

    def robust_mean(arr: np.ndarray) -> float:
        arr = arr[np.isfinite(arr)]
        if arr.size == 0:
            return np.nan
        lo, hi = robust_pctiles
        q_lo, q_hi = np.quantile(arr, lo), np.quantile(arr, hi)
        inner = arr[(arr >= q_lo) & (arr <= q_hi)]
        return float(inner.mean()) if inner.size else np.nan

    norm_rows = []

    # Ensure consistent ordering so "first publication date" is the earliest date
    df_pen = df_pen.copy()
    df_pen["penalty_list_date"] = pd.to_datetime(df_pen["penalty_list_date"])

    for (season, gender), grp in df_pen.groupby(
        ["penalty_list_season", "gender"], dropna=False
    ):
        # Pivot to dates x athlete_id
        piv = grp.sort_values(["penalty_list_date", penalty_col]).pivot_table(
            index="penalty_list_date",
            columns="athlete_id",
            values=penalty_col,
            aggfunc="first",
        )

        if piv.empty:
            continue

        # Cohort mask: athletes who appear on the first publication date in this season+gender
        first_date = piv.index.min()
        cohort_mask = piv.loc[first_date].notna()
        cohort_cols = piv.columns[cohort_mask]

        if len(cohort_cols) == 0:
            # No cohort => can't compute normalization
            continue

        # Robust mean for each date using only cohort athletes
        robust_means = piv[cohort_cols].apply(
            lambda row: robust_mean(row.to_numpy()), axis=1
        )
        norm_factor = target_mean / robust_means

        norm_rows.append(
            pd.DataFrame(
                {
                    "penalty_list_season": season,
                    "gender": gender,
                    "penalty_list_date": robust_means.index,
                    "normalization_factor": norm_factor.values,
                }
            )
        )

    if not norm_rows:
        df_pen[out_col] = np.nan
        return df_pen

    df_norm = pd.concat(norm_rows, ignore_index=True)

    df_pen = df_pen.merge(
        df_norm,
        on=["penalty_list_season", "gender", "penalty_list_date"],
        how="left",
    )
    df_pen[out_col] = df_pen["normalization_factor"] * df_pen[penalty_col]
    df_pen = df_pen.drop(columns=["normalization_factor"])

    return df_pen


def calculate_penalties_2025(
    df_log: pd.DataFrame,
    *,
    target: pd.DataFrame,
    target_mean: float = 200.0,
    robust_pctiles: tuple[float, float] = (0.25, 0.75),
) -> dict[str, np.ndarray]:
    """
    Fit on df_log (race-grain), return arrays aligned to `target` (athlete-grain).
    Intended usage:
        df_ext = df_ext_prep.assign(**calculate_penalties_2025(df_log, target=df_ext_prep))
    """

    group_cols = ["penalty_list_season", "penalty_list_date", "gender"]

    # ---- helpers ----

    def construct_X_y(group: pd.DataFrame):
        g = group.loc[group["athlete_id"] != group["reference_athlete_id"]].copy()
        if g.empty:
            return None, None

        athletes = pd.Index(
            pd.concat([g["athlete_id"], g["reference_athlete_id"]]).unique()
        )
        X = pd.DataFrame(0.0, index=range(len(g)), columns=athletes)

        # IMPORTANT: use positional loop to avoid NaNs from index mismatch
        a_ids = g["athlete_id"].to_numpy()
        r_ids = g["reference_athlete_id"].to_numpy()
        col_index = {col: j for j, col in enumerate(X.columns)}

        for i in range(len(g)):
            X.iat[i, col_index[a_ids[i]]] = 1.0
            X.iat[i, col_index[r_ids[i]]] = -1.0

        y = g["time_log_ratio"].to_numpy()

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

    # ---- fit per (season, date, gender) -> athlete penalties ----

    dfs = []
    for key, grp in df_log.groupby(group_cols, dropna=False):
        X, y = construct_X_y(grp)
        if X is None:
            continue

        model = linear_model.LinearRegression()
        model.fit(X, y)

        raw = np.exp(model.coef_)
        athlete_penalty = scale_to_target_mean(raw)

        dfs.append(
            pd.DataFrame(
                {
                    "penalty_list_season": key[0],
                    "penalty_list_date": key[1],
                    "gender": key[2],
                    "athlete_id": X.columns.astype("string"),
                    "athlete_penalty_25": athlete_penalty,
                }
            )
        )

    if not dfs:
        # align to target: all NaNs
        n = len(target)
        return {
            "athlete_penalty_25": np.full(n, np.nan, dtype="float64"),
            "standardized_athlete_penalty_25": np.full(n, np.nan, dtype="float64"),
        }

    df_pen = pd.concat(dfs, ignore_index=True)

    # ---- standardize within each (season, gender, date) ----
    df_pen = standardize_penalties_first_date_cohort(
        df_pen,
        target_mean=target_mean,
        robust_pctiles=robust_pctiles,
        penalty_col="athlete_penalty_25",
        out_col="standardized_athlete_penalty_25",
    )

    # ---- align onto the *target* dataframe’s rows ----

    key_cols = ["penalty_list_season", "penalty_list_date", "gender", "athlete_id"]
    target_idx = target.set_index(key_cols).index
    df_pen = df_pen.set_index(key_cols)

    aligned = df_pen.reindex(target_idx)

    return {
        "athlete_penalty_25": aligned["athlete_penalty_25"].to_numpy(),
        "standardized_athlete_penalty_25": aligned[
            "standardized_athlete_penalty_25"
        ].to_numpy(),
    }


# -----------------------------------------------------------------------------
# 2.5) 2026 penalties with outlier removal
# -----------------------------------------------------------------------------

import numpy as np
import pandas as pd
from sklearn import linear_model


def calculate_penalties_2026(
    df_log: pd.DataFrame,
    *,
    target: pd.DataFrame,
    target_mean: float = 200.0,
    robust_pctiles: tuple[float, float] = (0.25, 0.75),
    outlier_min_races: int = 5,
    outlier_sigma: float = 2.0,
) -> tuple[pd.DataFrame, dict[str, np.ndarray]]:
    """
    Returns:
      (df_log_with_outliers, penalty_dict)

    Where:
      - df_log_with_outliers: df_log with boolean column is_outlier_race (race-grain)
      - penalty_dict: arrays aligned to `target` (athlete-grain), keys:
          * athlete_penalty_26
          * standardized_athlete_penalty_26
    """

    group_cols = ["penalty_list_season", "penalty_list_date", "gender"]

    # ---- helpers ----

    def construct_X_y(group: pd.DataFrame):
        # Match 2025: omit rows where athlete_id == reference_athlete_id
        g = group.loc[group["athlete_id"] != group["reference_athlete_id"]].copy()
        if g.empty:
            return None, None, None

        athletes = pd.Index(
            pd.concat([g["athlete_id"], g["reference_athlete_id"]]).unique()
        )
        X = pd.DataFrame(0.0, index=range(len(g)), columns=athletes)

        a_ids = g["athlete_id"].to_numpy()
        r_ids = g["reference_athlete_id"].to_numpy()
        col_index = {col: j for j, col in enumerate(X.columns)}

        for i in range(len(g)):
            X.iat[i, col_index[a_ids[i]]] = 1.0
            X.iat[i, col_index[r_ids[i]]] = -1.0

        y = g["time_log_ratio"].to_numpy()

        # mean-to-zero constraint row (same as 2025)
        mean_row = pd.DataFrame(
            {c: 1.0 / len(athletes) for c in athletes}, index=[len(X)]
        )
        X = pd.concat([X, mean_row], axis=0)
        y = np.append(y, 0.0)

        return X, y, g

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

    # ---- storage ----
    penalty_rows: list[pd.DataFrame] = []

    # For outlier flags, accumulate a mapping at race-grain by stable keys
    outlier_key_cols = group_cols + ["race_id", "athlete_id"]
    outlier_rows: list[pd.DataFrame] = []

    # ---- per-group fit ----
    for key, grp in df_log.groupby(group_cols, dropna=False):
        X1, y1, g1 = construct_X_y(grp)
        if X1 is None:
            continue

        # ----- first fit -----
        model1 = linear_model.LinearRegression()
        model1.fit(X1, y1)

        # Residuals on real rows only (exclude constraint row)
        X1_real = X1.iloc[:-1, :]
        y1_real = y1[:-1]
        pred1 = model1.predict(X1_real)
        resid1 = y1_real - pred1  # positive => slower than model expects (worse)

        g1 = g1.copy()
        g1["_resid"] = resid1

        # ----- outlier identification -----
        race_counts = g1.groupby("athlete_id", dropna=False).size()
        eligible = race_counts.index[race_counts >= outlier_min_races]

        if len(eligible) > 0:
            stats = (
                g1.loc[g1["athlete_id"].isin(eligible)]
                .groupby("athlete_id", dropna=False)["_resid"]
                .agg(["mean", "std"])
                .rename(columns={"mean": "_resid_mean", "std": "_resid_std"})
            )

            g1 = g1.merge(stats, left_on="athlete_id", right_index=True, how="left")

            g1["is_outlier_race"] = (
                g1["_resid_std"].notna()
                & (g1["_resid_std"] > 0)
                & (
                    g1["_resid"]
                    > (g1["_resid_mean"] + outlier_sigma * g1["_resid_std"])
                )
            )
        else:
            g1["is_outlier_race"] = False

        # Save outlier flags for this group at race-grain
        outlier_rows.append(
            g1.assign(
                penalty_list_season=key[0],
                penalty_list_date=key[1],
                gender=key[2],
            )[outlier_key_cols + ["is_outlier_race"]]
        )

        # ----- second fit (omit outliers) -----
        outlier_idx = g1.index[g1["is_outlier_race"]].to_numpy()
        grp2 = grp.drop(index=outlier_idx) if outlier_idx.size > 0 else grp

        X2, y2, _g2 = construct_X_y(grp2)

        # If refit collapses, fall back to first fit
        if X2 is None:
            coef = model1.coef_
            cols = X1.columns
        else:
            model2 = linear_model.LinearRegression()
            model2.fit(X2, y2)
            coef = model2.coef_
            cols = X2.columns

        raw = np.exp(coef)
        athlete_penalty = scale_to_target_mean(raw)

        penalty_rows.append(
            pd.DataFrame(
                {
                    "penalty_list_season": key[0],
                    "penalty_list_date": key[1],
                    "gender": key[2],
                    "athlete_id": cols.astype("string"),
                    "athlete_penalty_26": athlete_penalty,
                }
            )
        )

    # ---- build df_log_with_outliers ----
    df_log_with_outliers = df_log.copy()
    df_log_with_outliers["is_outlier_race"] = False

    if outlier_rows:
        df_out = pd.concat(outlier_rows, ignore_index=True)

        # In case duplicates arise, "any" is a safe aggregation
        df_out = (
            df_out.groupby(outlier_key_cols, dropna=False)["is_outlier_race"]
            .any()
            .reset_index()
        )

        # merge back preserving original order
        tmp = df_log_with_outliers.copy()
        tmp["_row_i"] = np.arange(len(tmp))
        tmp = tmp.merge(df_out, on=outlier_key_cols, how="left", suffixes=("", "_m"))
        # prefer merged value when present
        tmp["is_outlier_race"] = tmp["is_outlier_race_m"].fillna(False).astype(bool)
        tmp = (
            tmp.drop(columns=["is_outlier_race_m"])
            .sort_values("_row_i")
            .drop(columns=["_row_i"])
        )
        df_log_with_outliers = tmp

    # ---- handle empty penalties ----
    if not penalty_rows:
        n_target = len(target)
        return df_log_with_outliers, {
            "athlete_penalty_26": np.full(n_target, np.nan, dtype="float64"),
            "standardized_athlete_penalty_26": np.full(
                n_target, np.nan, dtype="float64"
            ),
        }

    df_pen = pd.concat(penalty_rows, ignore_index=True)

    # ---- standardize (same cohort logic as 2025) ----
    df_pen = standardize_penalties_first_date_cohort(
        df_pen,
        target_mean=target_mean,
        robust_pctiles=robust_pctiles,
        penalty_col="athlete_penalty_26",
        out_col="standardized_athlete_penalty_26",
    )

    # ---- align penalties onto target ----
    key_cols = ["penalty_list_season", "penalty_list_date", "gender", "athlete_id"]
    target_idx = target.set_index(key_cols).index
    df_pen = df_pen.set_index(key_cols)
    aligned = df_pen.reindex(target_idx)

    return df_log_with_outliers, {
        "athlete_penalty_26": aligned["athlete_penalty_26"].to_numpy(),
        "standardized_athlete_penalty_26": aligned[
            "standardized_athlete_penalty_26"
        ].to_numpy(),
    }


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
            df_dim_registrations=df_dim_registrations,
            df_dim_results=df_dim_results,
            df_ext_races_by_penalty_window=df_ext_races_by_penalty_window,
        )
    )

    df_ext_athlete_penalties_prep = build_df_ext_athlete_penalties_prep(
        df_log_ratio_times_by_publication_date=df_log_ratio_times_by_publication_date,
        df_dim_registrations=df_dim_registrations,
    )

    df_penalties_2025 = calculate_penalties_2025(
        df_log_ratio_times_by_publication_date,
        target=df_ext_athlete_penalties_prep,
    )

    df_results_by_penalty_window, df_penalties_2026 = calculate_penalties_2026(
        df_log_ratio_times_by_publication_date,
        target=df_ext_athlete_penalties_prep,
    )

    df_ext_athlete_penalties = (
        df_ext_athlete_penalties_prep.assign(**df_penalties_2025)
        .assign(**df_penalties_2026)
        .sort_values(
            [
                "penalty_list_season",
                "penalty_list_date",
                "gender",
                "standardized_athlete_penalty_26",
            ],
            ascending=[True, True, True, True],
        )
        .reset_index(drop=True)
    )

    return df_results_by_penalty_window, df_ext_athlete_penalties
