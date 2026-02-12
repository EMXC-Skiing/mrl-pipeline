/*
===============================================================================
BUILD_EXT_RESULTS.SQL
-------------------------------------------------------------------------------
Purpose
    Construct df_ext_results by combining:

        - Athlete-level penalties (2025 and 2026 versions)
        - Race-level penalty calculations
        - Result-level finish data
        - Registration eligibility
        - Race metadata
        - Series-based compression rules

    This query is intended to be executed inside an in-process DuckDB engine.
    It returns a result-level table at the grain:

        (result_id)

    One row per race result, with parallel 2025 and 2026 point calculations.

-------------------------------------------------------------------------------
Inputs (registered as DuckDB tables)

    df_prep_penalty_contributions
        Series-level configuration:
            - is_point_generating
            - compression_factor_di
            - compression_factor_sp

    df_ext_athlete_penalties
        Athlete penalties by (penalty_list_date, athlete_id):
            - standardized_athlete_penalty_25
            - standardized_athlete_penalty_26

    df_dim_results
        Canonical race results:
            - result_id
            - race_id
            - athlete_id
            - race_date
            - race_season
            - gender
            - time_float
            - finish_place

    df_dim_races
        Race metadata:
            - race_name
            - discipline
            - technique
            - series
            - division
            - venue
            - race_format

    df_dim_registrations
        Season-level athlete registrations:
            - registration_id
            - is_ghost_registration

-------------------------------------------------------------------------------
High-Level Flow

1. most_recent_penalty_lists
        For each race_date, determine the latest penalty_list_date
        published on or before that race.

2. df_point_generating_races
        Determine whether each race is point-generating and retrieve
        compression factors by discipline.

3. df_results_with_athlete_penalties
        Join results with:
            - Most recent applicable penalty list
            - Athlete penalties (25 and 26 versions)
            - Registration status

        Compute:
            - Penaltied athlete flags
            - Penaltied athlete ranks (per race/gender)
            - Penaltied athlete counts (per race/gender)
            - Registration eligibility flags

4. df_race_penalties
        Compute race-level penalties for BOTH systems:

            2025:
                - Top 5 penaltied finishers
                - Geometric mean of penalties and times
                - Requires >= 3 penaltied athletes

            2026:
                - Top 6 penaltied finishers
                - Geometric mean of penalties and times
                - Requires >= 3 penaltied athletes

        Output grain:
            (race_id, gender)

5. Final SELECT (df_ext_results)
        For each result row:

            - Attach race metadata
            - Attach race penalties (25 and 26)
            - Compute time-to-penalty ratios
            - Apply compression logic
            - Compute race_points_25 and race_points_26
            - Compute within-season ranks
            - Generate 2026 eligibility and reason flags

-------------------------------------------------------------------------------
Notes

    - All joins are LEFT JOIN unless logic requires filtering.

    - Null race_penalty values indicate that the race is unscorable
      due to insufficient penalized finishers.

    - This query is designed to be executed as a single CTE chain
      and should NOT create persistent tables inside DuckDB.

===============================================================================
*/


WITH
-- 1) Most recent penalty list available for each race_date
most_recent_penalty_lists AS (
    SELECT DISTINCT
        r.race_date,
        MAX(p.penalty_list_date) AS penalty_list_date
    FROM df_dim_results AS r
    LEFT JOIN df_ext_athlete_penalties AS p
        ON p.penalty_list_date <= r.race_date
    GROUP BY 1
    ORDER BY race_date
),

-- 2) Point-generating races and compression factors by series
df_point_generating_races AS (
    SELECT
        races.race_id,
        races.series,
        CAST(c.is_point_generating AS BOOL) AS is_point_generating_race,
        c.compression_factor_di,
        c.compression_factor_sp
    FROM df_dim_races AS races
    LEFT JOIN df_prep_penalty_contributions AS c
        USING(series)
),

-- 3) Results with penalties (carry BOTH 25 and 26 in parallel)
df_results_with_athlete_penalties AS (
    SELECT
        r.result_id,
        r.race_id,
        r.athlete_id,
        r.race_season,
        reg.registration_id,
        r.athlete_name,
        r.time,
        r.time_float,
        r.finish_place,
        r.race_date,
        r.gender,
        l.penalty_list_date,

        -- penalties for both vintages
        p.standardized_athlete_penalty_25 AS athlete_penalty_25,
        p.standardized_athlete_penalty_26 AS athlete_penalty_26,

        -- eligibility flags (shared)
        (r.time_float IS NOT NULL) AND (reg.registration_id IS NOT NULL) AS is_registered_or_ghost_finish,
        (r.time_float IS NOT NULL) AND (reg.registration_id IS NOT NULL) AND (NOT COALESCE(reg.is_ghost_registration, FALSE)) AS is_registered_finish,

        -- penalized-finish flags per vintage
        (r.time_float IS NOT NULL) AND (p.standardized_athlete_penalty_25 IS NOT NULL) AS is_penaltied_athlete_finish_25,
        (r.time_float IS NOT NULL) AND (p.standardized_athlete_penalty_26 IS NOT NULL) AS is_penaltied_athlete_finish_26,

        -- ranks per vintage (only for penalized athletes)
        CASE
            WHEN (r.time_float IS NOT NULL) AND (p.standardized_athlete_penalty_25 IS NOT NULL)
            THEN RANK() OVER (
                PARTITION BY r.race_id, r.gender, ((r.time_float IS NOT NULL) AND (p.standardized_athlete_penalty_25 IS NOT NULL))
                ORDER BY r.time_float ASC
            )
            ELSE NULL
        END AS penaltied_athlete_place_25,

        CASE
            WHEN (r.time_float IS NOT NULL) AND (p.standardized_athlete_penalty_26 IS NOT NULL)
            THEN RANK() OVER (
                PARTITION BY r.race_id, r.gender, ((r.time_float IS NOT NULL) AND (p.standardized_athlete_penalty_26 IS NOT NULL))
                ORDER BY r.time_float ASC
            )
            ELSE NULL
        END AS penaltied_athlete_place_26,

        -- counts per vintage
        COUNT(*) FILTER (
            WHERE (r.time_float IS NOT NULL) AND (p.standardized_athlete_penalty_25 IS NOT NULL)
        ) OVER (PARTITION BY r.race_id, r.gender) AS penaltied_athlete_count_25,

        COUNT(*) FILTER (
            WHERE (r.time_float IS NOT NULL) AND (p.standardized_athlete_penalty_26 IS NOT NULL)
        ) OVER (PARTITION BY r.race_id, r.gender) AS penaltied_athlete_count_26

    FROM df_dim_results AS r
    LEFT JOIN most_recent_penalty_lists AS l
        ON r.race_date = l.race_date
    LEFT JOIN df_ext_athlete_penalties AS p
        ON l.penalty_list_date = p.penalty_list_date
        AND r.athlete_id = p.athlete_id
    LEFT JOIN df_dim_registrations AS reg
        ON (r.athlete_id = reg.athlete_id AND r.race_season = reg.race_season)
    ORDER BY r.race_date, r.race_id, r.gender, r.time_float
),

-- 4) Race penalties (compute BOTH 25 and 26 in parallel)

race_penalties_and_times_25 AS (
    SELECT
        race_id,
        gender,
        GEOMETRIC_MEAN(athlete_penalty_25) AS mean_podium_seed_25,
        GEOMETRIC_MEAN(time_float) AS mean_podium_time_25
    FROM df_results_with_athlete_penalties 
    WHERE penaltied_athlete_count_25 >=3
        AND penaltied_athlete_place_25 BETWEEN 1 and 5
    GROUP BY race_id, gender
),

race_penalties_and_times_26 AS (
    SELECT
        race_id,
        gender,
        GEOMETRIC_MEAN(athlete_penalty_26) AS mean_podium_seed_26,
        GEOMETRIC_MEAN(time_float) AS mean_podium_time_26
    FROM df_results_with_athlete_penalties 
    WHERE penaltied_athlete_count_26 >=3
        AND penaltied_athlete_place_26 BETWEEN 1 and 6
    GROUP BY race_id, gender
),

df_race_penalties AS (
    SELECT
        races.race_id,
        races.race_season,
        races.race_date,
        g.gender,

        ROUND(pt25.mean_podium_seed_25, 2) AS race_penalty_25,
        pt25.mean_podium_time_25 AS race_penalty_time_25,

        ROUND(pt26.mean_podium_seed_26, 2) AS race_penalty_26,
        pt26.mean_podium_time_26 AS race_penalty_time_26

    FROM df_dim_races AS races
    CROSS JOIN (VALUES ('b'), ('g')) AS g(gender)
    LEFT JOIN race_penalties_and_times_25 AS pt25
        ON (races.race_id = pt25.race_id AND g.gender = pt25.gender)
    LEFT JOIN race_penalties_and_times_26 AS pt26
        ON (races.race_id = pt26.race_id AND g.gender = pt26.gender)        
    ORDER BY races.race_date, races.race_id, g.gender
)

-- 5) Final: result-level points, both 25 and 26 in parallel
SELECT
    res.*,

    races.race_name,
    races.discipline,
    races.technique,
    races.series,
    races.division,
    races.race_format,
    races.venue,

    c.is_point_generating_race,

    -- bring both race penalty versions
    rp.race_penalty_25,
    rp.race_penalty_time_25,
    rp.race_penalty_26,
    rp.race_penalty_time_26,

    -- compression factors
    1.0 AS compression_factor_25,

    CASE
        WHEN races.discipline = 'DI' THEN COALESCE(c.compression_factor_di, 1.0)
        WHEN races.discipline = 'SP' THEN COALESCE(c.compression_factor_sp, 1.0)
        ELSE 1.0
    END AS compression_factor_26,

    -- ratios
    CASE
        WHEN (res.is_registered_finish AND c.is_point_generating_race)
        THEN res.time_float / rp.race_penalty_time_25
        ELSE NULL
    END AS ma_time_penalty_ratio_25,

    CASE
        WHEN (res.is_registered_finish AND c.is_point_generating_race)
        THEN res.time_float / rp.race_penalty_time_26
        ELSE NULL
    END AS ma_time_penalty_ratio_26,

    -- points
    ROUND(rp.race_penalty_25 * POW(ma_time_penalty_ratio_25, compression_factor_25), 2) AS race_points_25,
    ROUND(rp.race_penalty_26 * POW(ma_time_penalty_ratio_26, compression_factor_26), 2) AS race_points_26,

    -- ranks (25)
    CASE WHEN res.is_registered_finish THEN
        RANK() OVER (
            PARTITION BY res.race_season, res.athlete_id
            ORDER BY race_points_25 ASC
        )
    ELSE NULL END AS race_rank_by_athlete_season_25,

    CASE WHEN res.is_registered_finish THEN
        RANK() OVER (
            PARTITION BY res.race_season, res.athlete_id, races.technique
            ORDER BY race_points_25 ASC
        )
    ELSE NULL END AS race_rank_per_technique_by_athlete_season_25,

    -- ranks (26)
    CASE WHEN res.is_registered_finish THEN
        RANK() OVER (
            PARTITION BY res.race_season, res.athlete_id
            ORDER BY race_points_26 ASC
        )
    ELSE NULL END AS race_rank_by_athlete_season_26,

    CASE WHEN res.is_registered_finish THEN
        RANK() OVER (
            PARTITION BY res.race_season, res.athlete_id, races.technique
            ORDER BY race_points_26 ASC
        )
    ELSE NULL END AS race_rank_per_technique_by_athlete_season_26,

    -- 2026 string field (based on 26 points)
    CASE WHEN rp.race_season >= 2026 AND res.is_registered_finish THEN CAST(race_points_26 AS VARCHAR) ELSE '' END AS ma_points_2026_str,

    -- Reason for no points (kept aligned with your original 2026-oriented logic; uses 26 race-penalty sufficiency)
    CASE
        WHEN rp.race_season < 2026 THEN ''
        WHEN NOT c.is_point_generating_race THEN 'Race is not scored for points'
        WHEN res.time_float IS NULL THEN 'Racer has no finish time (e.g., DNS/DNF/DQ)'
        WHEN res.registration_id IS NULL OR (NOT res.is_registered_finish) THEN 'Racer is not eligible for points'
        WHEN res.penaltied_athlete_count_26 < 3 THEN 'Race is unscorable'
        WHEN race_points_26 IS NULL THEN 'No points earned: other reason'
        ELSE ''
    END AS reason_for_no_points

FROM df_results_with_athlete_penalties AS res
LEFT JOIN df_point_generating_races AS c
    ON (c.race_id = res.race_id)
LEFT JOIN df_race_penalties AS rp
    ON (rp.race_id = res.race_id AND rp.gender = res.gender)
LEFT JOIN df_dim_races AS races
    ON (races.race_id = res.race_id)
;