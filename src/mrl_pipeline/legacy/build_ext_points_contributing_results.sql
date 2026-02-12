/*
ext_points_contributing_results
Carry _25 and _26 points in parallel (same slot logic; separate ranks & comparisons).
*/

WITH points_results AS (
    SELECT
        *,
        series <> 'MAQ' AS is_not_maq,

        CASE WHEN (series <> 'MAQ') AND (race_points_25 IS NOT NULL) THEN
            row_number() OVER (
                PARTITION BY registration_id, (series <> 'MAQ'), technique
                ORDER BY race_points_25 ASC
            )
        END AS race_rank_excluding_maq_25,

        CASE WHEN (series <> 'MAQ') AND (race_points_26 IS NOT NULL) THEN
            row_number() OVER (
                PARTITION BY registration_id, (series <> 'MAQ'), technique
                ORDER BY race_points_26 ASC
            )
        END AS race_rank_excluding_maq_26

    FROM df_ext_results
    WHERE is_registered_finish
      AND (race_points_25 IS NOT NULL OR race_points_26 IS NOT NULL)
),

distinct_registrations AS (
    SELECT DISTINCT
        reg.registration_id,
        t.technique
    FROM df_dim_registrations AS reg
    CROSS JOIN (VALUES ('CL'), ('SK')) AS t(technique)
    WHERE NOT reg.is_ghost_registration
),

maq_points_results AS (
    SELECT
        d.registration_id,
        r.athlete_name,
        r.race_season,
        d.technique,
        '1. MA Nordic Cup' AS description,
        1 AS weight,

        first(p.race_points_25) AS maq_points_25,
        first(p.race_name)      AS maq_race_name_25,
        first(p.race_points_25) AS maq_race_points_25,
        first(p.series)         AS maq_race_series_25,

        first(p.race_points_26) AS maq_points_26,
        first(p.race_name)      AS maq_race_name_26,
        first(p.race_points_26) AS maq_race_points_26,
        first(p.series)         AS maq_race_series_26

    FROM distinct_registrations AS d
    LEFT JOIN df_dim_registrations AS r
        ON d.registration_id = r.registration_id
    LEFT JOIN points_results AS p
        ON d.registration_id = p.registration_id
       AND d.technique = p.technique
       AND p.series = 'MAQ'
    GROUP BY 1,2,3,4
    ORDER BY 1,2
),

top_other_points_races AS (
    SELECT
        r.registration_id,
        r.athlete_name,
        r.race_season,
        r.technique,
        points_rank.description,
        1 AS weight,

        -- carry MAQ points (both versions)
        maq.maq_race_points_25 AS maq_points_25,
        maq.maq_race_points_26 AS maq_points_26,

        -- Best contributing race for 25
        first(best25.race_name)      AS contributing_race_name_25,
        first(best25.race_points_25) AS contributing_race_points_25,
        first(best25.series)         AS contributing_race_series_25,

        -- Best contributing race for 26
        first(best26.race_name)      AS contributing_race_name_26,
        first(best26.race_points_26) AS contributing_race_points_26,
        first(best26.series)         AS contributing_race_series_26

    FROM df_ext_results AS r
    CROSS JOIN (VALUES
        ('2. First Best Points Race (if better than MA Nordic Cup points)', 1),
        ('3. Second Best Points Race (if better than MA Nordic Cup points)', 2),
        ('4. Third Best Points Race (if better than MA Nordic Cup points)', 3)
    ) AS points_rank(description, rk)

    LEFT JOIN maq_points_results AS maq
        ON maq.registration_id = r.registration_id
       AND maq.technique = r.technique

    -- pick best race for _25
    LEFT JOIN points_results AS best25
        ON best25.registration_id = r.registration_id
       AND best25.technique = r.technique
       AND best25.race_rank_excluding_maq_25 = points_rank.rk
       AND (
            CASE
                WHEN maq.maq_race_points_25 IS NOT NULL
                    THEN best25.race_points_25 < maq.maq_race_points_25
                ELSE TRUE
            END
       )

    -- pick best race for _26
    LEFT JOIN points_results AS best26
        ON best26.registration_id = r.registration_id
       AND best26.technique = r.technique
       AND best26.race_rank_excluding_maq_26 = points_rank.rk
       AND (
            CASE
                WHEN maq.maq_race_points_26 IS NOT NULL
                    THEN best26.race_points_26 < maq.maq_race_points_26
                ELSE TRUE
            END
       )

    -- only calculate contributing for points 
    WHERE (r.registration_id IS NOT NULL AND (r.race_penalty_25 IS NOT NULL OR r.race_penalty_26 IS NOT NULL))
    GROUP BY ALL
    ORDER BY 1,4,5
),

unioned AS (
    SELECT
        registration_id,
        athlete_name,
        race_season,
        technique,
        description,
        weight,

        -- race info 25
        maq_race_name_25 AS contributing_race_name_25,
        maq_race_points_25 AS contributing_race_points_25,
        maq_race_series_25 AS contributing_race_series_25,
        maq_points_25,

        -- race info 26
        maq_race_name_26 AS contributing_race_name_26,
        maq_race_points_26 AS contributing_race_points_26,
        maq_race_series_26 AS contributing_race_series_26,
        maq_points_26

    FROM maq_points_results

    UNION ALL BY NAME

    SELECT
        registration_id,
        athlete_name,
        race_season,
        technique,
        description,
        weight,

        contributing_race_name_25,
        contributing_race_points_25,
        contributing_race_series_25,
        maq_points_25,

        contributing_race_name_26,
        contributing_race_points_26,
        contributing_race_series_26,
        maq_points_26

    FROM top_other_points_races
)

SELECT
    athlete_name,
    race_season,
    registration_id,
    technique,
    description,

    -- 25 fields
    contributing_race_name_25,
    contributing_race_points_25,
    contributing_race_series_25,
    maq_points_25,
    maq_points_25 IS NOT NULL AS is_maq_finisher_25,
    COALESCE(contributing_race_points_25, maq_points_25) AS coalesced_points_25,

    -- 26 fields
    contributing_race_name_26,
    contributing_race_points_26,
    contributing_race_series_26,
    maq_points_26,
    maq_points_26 IS NOT NULL AS is_maq_finisher_26,
    COALESCE(contributing_race_points_26, maq_points_26) AS coalesced_points_26,

    CASE WHEN COALESCE(contributing_race_points_25, maq_points_25) IS NOT NULL THEN weight ELSE NULL END AS coalesced_weight_25,
    CASE WHEN COALESCE(contributing_race_points_26, maq_points_26) IS NOT NULL THEN weight ELSE NULL END AS coalesced_weight_26

FROM unioned
ORDER BY athlete_name, race_season, technique, description
;
