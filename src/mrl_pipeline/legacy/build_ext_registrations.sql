/*
ext_registrations
Aggregate contributing results into technique points + total points (parallel _25/_26).
*/

WITH by_technique AS (
    SELECT
        registration_id,
        technique,

        -- is_maq_finisher flags should be constant per group
        bool_or(is_maq_finisher_25) AS is_maq_finisher_25,
        bool_or(is_maq_finisher_26) AS is_maq_finisher_26,

        -- weighted averages; NULL if no contributing weights
        fsum(coalesced_points_25 * coalesced_weight_25)
            / nullif(fsum(coalesced_weight_25), 0) AS technique_points_25,

        fsum(coalesced_points_26 * coalesced_weight_26)
            / nullif(fsum(coalesced_weight_26), 0) AS technique_points_26

    FROM df_ext_points_contributing_results
    GROUP BY 1, 2
)

SELECT
    r.*,

    -- MAQ finisher flags (25)
    sk.is_maq_finisher_25 AS is_maq_sk_finisher_25,
    cl.is_maq_finisher_25 AS is_maq_cl_finisher_25,

    -- MAQ finisher flags (26)
    sk.is_maq_finisher_26 AS is_maq_sk_finisher_26,
    cl.is_maq_finisher_26 AS is_maq_cl_finisher_26,

    -- Technique points (25)
    ROUND(sk.technique_points_25, 2) AS sk_points_25,
    ROUND(cl.technique_points_25, 2) AS cl_points_25,
    (sk_points_25 + cl_points_25) AS total_points_25,

    -- Technique points (26)
    ROUND(sk.technique_points_26, 2) AS sk_points_26,
    ROUND(cl.technique_points_26, 2) AS cl_points_26,
    (sk_points_26 + cl_points_26) AS total_points_26,

    -- qualifying places (25)
    CASE WHEN is_ehs_eligible AND total_points_25 IS NOT NULL THEN
        RANK() OVER (
            PARTITION BY race_season, gender, is_ehs_eligible
            ORDER BY total_points_25
        )
    END AS ehsc_qualifying_place_25,

    CASE WHEN is_u16c_eligible AND total_points_25 IS NOT NULL THEN
        RANK() OVER (
            PARTITION BY race_season, gender, is_u16c_eligible
            ORDER BY total_points_25
        )
    END AS u16c_qualifying_place_25,

    -- qualifying places (26)
    CASE WHEN is_ehs_eligible AND total_points_26 IS NOT NULL THEN
        RANK() OVER (
            PARTITION BY race_season, gender, is_ehs_eligible
            ORDER BY total_points_26
        )
    END AS ehsc_qualifying_place_26,

    CASE WHEN is_u16c_eligible AND total_points_26 IS NOT NULL THEN
        RANK() OVER (
            PARTITION BY race_season, gender, is_u16c_eligible
            ORDER BY total_points_26
        )
    END AS u16c_qualifying_place_26

FROM df_dim_registrations AS r
LEFT JOIN by_technique AS cl
    ON cl.registration_id = r.registration_id AND cl.technique = 'CL'
LEFT JOIN by_technique AS sk
    ON sk.registration_id = r.registration_id AND sk.technique = 'SK'
ORDER BY race_season, gender
;
