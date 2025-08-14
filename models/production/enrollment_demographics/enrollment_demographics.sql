{{ config(materialized='view', schema='views') }}

WITH subgroup_counts AS (
    SELECT
        school_name,
        CAST(grade_level AS STRING) AS grade_level,
        'race' AS subgroup,
        race AS demographic,
        COUNT(*) AS student_count,
        CAST(NULL AS INT64) AS budgeted_enrollment,
        CAST(NULL AS INT64) AS seats_remaining
    FROM {{ source('views', 'student_to_teacher') }}
    GROUP BY school_name, grade_level, race

    UNION ALL

    SELECT
        school_name,
        CAST(grade_level AS STRING) AS grade_level,
        'elastatus' AS subgroup,
        elastatus AS demographic,
        COUNT(*) AS student_count,
        CAST(NULL AS INT64) AS budgeted_enrollment,
        CAST(NULL AS INT64) AS seats_remaining
    FROM {{ source('views', 'student_to_teacher') }}
    GROUP BY school_name, grade_level, elastatus

    UNION ALL

    SELECT
        school_name,
        CAST(grade_level AS STRING) AS grade_level,
        'sped_identifier' AS subgroup,
        sped_identifier AS demographic,
        COUNT(*) AS student_count,
        CAST(NULL AS INT64) AS budgeted_enrollment,
        CAST(NULL AS INT64) AS seats_remaining
    FROM {{ source('views', 'student_to_teacher') }}
    GROUP BY school_name, grade_level, sped_identifier

    UNION ALL

    SELECT
        school_name,
        CAST(grade_level AS STRING) AS grade_level,
        'absenteeism_status' AS subgroup,
        absenteeism_status AS demographic,
        COUNT(*) AS student_count,
        CAST(NULL AS INT64) AS budgeted_enrollment,
        CAST(NULL AS INT64) AS seats_remaining
    FROM {{ source('views', 'student_to_teacher') }}
    GROUP BY school_name, grade_level, absenteeism_status

    UNION ALL

    SELECT
        school_name,
        CAST(grade_level AS STRING) AS grade_level,
        'frlstatus' AS subgroup,
        frlstatus AS demographic,
        COUNT(*) AS student_count,
        CAST(NULL AS INT64) AS budgeted_enrollment,
        CAST(NULL AS INT64) AS seats_remaining
    FROM {{ source('views', 'student_to_teacher') }}
    GROUP BY school_name, grade_level, frlstatus
)

SELECT * 
FROM subgroup_counts

UNION ALL

SELECT
    school AS school_name,
    CAST(grade AS STRING) AS grade_level,
    '' AS subgroup,
    '' AS demographic,
    total_enrollment AS student_count,
    CAST(budgeted_enrollment AS INT64) AS budgeted_enrollment,
    CAST(seats_available AS INT64) AS seats_remaining
FROM {{ source('enrollment', 'budgeted_enrollment_capacity') }}
WHERE grade != 'total'




