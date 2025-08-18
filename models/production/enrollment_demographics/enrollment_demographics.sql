{{ config(materialized='view', schema='views') }}

WITH total_students_cte AS (
    SELECT
        school_name,
        year,
        CAST(grade_level AS STRING) AS grade_level,
        COUNT(*) AS total_students
    FROM {{ source('views', 'student_to_teacher') }}
    WHERE year = '25-26'
    GROUP BY school_name, grade_level, year
),
actual_vs_budget AS (
    SELECT
        bec.school_name,
        '25-26' AS year,
        CAST(bec.grade_level AS STRING) AS grade_level,
        '' AS subgroup,
        '' AS demographic,
        ts.total_students AS student_count,
        CAST(bec.budgeted_enrollment AS INT64) AS budgeted_enrollment,
        CAST(bec.budgeted_enrollment AS INT64) - SAFE_CAST(ts.total_students AS INT64) AS seats_remaining
    FROM {{ source('enrollment', 'budgeted_enrollment_capacity_hardcode_8_18_25') }} AS bec
    LEFT JOIN total_students_cte AS ts
        ON bec.school_name = ts.school_name
       AND CAST(bec.grade_level AS STRING) = ts.grade_level
       AND ts.year = '25-26'
),
subgroup_counts AS (
    SELECT
        school_name,
        year,
        CAST(grade_level AS STRING) AS grade_level,
        'race' AS subgroup,
        race AS demographic,
        COUNT(*) AS student_count,
        CAST(NULL AS INT64) AS budgeted_enrollment,
        CAST(NULL AS INT64) AS seats_remaining
    FROM {{ source('views', 'student_to_teacher') }}
    WHERE year = '25-26'
    GROUP BY school_name, year, grade_level, race

    UNION ALL

    SELECT
        school_name,
        year,
        CAST(grade_level AS STRING) AS grade_level,
        'elastatus' AS subgroup,
        elastatus AS demographic,
        COUNT(*) AS student_count,
        CAST(NULL AS INT64) AS budgeted_enrollment,
        CAST(NULL AS INT64) AS seats_remaining
    FROM {{ source('views', 'student_to_teacher') }}
    WHERE year = '25-26'
    GROUP BY school_name, year, grade_level, elastatus

    UNION ALL

    SELECT
        school_name,
        year,
        CAST(grade_level AS STRING) AS grade_level,
        'sped_identifier' AS subgroup,
        sped_identifier AS demographic,
        COUNT(*) AS student_count,
        CAST(NULL AS INT64) AS budgeted_enrollment,
        CAST(NULL AS INT64) AS seats_remaining
    FROM {{ source('views', 'student_to_teacher') }}
    WHERE year = '25-26'
    GROUP BY school_name, year, grade_level, sped_identifier

    UNION ALL

    SELECT
        school_name,
        year,
        CAST(grade_level AS STRING) AS grade_level,
        'absenteeism_status' AS subgroup,
        absenteeism_status AS demographic,
        COUNT(*) AS student_count,
        CAST(NULL AS INT64) AS budgeted_enrollment,
        CAST(NULL AS INT64) AS seats_remaining
    FROM {{ source('views', 'student_to_teacher') }}
    WHERE year = '25-26'
    GROUP BY school_name, year, grade_level, absenteeism_status

    UNION ALL

    SELECT
        school_name,
        year,
        CAST(grade_level AS STRING) AS grade_level,
        'frlstatus' AS subgroup,
        frlstatus AS demographic,
        COUNT(*) AS student_count,
        CAST(NULL AS INT64) AS budgeted_enrollment,
        CAST(NULL AS INT64) AS seats_remaining
    FROM {{ source('views', 'student_to_teacher') }}
    WHERE year = '25-26'
    GROUP BY school_name, year, grade_level, frlstatus
)

SELECT *
FROM actual_vs_budget
UNION ALL
SELECT *
FROM subgroup_counts
ORDER BY school_name, grade_level, subgroup, demographic


--student count coming from student_to_teacher seats_remaining recalculated.