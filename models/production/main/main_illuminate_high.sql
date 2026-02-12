{{ config(
    materialized='view',
    schema='views'
) }}

SELECT 
    CAST(st.year AS STRING) AS year,
    CAST(st.elastatus AS STRING) AS elastatus,
    CAST(st.english_teacher AS STRING) AS english_teacher,
    CAST(st.math_teacher AS STRING) AS math_teacher,
    CAST(st.science_teacher AS STRING) AS science_teacher,
    CAST(st.history_teacher AS STRING) AS history_teacher,
    CAST(st.lastfirst AS STRING) AS lastfirst,
    CAST(st.school_name AS STRING) AS school_name,
    CAST(st.sped_identifier AS STRING) AS sped_identifier,
    CAST(st.absenteeism_status AS STRING) AS absenteeism_status,
    CAST(st.grade_level AS INT64) AS grade_level,
    CAST(st.student_number AS INT64) AS student_number,
    CAST(m.data_source AS STRING) AS data_source,
    CAST(m.curriculum AS STRING) AS curriculum,
    CAST(m.proficiency AS STRING) AS proficiency,
    CAST(m.standard_code AS STRING) AS standard_code,
    CAST(m.test_type AS STRING) AS test_type,
    CAST(m.unit AS STRING) AS unit,
    CAST(m.title AS STRING) AS title,
    SAFE_CAST(m.score AS INT64) AS score
FROM {{ source('views', 'student_to_teacher') }} st 
LEFT JOIN {{ ref('main') }} m
    ON CAST(st.student_number AS STRING) = m.local_student_id
    AND st.year = m.year
WHERE m.grade IN ('9', '10', '11', '12')
    AND m.data_source = 'illuminate'


