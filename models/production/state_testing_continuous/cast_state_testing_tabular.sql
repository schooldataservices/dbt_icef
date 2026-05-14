{{ config(materialized='view', schema='views') }}

-- CAST summative: current from state_testing_continuous (24-25) and state_testing_continuous_25_26 (25-26).
-- prev_year_* = prior school year CAST summative: 23-24 from state_testing_2324 when year = '24-25';
-- 24-25 from state_testing_continuous when year = '25-26'.
-- Domain scores (cast__*): STRING reporting bands / category text on both continuous extracts.
-- Historical snapshot still uses legacy column names; aliased below.

WITH cast_prior_2324 AS (
  SELECT DISTINCT
    studentidentifier,
    student_number,
    scalescoreachievementlevel AS scalescoreachievementlevel_prior,
    proficiency AS proficiency_prior
  FROM {{ source('state_testing', 'state_testing_2324') }}
  WHERE assessmentname IN ('CAST Summative Grade 5', 'CAST Summative Grade 8', 'CAST Summative Grade HS')
),

cast_prior_2425_continuous AS (
  SELECT DISTINCT
    studentidentifier,
    student_number,
    scalescoreachievementlevel AS scalescoreachievementlevel_prior,
    proficiency AS proficiency_prior
  FROM {{ source('state_testing', 'state_testing_continuous') }}
  WHERE assessmentname IN ('CAST Summative Grade 5', 'CAST Summative Grade 8', 'CAST Summative Grade HS')
),

cast_continuous_2425 AS (
  SELECT DISTINCT
    studentidentifier,
    student_number,
    assessmentname,
    scalescoreachievementlevel,
    proficiency,
    dfs_cast AS cast_dfs,
    scalescore,
    lexilemeasure,
    -- Reporting-category performance (STRING); do not cast to FLOAT64 or bands become NULL.
    CAST(`cast__life_sciences` AS STRING) AS `cast__life_sciences`,
    CAST(`cast__physical_sciences` AS STRING) AS `cast__physical_sciences`,
    CAST(`cast__earth_and_space_sciences` AS STRING) AS `cast__earth_and_space_sciences`
  FROM {{ source('state_testing', 'state_testing_continuous') }}
  WHERE assessmentname IN ('CAST Summative Grade 5', 'CAST Summative Grade 8', 'CAST Summative Grade HS')
),

cast_continuous_2526 AS (
  SELECT DISTINCT
    a.StudentIdentifier AS studentidentifier,
    a.student_number,
    a.AssessmentName AS assessmentname,
    a.ScaleScoreAchievementLevel AS scalescoreachievementlevel,
    a.proficiency,
    a.dfs_cast AS cast_dfs,
    a.ScaleScore AS scalescore,
    a.LexileMeasure AS lexilemeasure,
    CAST(a.`CAST: Life Sciences` AS STRING) AS `cast__life_sciences`,
    CAST(a.`CAST: Physical Sciences` AS STRING) AS `cast__physical_sciences`,
    CAST(a.`CAST: Earth and Space Sciences` AS STRING) AS `cast__earth_and_space_sciences`
  FROM {{ source('state_testing', 'state_testing_continuous_25_26') }} a
  WHERE a.AssessmentName IN ('CAST Summative Grade 5', 'CAST Summative Grade 8', 'CAST Summative Grade HS')
),

stacked_2425 AS (
  SELECT
    cc.*,
    CAST(c23.scalescoreachievementlevel_prior AS FLOAT64) AS prev_year_scalescoreachievementlevel,
    c23.proficiency_prior AS prev_year_proficiency
  FROM cast_continuous_2425 cc
  LEFT JOIN cast_prior_2324 c23
    ON cc.studentidentifier = c23.studentidentifier
),

stacked_2526 AS (
  SELECT
    cc.*,
    CAST(c2425.scalescoreachievementlevel_prior AS FLOAT64) AS prev_year_scalescoreachievementlevel,
    c2425.proficiency_prior AS prev_year_proficiency
  FROM cast_continuous_2526 cc
  LEFT JOIN cast_prior_2425_continuous c2425
    ON cc.studentidentifier = c2425.studentidentifier
),

student_to_teacher_2425 AS (
  SELECT
    student_number,
    lastfirst,
    grade_level,
    elastatus,
    race,
    school_name,
    sped_identifier,
    absenteeism_status,
    science_teacher,
    math_teacher,
    CASE
      WHEN grade_level = 5 THEN math_teacher
      WHEN grade_level IN (8, 12) THEN science_teacher
      ELSE NULL
    END AS teacher
  FROM {{ source('views', 'student_to_teacher') }}
  WHERE year = '24-25'
),

student_to_teacher_2526 AS (
  SELECT
    student_number,
    lastfirst,
    grade_level,
    elastatus,
    race,
    school_name,
    sped_identifier,
    absenteeism_status,
    science_teacher,
    math_teacher,
    CASE
      WHEN grade_level = 5 THEN math_teacher
      WHEN grade_level IN (8, 12) THEN science_teacher
      ELSE NULL
    END AS teacher
  FROM {{ source('views', 'student_to_teacher') }}
  WHERE year = '25-26'
),

current_year_2425 AS (
  SELECT
    sst.studentidentifier,
    sst.student_number,
    sst.assessmentname,
    sst.scalescoreachievementlevel,
    sst.proficiency,
    sst.cast_dfs,
    sst.scalescore,
    CAST(sst.lexilemeasure AS FLOAT64) AS lexilemeasure,
    sst.`cast__life_sciences`,
    sst.`cast__physical_sciences`,
    sst.`cast__earth_and_space_sciences`,
    sst.prev_year_scalescoreachievementlevel,
    sst.prev_year_proficiency,
    st.lastfirst,
    st.grade_level,
    st.elastatus,
    st.race,
    st.school_name,
    st.sped_identifier,
    st.absenteeism_status,
    st.teacher,
    '24-25' AS year
  FROM stacked_2425 sst
  RIGHT JOIN student_to_teacher_2425 st
    ON sst.student_number = st.student_number
  WHERE st.grade_level IN (5, 8, 12)
),

current_year_2526 AS (
  SELECT
    sst.studentidentifier,
    sst.student_number,
    sst.assessmentname,
    sst.scalescoreachievementlevel,
    sst.proficiency,
    sst.cast_dfs,
    sst.scalescore,
    CAST(sst.lexilemeasure AS FLOAT64) AS lexilemeasure,
    sst.`cast__life_sciences`,
    sst.`cast__physical_sciences`,
    sst.`cast__earth_and_space_sciences`,
    sst.prev_year_scalescoreachievementlevel,
    sst.prev_year_proficiency,
    st.lastfirst,
    st.grade_level,
    st.elastatus,
    st.race,
    st.school_name,
    st.sped_identifier,
    st.absenteeism_status,
    st.teacher,
    '25-26' AS year
  FROM stacked_2526 sst
  RIGHT JOIN student_to_teacher_2526 st
    ON sst.student_number = st.student_number
  WHERE st.grade_level IN (5, 8, 12)
),

historical AS (
  SELECT
    studentidentifier,
    student_number,
    assessmentname,
    scalescoreachievementlevel,
    proficiency,
    cast_dfs,
    scalescore,
    CAST(lexilemeasure AS FLOAT64) AS lexilemeasure,
    CAST(`cast__life_sciences` AS STRING) AS `cast__life_sciences`,
    CAST(`cast__physical_sciences` AS STRING) AS `cast__physical_sciences`,
    CAST(`cast__earth_and_space_sciences` AS STRING) AS `cast__earth_and_space_sciences`,
    CAST(`24_scalescoreachievementlevel` AS FLOAT64) AS prev_year_scalescoreachievementlevel,
    `24_proficiency` AS prev_year_proficiency,
    lastfirst,
    grade_level,
    elastatus,
    race,
    school_name,
    sped_identifier,
    absenteeism_status,
    teacher,
    '24-25' AS year
  FROM `icef-437920.dbt_historical.cast_state_testing_tabular_24-25`
)

SELECT DISTINCT *
FROM (
  SELECT * FROM current_year_2425
  UNION ALL
  SELECT * FROM current_year_2526
  UNION ALL
  SELECT * FROM historical
)


