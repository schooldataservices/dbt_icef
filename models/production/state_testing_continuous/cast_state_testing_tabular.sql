{{ config(materialized='view', schema='views') }}

WITH cast_continuous AS (
  SELECT DISTINCT  
    studentidentifier, 
    student_number,
    assessmentname,
    scalescoreachievementlevel,
    proficiency,
    dfs_cast AS cast_dfs,
    scalescore,
    lexilemeasure,
    SAFE_CAST(`cast__life_sciences` AS FLOAT64) AS `cast__life_sciences`,
    SAFE_CAST(`cast__physical_sciences` AS FLOAT64) AS `cast__physical_sciences`,
    SAFE_CAST(`cast__earth_and_space_sciences` AS FLOAT64) AS `cast__earth_and_space_sciences`
  FROM {{ source('state_testing', 'state_testing_continuous') }}
  WHERE assessmentname IN ('CAST Summative Grade 5', 'CAST Summative Grade 8', 'CAST Summative Grade HS') 
), 

cast_2324 AS (
  SELECT DISTINCT  
    studentidentifier, 
    student_number,
    scalescoreachievementlevel AS scalescoreachievementlevel_2324,
    proficiency AS proficiency_2324
  FROM {{ source('state_testing', 'state_testing_2324') }}
  WHERE assessmentname IN ('CAST Summative Grade 5', 'CAST Summative Grade 8', 'CAST Summative Grade HS') 
),

stacked_state_testing AS (
  SELECT 
    cc.*,
    c24.scalescoreachievementlevel_2324,
    c24.proficiency_2324
  FROM cast_continuous cc
  LEFT JOIN cast_2324 c24
    ON cc.studentidentifier = c24.studentidentifier
),

student_to_teacher AS (
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
        ELSE NULL  -- Or whatever fallback you want
    END AS teacher

  FROM {{ source('views', 'student_to_teacher') }}
)

SELECT 
  sst.studentidentifier,
  sst.student_number,
  sst.assessmentname,
  sst.scalescoreachievementlevel,
  sst.proficiency,
  sst.cast_dfs,
  sst.scalescore,
  sst.lexilemeasure,
  sst.`cast__life_sciences`,
  sst.`cast__physical_sciences`,
  sst.`cast__earth_and_space_sciences`,
  sst.scalescoreachievementlevel_2324 AS `24_scalescoreachievementlevel`,
  sst.proficiency_2324 AS `24_proficiency`,
  st.lastfirst,
  st.grade_level,
  st.elastatus,
  st.race,
  st.school_name,
  st.sped_identifier,
  st.absenteeism_status,
  st.teacher
FROM stacked_state_testing sst
RIGHT JOIN student_to_teacher st
  ON sst.student_number = st.student_number
WHERE st.grade_level IN (5, 8, 12)
