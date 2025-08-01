-- goes from multiple rows per assessmenttype and subject to one row per student based on ELA and Math, with assessmenttype summative
{{ config(materialized='view', schema='views') }}


WITH ela_continuous AS (
  SELECT DISTINCT  
    studentidentifier, 
    student_number,
    assessmentname AS ela_assessmentname,
    scalescoreachievementlevel AS ela_scalescoreachievementlevel,
    proficiency AS ela_proficiency,
    dfs_ela AS ela_dfs,
    scalescore AS ela_scalescore,
    lexilemeasure AS ela_lexilemeasure,
    SAFE_CAST(`ela__reading` AS FLOAT64) AS `ela__reading`,
    SAFE_CAST(`ela__writing` AS FLOAT64) AS `ela__writing`,
    SAFE_CAST(`ela__listening` AS FLOAT64) AS `ela__listening`,
    SAFE_CAST(`ela__research_and_inquiry` AS FLOAT64) AS `ela__research_and_inquiry`
  FROM {{ source('state_testing', 'state_testing_continuous') }}
  WHERE assessmenttype = 'Summative' AND subject = 'ELA'
), 

math_continuous AS (
  SELECT DISTINCT  
    studentidentifier, 
    student_number,
    assessmentname AS math_assessmentname,
    scalescoreachievementlevel AS math_scalescoreachievementlevel,
    proficiency AS math_proficiency,
    dfs_math AS math_dfs,
    scalescore AS math_scalescore,
    lexilemeasure AS math_lexilemeasure,
    SAFE_CAST(`math__concepts_and_procedures` AS FLOAT64) AS `math__concepts_and_procedures`,
    SAFE_CAST(`math__problem_solving_and_modeling___data_analysis` AS FLOAT64) AS `math__problem_solving_and_modeling___data_analysis`,
    SAFE_CAST(`math__communicating_reasoning` AS FLOAT64) AS `math__communicating_reasoning`
  FROM {{ source('state_testing', 'state_testing_continuous') }}
  WHERE assessmenttype = 'Summative' AND subject = 'Math'
),

joined_continuous AS (
  SELECT 
    COALESCE(e.studentidentifier, m.studentidentifier) AS studentidentifier,
    COALESCE(e.student_number, m.student_number) AS student_number,

    -- ELA columns
    e.ela_assessmentname,
    e.ela_scalescoreachievementlevel,
    e.ela_proficiency,
    e.ela_dfs,
    e.ela_scalescore,
    e.ela_lexilemeasure,
    e.`ela__reading`,
    e.`ela__writing`,
    e.`ela__listening`,
    e.`ela__research_and_inquiry`,

    -- Math columns
    m.math_assessmentname,
    m.math_scalescoreachievementlevel,
    m.math_proficiency,
    m.math_dfs,
    m.math_scalescore,
    m.math_lexilemeasure,
    m.`math__concepts_and_procedures`,
    m.`math__problem_solving_and_modeling___data_analysis`,
    m.`math__communicating_reasoning`

  FROM ela_continuous e
  FULL OUTER JOIN math_continuous m
    ON e.studentidentifier = m.studentidentifier
),

ela_2324 AS (
  SELECT DISTINCT  
    studentidentifier, 
    student_number,
    scalescoreachievementlevel AS ela_scalescoreachievementlevel,
    dfs_ela AS ela_dfs,
    proficiency
  FROM {{ source('state_testing', 'state_testing_2324') }}
  WHERE assessmenttype = 'Summative' AND subject = 'ELA'
), 

math_2324 AS (
  SELECT DISTINCT  
    studentidentifier, 
    student_number,
    scalescoreachievementlevel AS math_scalescoreachievementlevel,
    dfs_math AS math_dfs
  FROM {{ source('state_testing', 'state_testing_2324') }}
  WHERE assessmenttype = 'Summative' AND subject = 'Math'
),

joined_2324 AS (
  SELECT 
    COALESCE(e.studentidentifier, m.studentidentifier) AS studentidentifier,
    COALESCE(e.student_number, m.student_number) AS student_number,
    e.ela_scalescoreachievementlevel AS `24_ela_scalescoreachievementlevel`,
    e.ela_dfs AS `24_ela_dfs`,
    m.math_scalescoreachievementlevel AS `24_math_scalescoreachievementlevel`,
    m.math_dfs AS `24_math_dfs`,
    e.proficiency AS `24_proficiency`
  FROM ela_2324 e
  FULL OUTER JOIN math_2324 m
    ON e.studentidentifier = m.studentidentifier
),

stacked_state_testing AS (
  SELECT 
    jc.*,
    j24.24_ela_scalescoreachievementlevel,
    j24.24_ela_dfs,
    j24.24_math_scalescoreachievementlevel,
    j24.24_math_dfs,
    j24.24_proficiency
  FROM joined_continuous jc
  LEFT JOIN joined_2324 j24
    ON jc.studentidentifier = j24.studentidentifier
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
    english_teacher,
    math_teacher
  FROM {{ source('views', 'student_to_teacher') }}
  WHERE year = '24-25'
),

intervention_11 AS (
  SELECT 
    student_number,
    intervention
  FROM {{ source('powerschool', 'knight_time_intervention_grade_11_2025') }}
),

current_year AS (
  SELECT 
    sst.studentidentifier,
    sst.student_number,
    sst.ela_assessmentname,
    sst.ela_scalescoreachievementlevel,
    sst.ela_proficiency,
    sst.ela_dfs,
    sst.ela_scalescore,
    sst.ela_lexilemeasure,
    sst.`ela__reading`,
    sst.`ela__writing`,
    sst.`ela__listening`,
    sst.`ela__research_and_inquiry`,
    sst.math_assessmentname,
    sst.math_scalescoreachievementlevel,
    sst.math_proficiency,
    sst.math_dfs,
    sst.math_scalescore,
    CAST(sst.math_lexilemeasure AS FLOAT64) AS math_lexilemeasure,
    sst.`math__concepts_and_procedures`,
    sst.`math__problem_solving_and_modeling___data_analysis`,
    sst.`math__communicating_reasoning`,
    sst.24_ela_scalescoreachievementlevel,
    sst.24_ela_dfs,
    sst.24_math_scalescoreachievementlevel,
    sst.24_math_dfs,
    sst.24_proficiency,
    st.lastfirst,
    st.grade_level,
    st.elastatus,
    st.race,
    st.school_name,
    st.sped_identifier,
    st.absenteeism_status,
    st.english_teacher,
    st.math_teacher,
    CAST('24-25' AS STRING) AS year,
    i11.intervention
  FROM stacked_state_testing sst
  RIGHT JOIN student_to_teacher st
    ON sst.student_number = st.student_number
  LEFT JOIN intervention_11 i11
    ON st.student_number = i11.student_number
  WHERE st.grade_level IN (3, 4, 5, 6, 7, 8, 11)
),

historical AS (
  SELECT
    studentidentifier,
    student_number,
    ela_assessmentname,
    ela_scalescoreachievementlevel,
    ela_proficiency,
    ela_dfs,
    ela_scalescore,
    ela_lexilemeasure,
    `ela__reading`,
    `ela__writing`,
    `ela__listening`,
    `ela__research_and_inquiry`,
    math_assessmentname,
    math_scalescoreachievementlevel,
    math_proficiency,
    math_dfs,
    math_scalescore,
    CAST(math_lexilemeasure AS FLOAT64) AS math_lexilemeasure,
    `math__concepts_and_procedures`,
    `math__problem_solving_and_modeling___data_analysis`,
    `math__communicating_reasoning`,
    `24_ela_scalescoreachievementlevel`,
    `24_ela_dfs`,
    `24_math_scalescoreachievementlevel`,
    `24_math_dfs`,
    `24_proficiency`,
    lastfirst,
    grade_level,
    elastatus,
    race,
    school_name,
    sped_identifier,
    absenteeism_status,
    english_teacher,
    math_teacher,
    CAST('24-25' AS STRING) AS year,
    intervention
  FROM `icef-437920.dbt_historical.sbac_state_testing_tabular_24-25`
)

SELECT DISTINCT *
FROM (
  SELECT * FROM current_year
  UNION ALL
  SELECT * FROM historical
)

--1183 total row count before historical