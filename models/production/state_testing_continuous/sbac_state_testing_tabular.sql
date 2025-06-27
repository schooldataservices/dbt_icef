-- goes from multiple rows per assessmenttype and subject to one row per student based on ELA and Math, with assessmenttype summative
-- {{ config(materialized='view', schema='views') }}
-- {{ config(materialized='table', schema='views') }} --(one time)
{{ config(enabled=false) }}


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
),

intervention_11 AS (
  SELECT 
    student_number,
    intervention
  FROM {{ source('powerschool', 'knight_time_intervention_grade_11_2025') }}
)

SELECT 
  sst.studentidentifier,
  sst.student_number,

  -- ELA
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

  -- Math
  sst.math_assessmentname,
  sst.math_scalescoreachievementlevel,
  sst.math_proficiency,
  sst.math_dfs,
  sst.math_scalescore,
  sst.math_lexilemeasure,
  sst.`math__concepts_and_procedures`,
  sst.`math__problem_solving_and_modeling___data_analysis`,
  sst.`math__communicating_reasoning`,

  -- 2023–2024 indicators
  sst.24_ela_scalescoreachievementlevel,
  sst.24_ela_dfs,
  sst.24_math_scalescoreachievementlevel,
  sst.24_math_dfs,
  sst.24_proficiency,

  -- Student details
  st.lastfirst,
  st.grade_level,
  st.elastatus,
  st.race,
  st.school_name,
  st.sped_identifier,
  st.absenteeism_status,
  st.english_teacher,
  st.math_teacher,
  i11.intervention

FROM stacked_state_testing sst
RIGHT JOIN student_to_teacher st
  ON sst.student_number = st.student_number
LEFT JOIN intervention_11 i11
  ON st.student_number = i11.student_number
WHERE st.grade_level IN (3, 4, 5, 6, 7, 8, 11)

-- there are minimal records currently coming through for continuous because they are trickling through

-- have 9 students where the student_number is coming across as 0
-- aka they are not in the student_to_teacher table. 

--The population of the student_number generations in the student_to_teacher table stems 
-- from icef-437920.powerschool.vw_attendance_demographics_CA

-- Therefore if a student_number is missing it was inherently missing in the icef-437920.powerschool.vw_attendance_demographics_CA

