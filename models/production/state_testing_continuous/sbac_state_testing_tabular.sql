-- goes from multiple rows per assessmenttype and subject to one row per student based on ELA and Math, with assessmenttype summative
{{ config(materialized='view', schema='views') }}

WITH ela_frame AS (
  SELECT DISTINCT  
    studentidentifier, 
    student_number,
    assessmentname AS ela_assessmentname,
    scalescoreachievementlevel AS ela_scalescoreachievementlevel,
    proficiency AS ela_proficiency,
    dfs_ela AS ela_dfs,
    scalescore AS ela_scalescore,
    lexilemeasure AS ela_lexilemeasure,
    `ela:_reading`,
    `ela:_writing`,
    `ela:_listening`,
    `ela:_research_and_inquiry`
  FROM {{ source('state_testing', 'state_testing_2324') }}
  WHERE assessmenttype = 'Summative'
    AND subject = 'ELA'
), 

math_frame AS (
  SELECT DISTINCT  
    studentidentifier, 
    student_number,
    assessmentname AS math_assessmentname,
    scalescoreachievementlevel AS math_scalescoreachievementlevel,
    proficiency AS math_proficiency,
    dfs_math AS math_dfs,
    scalescore AS math_scalescore,
    lexilemeasure AS math_lexilemeasure,
    `math:_concepts_and_procedures`,
    `math:_problem_solving_and_modeling_&_data_analysis`,
    `math:_communicating_reasoning`
  FROM {{ source('state_testing', 'state_testing_2324') }}
  WHERE assessmenttype = 'Summative'
    AND subject = 'Math'
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
)

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
  e.`ela:_reading`,
  e.`ela:_writing`,
  e.`ela:_listening`,
  e.`ela:_research_and_inquiry`,

  -- Math columns
  m.math_assessmentname,
  m.math_scalescoreachievementlevel,
  m.math_proficiency,
  m.math_dfs,
  m.math_scalescore,
  m.math_lexilemeasure,
  m.`math:_concepts_and_procedures`,
  m.`math:_problem_solving_and_modeling_&_data_analysis`,
  m.`math:_communicating_reasoning`,

  -- Student-to-teacher columns
  s.lastfirst,
  s.grade_level,
  s.elastatus,
  s.race,
  s.school_name,
  s.sped_identifier,
  s.absenteeism_status,
  s.english_teacher,
  s.math_teacher

FROM ela_frame e
LEFT JOIN math_frame m
  ON e.studentidentifier = m.studentidentifier
LEFT JOIN student_to_teacher s
  ON COALESCE(e.student_number, m.student_number) = s.student_number

  
-- have 9 students where the student_number is coming across as 0
-- aka they are not in the student_to_teacher table. 

--The population of the student_number generations in the student_to_teacher table stems 
-- from icef-437920.powerschool.vw_attendance_demographics_CA

-- Therefore if a student_number is missing it was inherently missing in the icef-437920.powerschool.vw_attendance_demographics_CA