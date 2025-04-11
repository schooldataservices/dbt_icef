{{ config(materialized='view', schema='views') }}

WITH MaxPartition AS (
  SELECT MAX(S.partitiontime) AS max_partitiontime
  FROM {{ source('powerschool_grade_auditing', 'pq_studentRosters') }} S
),

course_category AS (
  SELECT *
  FROM {{ source('powerschool_grade_auditing', 'stored_grades_course_category') }}
),

output AS (
  SELECT 
    sg.*,
    cc.category
  FROM {{ source('powerschool_grade_auditing', 'pq_storedgrades') }} sg
  JOIN MaxPartition mp
    ON sg.partitiontime = mp.max_partitiontime
  JOIN course_category cc
    ON cc.course_name = sg.course_name
    AND CAST(cc.grade_level AS STRING) = CAST(sg.grade_level AS STRING)
),

teacher_name_map AS (
  SELECT 
    CONCAT(last_name, ', ', first_name) AS placeholders,
    teacher_name AS clean_name
  FROM {{ source('powerschool_grade_auditing', 'placeholder_real_names') }}
),

teachers_replaced AS (
  SELECT 
    COALESCE(tnm.clean_name, o.teacher_name) AS teacher_name,
    o.* EXCEPT(teacher_name),
    -- Normalized grade logic applied inline
    CASE
      WHEN o.grade IN ('0', '1', '2', '3', '4') THEN
        CASE o.grade
          WHEN '0' THEN 'F'
          WHEN '1' THEN 'D'
          WHEN '2' THEN 'C'
          WHEN '3' THEN 'B'
          WHEN '4' THEN 'A'
        END
      ELSE REGEXP_REPLACE(o.grade, r'([A-F])[+-]', r'\1')
    END AS normalized_grade
  FROM output o
  LEFT JOIN teacher_name_map tnm
    ON o.teacher_name = tnm.placeholders
)

SELECT *
FROM teachers_replaced