{{ config(materialized='view', schema='views') }}

WITH MaxPartition AS (
  SELECT MAX(S.partitiontime) AS max_partitiontime
  FROM {{ source('powerschool', 'pq_studentRosters') }} S
)

SELECT
  S.student_number,
  S.expression,
  S.course_name,
  T.first_name,
  T.last_name,
  CASE 
    WHEN T.last_name IN ('Jones', 'Turney') THEN 'Algebra 1'
    WHEN T.first_name IN ('Math-Teacher-2-VPHS', 'Science-Teach-1-VPHS') THEN 'Geometry'
    WHEN T.first_name IN ('Math-Teacher-3-VPHS', 'Lennar') THEN 'Algebra 2'
    WHEN T.last_name IN ('Estrada', 'Jameison') THEN 'English 9'
    WHEN T.last_name IN ('Dobbs', 'Fowler') THEN 'English 10'
    WHEN T.last_name IN ('Price', 'McCutcheon') THEN 'English 11'
  END AS subject,
  CURRENT_TIMESTAMP AS last_run_time  -- Adding a dummy column to force changes
FROM {{ source('powerschool', 'pq_studentRosters') }} S
JOIN `icef-437920.powerschool.pq_StaffDemos` T
  ON S.teacherid = T.schoolstaff_id
  AND S.partitiontime = T.partitiontime
JOIN (
  SELECT MAX(S.partitiontime) AS max_partitiontime
  FROM {{ source('powerschool', 'pq_studentRosters') }} S
) MP
  ON S.partitiontime = MP.max_partitiontime
WHERE S.course_name LIKE '%Academic Enrichment%'
  AND S.schoolid LIKE '543'
  AND S.roleid LIKE '41'
  AND CURRENT_TIMESTAMP BETWEEN TIMESTAMP(S.teacher_startdate) AND TIMESTAMP(S.teacher_enddate)
  AND T.last_name IN ('Estrada', 'Dobbs', 'Fowler', 'Jameison', 'Jones', 'Madlansacay', 'McCutcheon', 'Placeholder', 'Price', 'Turney')
