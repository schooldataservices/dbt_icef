{{ config(materialized='view', schema='pear') }}

WITH ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY assessment_id, user_id, teacher_username
      ORDER BY 
        CASE WHEN status = 'GRADED' THEN 1 ELSE 0 END DESC,
        submitted_date DESC
    ) AS rn
  FROM {{ source('pear', 'pear_daily_updates') }}
  WHERE assignment_status = 'DONE'
)

SELECT *
FROM ranked
WHERE rn = 1


-- One row per student–assessment–teacher
-- Preference for graded work
-- Only completed (“DONE”) assignments
-- keeps only one record per student per assessment per teacher, prioritizing graded and latest submissions.