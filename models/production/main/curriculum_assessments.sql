{{ config(
    materialized='view',
    schema='views'
) }}

-- K-5 curriculum assessments (2.0 / Illuminate): Illuminate-only slice of main_elem.
-- Grade 0–5 is enforced in main_elem. Curriculum limited to ELA, Math, Science (case-insensitive).
-- standard_code limited to values containing "percent" (case-insensitive).
-- Excludes test_type checkpoint and exit ticket (case-insensitive); NULL test_type kept.

SELECT *
FROM {{ ref('main_elem') }}
WHERE LOWER(TRIM(data_source)) = 'illuminate'
  AND LOWER(TRIM(curriculum)) IN ('ela', 'math', 'science')
  AND LOWER(CAST(standard_code AS STRING)) LIKE '%percent%'
  AND COALESCE(LOWER(TRIM(CAST(test_type AS STRING))), '') NOT IN ('checkpoint', 'exit ticket')
