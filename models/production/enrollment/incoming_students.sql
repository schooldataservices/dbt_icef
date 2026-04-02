{{ config(
    materialized='view',
    schema='enrollment'
) }}

-- Stacked union of historical incoming-students tables from dbt_historical → enrollment.incoming_students.
-- `year` tags which school-year snapshot each row came from ('24-25' vs '25-26').
-- Source names are defined in enrollment/schema.yml.

SELECT
  s.*,
  '24-25' AS year
FROM {{ source('dbt_historical', 'incoming_students_2025') }} s

UNION ALL

SELECT
  s.*,
  '25-26' AS year
FROM {{ source('dbt_historical', 'incoming_students_2026') }} s
