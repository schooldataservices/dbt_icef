{{ config(
    materialized='view',
    schema='enrollment'
) }}

-- Stacked union of historical application-pipeline tables from dbt_historical → enrollment.application_pipeline.
-- `year` tags which school-year snapshot each row came from ('24-25' vs '25-26').

SELECT
  s.*,
  '24-25' AS year
FROM {{ source('dbt_historical', 'application_pipeline_2025') }} s

UNION ALL

SELECT
  s.*,
  '25-26' AS year
FROM {{ source('dbt_historical', 'application_pipeline_2026') }} s
