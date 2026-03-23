{{ config(
    materialized='table',
    schema='enrollment'
) }}

-- Stacked union of historical completed-registration tables from dbt_historical → enrollment.completed_registrations.
-- `year` tags which school-year snapshot each row came from ('24-25' vs '25-26').

SELECT
  s.*,
  '24-25' AS year
FROM {{ source('dbt_historical', 'completed_registrations_2025') }} s

UNION ALL

SELECT
  s.*,
  '25-26' AS year
FROM {{ source('dbt_historical', 'completed_registrations_2026') }} s
