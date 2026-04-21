{{ config(
    materialized='view',
    schema='enrollment'
) }}

-- Daily-refreshed replica of dbt_historical.application_pipeline in enrollment dataset.
SELECT *
FROM {{ source('dbt_historical', 'application_pipeline') }}
