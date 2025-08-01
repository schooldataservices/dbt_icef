{{ config(materialized='view', schema='ixl') }}

with source_data as (
    select 
        scores.local_student_id, 
        scores.skill_name,
        names.date_assigned,
        scores.score,
        scores.subject,
        scores.curriculum,
        CAST('24-25' AS STRING) AS year
    from {{ source('ixl', 'ixl_scores') }} scores
    join {{ source('ixl', 'math_skill_names') }} names
      on scores.skill_name = names.skill_name
     and scores.subject = names.subject

    union all

    select 
        local_student_id,
        skill_name,
        date_assigned,
        score,
        subject,
        curriculum,
        CAST('24-25' AS STRING) AS year
    from `icef-437920.dbt_historical.ixl_scores_math_24-25`
)

select distinct *
from source_data