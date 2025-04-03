{{ config(materialized='view', schema='ixl') }}

with source_data as (
    select 
        scores.local_student_id, 
        scores.skill_name,
        names.date_assigned,
        scores.score,
        scores.subject,
        scores.curriculum
    from {{ source('ixl', 'ixl_scores') }} scores
    join {{ source('ixl', 'math_skill_names') }} names
    on scores.skill_name = names.skill_name
    and scores.subject = names.subject
)

select *
from source_data

-- put in dbt cloud, and then implement to airflow setup