SELECT *
FROM {{ source('state_testing', 'state_testing_continuous') }}