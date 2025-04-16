{% test state_testing_continuous_schoolname_values(model) %}
WITH required_values AS (
    SELECT 'ICEF View Park Preparatory Elementary' AS schoolname UNION ALL
    SELECT 'ICEF View Park Preparatory High' UNION ALL
    SELECT 'ICEF Vista Elementary Academy' UNION ALL
    SELECT 'ICEF Innovation Los Angeles Charter' UNION ALL
    SELECT 'ICEF View Park Preparatory Middle' UNION ALL
    SELECT 'ICEF Vista Middle Academy' UNION ALL
    SELECT 'ICEF Inglewood Elementary Charter Academy'
),

existing_values AS (
    SELECT DISTINCT schoolname
    FROM {{ model }}
)

SELECT rv.schoolname
FROM required_values rv
LEFT JOIN existing_values ev
  ON rv.schoolname = ev.schoolname
WHERE ev.schoolname IS NULL
{% endtest %}