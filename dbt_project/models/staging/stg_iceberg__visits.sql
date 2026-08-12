{{ config(
    materialized='view'
) }}

SELECT 
    *,
    toStartOfMonth(visit_date) AS event_month
FROM icebergS3(
    '{{ env_var("STORAGE") }}/{{ env_var("GOLD") }}gold/visits/', 
    '{{ env_var("ICE_ACCESS_KEY_ID") }}',
    '{{ env_var("ICE_SECRET_ACCESS_KEY") }}'
)
