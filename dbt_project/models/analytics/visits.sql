{{ config(
    materialized='incremental',
    engine='ReplacingMergeTree(updated_at)',
    incremental_strategy='append',
    unique_key='id',
    order_by='(event_date, id)'
) }}

SELECT 
    *,
    toStartOfDay(visit_date) AS event_date
FROM {{ ref('stg_iceberg__visits') }}

{% if is_incremental() %}
  WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}