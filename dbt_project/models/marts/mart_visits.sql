{{ config(
    materialized='incremental',
    engine='ReplacingMergeTree(updated_at)',
    incremental_strategy='append',
    unique_key='id',
    order_by='(event_month, id)',
    partition_by='event_month',
    on_schema_change='sync_all_columns'
) }}

SELECT 
    id,
    visit_date,
    event_month,
    age,
    gender_id,
    profession_id,
    doctor_id,
    department_id,
    snils,
    height,
    weight,
    bmi,
    temperature,
    bp_systolic,
    bp_diastolic,
    CAST(disease_code AS LowCardinality(String) AS disease_code),
    CAST(blood_type AS LowCardinality(String) AS blood_type),
    lab_hemoglobin,
    lab_leukocytes,
    lab_glucose,
    lab_cholesterol,
    symptoms_list,
    chronic_list,
    department_name,
    profession_name,
    created_at,
    updated_at
FROM {{ ref('stg_iceberg__visits') }}

{% if is_incremental() %}
  WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}