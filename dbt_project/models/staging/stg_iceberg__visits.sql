{{ config(
    materialized='view'
) }}

SELECT 
    id,
    visit_date,
    age,
    gender_id,
    profession_id,
    doctor_id,
    department_id,
    snils,
    height,
    `weight`,
    bmi,
    temperature,
    bp_systolic,
    bp_diastolic,
    disease_code,
    blood_type,
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
FROM icebergS3(
    '{{ env_var("STORAGE") }}/{{ env_var("GOLD") }}gold/visits/', 
    '{{ env_var("ICE_ACCESS_KEY_ID") }}',                         
    '{{ env_var("ICE_SECRET_ACCESS_KEY") }}'
)
