-- Staging model for CASREPORT service products
-- This replaces the current casreport_v_prd_srv.sql logic

{{ config(
    materialized='view',
    schema='staging'
) }}

WITH source_data AS (
    SELECT * FROM {{ source('staging', 'casreport_v_prd_srv') }}
),

cleaned_data AS (
    SELECT
        TRIM(prd_srv_code) AS prd_srv_code,
        TRIM(prd_srv_name) AS prd_srv_name,
        TRIM(prd_srv_group_code) AS prd_srv_group_code,
        TRIM(prd_srv_group_name) AS prd_srv_group_name,
        TRIM(line) AS line,
        TRIM(prd_srv_code_name) AS prd_srv_code_name,
        CURRENT_TIMESTAMP AS dbt_updated_at
    FROM source_data
    WHERE prd_srv_code IS NOT NULL
)

SELECT * FROM cleaned_data
