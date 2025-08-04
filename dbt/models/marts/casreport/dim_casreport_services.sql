-- Final dimension table for CASREPORT services
-- This replaces the MERGE logic in casreport_v_prd_srv.sql

{{ config(
    materialized='incremental',
    unique_key='prd_srv_code',
    schema='casreport',
    on_schema_change='sync_all_columns'
) }}

WITH staging_data AS (
    SELECT * FROM {{ ref('stg_casreport__v_prd_srv') }}
)

SELECT
    prd_srv_code,
    prd_srv_name,
    prd_srv_group_code,
    prd_srv_group_name,
    line,
    prd_srv_code_name,
    dbt_updated_at
FROM staging_data

{% if is_incremental() %}
    -- Only process new or updated records
    WHERE dbt_updated_at > (SELECT COALESCE(MAX(dbt_updated_at), '1900-01-01') FROM {{ this }})
{% endif %}
