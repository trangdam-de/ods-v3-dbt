{% if dag_run.run_type == 'scheduled' %}
    {% set stg_table = "staging." ~ params.des_schema ~ "_" ~ params.des_table %}
{% else %}
    {% set stg_table = "staging." ~ params.des_schema ~ "_" ~ params.des_table ~ "_manual" %}
{% endif %}
MERGE INTO {{ params.des_schema }}.{{ params.des_table }} as des
USING {{ stg_table }} as src
ON des.type = src.type and des.status = src.status
WHEN MATCHED THEN
    UPDATE SET 
          status_name = src.status_name,
          status_name_en = src.status_name_en,
          stage = src.stage
WHEN NOT MATCHED THEN
    INSERT (
          type,
          status,
          status_name,
          status_name_en,
          stage
    ) VALUES(
          src.type,
          src.status,
          src.status_name,
          src.status_name_en,
          src.stage
    )
    
