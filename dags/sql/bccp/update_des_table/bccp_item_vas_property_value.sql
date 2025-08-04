{% if dag_run.run_type == 'scheduled' %}
    {% set stg_table = "staging." ~ params.des_schema ~ "_" ~ params.des_table %}
{% else %}
    {% set stg_table = "staging." ~ params.des_schema ~ "_" ~ params.des_table ~ "_manual" %}
{% endif %}
MERGE INTO {{ params.des_schema }}.{{ params.des_table }} as des
USING {{ stg_table }} as src
ON des.item_code = src.item_code 
   AND des.property_code = src.property_code 
   AND des.value_added_service_code = src.value_added_service_code 
WHEN MATCHED AND src.last_updated_time > des.last_updated_time THEN
    UPDATE SET (
        value = src.value,
        create_time = src.create_time,
        last_updated_time = src.last_updated_time
    )
WHEN NOT MATCHED THEN
    INSERT (
        item_code,
        property_code,
        value,
        value_added_service_code,
        create_time,
        last_updated_time
    ) VALUES (
        src.item_code,
        src.property_code,
        src.value,
        src.value_added_service_code,
        src.create_time,
        src.last_updated_time
    );
