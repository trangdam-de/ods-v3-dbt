{% if dag_run.run_type == 'scheduled' %}
    {% set stg_table = "staging." ~ params.des_schema ~ "_" ~ params.des_table %}
{% else %}
    {% set stg_table = "staging." ~ params.des_schema ~ "_" ~ params.des_table ~ "_manual" %}
{% endif %}
MERGE INTO {{ params.des_schema }}.{{ params.des_table }} as des
USING {{ stg_table }} as src
on des.service_code = src.service_code
  and des.value_added_service_code = src.value_added_service_code
  and des.item_code = src.item_code
WHEN MATCHED AND des.last_updated_time < src.last_updated_time
    UPDATE SET (
      service_code = src.service_code,
      value_added_service_code = src.value_added_service_code,
      item_code = src.item_code,
      freight = src.freight,
      phase_code = src.phase_code,
      added_date = src.added_date,
      freight_vat = src.freight_vat,
      pos_code = src.pos_code,
      original_freight = src.original_freight,
      original_freight_vat = src.original_freight_vat,
      sub_freight = src.sub_freight,
      sub_freight_vat = src.sub_freight_vat,
      original_sub_freight = src.original_sub_freight,
      original_sub_freight_vat = src.original_sub_freight_vat,
      create_time = src.create_time,
      last_updated_time = src.last_updated_time
    ) 
WHEN NOT MATCHED THEN
    INSERT (
      service_code,
      value_added_service_code,
      item_code,
      freight,
      phase_code,
      added_date,
      freight_vat,
      pos_code,
      original_freight,
      original_freight_vat,
      sub_freight,
      sub_freight_vat,
      original_sub_freight,
      original_sub_freight_vat,
      create_time,
      last_updated_time
    ) VALUES (
      src.service_code,
      src.value_added_service_code,
      src.item_code,
      src.freight,
      src.phase_code,
      src.added_date,
      src.freight_vat,
      src.pos_code,
      src.original_freight,
      src.original_freight_vat,
      src.sub_freight,
      src.sub_freight_vat,
      src.original_sub_freight,
      src.original_sub_freight_vat,
      src.create_time,
      src.last_updated_time
    )