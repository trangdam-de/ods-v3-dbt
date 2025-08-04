{% if dag_run.run_type == 'scheduled' %}
    {% set stg_table = "staging." ~ params.des_schema ~ "_" ~ params.des_table %}
{% else %}
    {% set stg_table = "staging." ~ params.des_schema ~ "_" ~ params.des_table ~ "_manual" %}
{% endif %}
MERGE INTO {{ params.des_schema }}.{{ params.des_table }} as des
USING 
(
    SELECT
        trace_index,
        coalesce(item_code, '') AS item_code,
        coalesce(pos_code, '') AS pos_code,
        status,
        trace_date,
        status_desc,
        note,
        transfer_machine,
        transfer_user,
        transfer_pos_code,
        transfer_date,
        CASE WHEN transfer_status THEN B'1' ELSE B'0' END AS transfer_status,
        transfer_times,
        create_time,
        last_updated_time
    FROM {{ stg_table }}
   -- WHERE status in (1,2,3,4,5,6,7,8,9,10,12,13,14,16,18,23,34,38,69,70,71,72,73,74,75,76,77,78,79)
) as src
ON des.trace_index = src.trace_index and des.item_code = src.item_code and des.pos_code = src.pos_code
WHEN MATCHED and src.last_updated_time > des.last_updated_time THEN
    UPDATE SET 
            trace_index = src.trace_index,
            item_code = src.item_code,
            pos_code = src.pos_code,
            status = src.status,
            trace_date = src.trace_date,
            status_desc = src.status_desc,
            note = src.note,
            transfer_machine = src.transfer_machine,
            transfer_user = src.transfer_user,
            transfer_pos_code = src.transfer_pos_code,
            transfer_date = src.transfer_date,
            transfer_status = src.transfer_status,
            transfer_times = src.transfer_times,
            create_time = src.create_time,
            last_updated_time = src.last_updated_time
WHEN NOT MATCHED THEN
    INSERT (
            trace_index,
            item_code,
            pos_code,
            status,
            trace_date,
            status_desc,
            note,
            transfer_machine,
            transfer_user,
            transfer_pos_code,
            transfer_date,
            transfer_status,
            transfer_times,
            create_time,
            last_updated_time
    )VALUES(
            src.trace_index,
            src.item_code,
            src.pos_code,
            src.status,
            src.trace_date,
            src.status_desc,
            src.note,
            src.transfer_machine,
            src.transfer_user,
            src.transfer_pos_code,
            src.transfer_date,
            src.transfer_status,
            src.transfer_times,
            src.create_time,
            src.last_updated_time
    )

