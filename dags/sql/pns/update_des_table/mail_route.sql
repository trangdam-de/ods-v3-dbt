-- MERGE INTO pns.mail_route des
-- USING staging.pns_mail_route src
MERGE INTO {{ params.des_schema_name }}.{{ params.des_table_name }} des
USING staging.{{ params.des_schema_name }}_{{ params.des_table_name }} src
ON des.mail_route_code = src.mail_route_code
WHEN MATCHED THEN
    UPDATE
    SET mail_route_name = src.mail_route_name,
        price = src.price,
        weight_avg = src.weight_avg,
        unit_id = src.unit_id
WHEN NOT MATCHED THEN
    INSERT (mail_route_code,
            mail_route_name,
            price,
            weight_avg,
            unit_id)
    VALUES (src.mail_route_code,
            src.mail_route_name,
            src.price,
            src.weight_avg,
            src.unit_id)
