-- MERGE INTO pns.mail_route des
-- USING staging.pns_mail_route src
MERGE INTO {{ params.des_schema_name }}.{{ params.des_table_name }} des
USING staging.{{ params.des_schema_name }}_{{ params.des_table_name }} src
ON des.route_code = src.route_code AND des.unit_code = src.unit_code
WHEN MATCHED THEN
    UPDATE
    SET route_name = src.route_name,
        status = src.status,
        route_difficult = src.route_difficult,
        unit_id = src.unit_id,
        unit_name = src.unit_name,
        route_kind_name = src.route_kind_name,
        route_kind_code = src.route_kind_code,
        route_type_code = src.route_type_code,
        route_type_name = src.route_type_name
WHEN NOT MATCHED THEN
    INSERT (route_code,
            route_name,
            status,
            route_difficult,
            unit_id,
            unit_code,
            unit_name,
            route_kind_name,
            route_kind_code,
            route_type_code,
            route_type_name)
    VALUES (src.route_code,
            src.route_name,
            src.status,
            src.route_difficult,
            src.unit_id,
            src.unit_code,
            src.unit_name,
            src.route_kind_name,
            src.route_kind_code,
            src.route_type_code,
            src.route_type_name)
