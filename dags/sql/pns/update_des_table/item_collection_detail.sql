DELETE FROM {{ params.des_schema_name }}.{{ params.des_table_name }} des 
WHERE DATE(des.acceptance_date) = DATE('{{ dag_run.conf.get('start_time', ds + ' 00:00:00') }}'::TIMESTAMP);

INSERT INTO {{ params.des_schema_name }}.{{ params.des_table_name }}
SELECT * FROM staging.{{ params.des_schema_name }}_{{ params.des_table_name }} as src 
WHERE DATE(src.acceptance_date) = DATE('{{ dag_run.conf.get('start_time', ds + ' 00:00:00') }}'::TIMESTAMP);

-- DO $$
-- DECLARE
--     latest_etl_date DATE;
-- BEGIN
--     SELECT src.etl_date
--     INTO latest_etl_date -- todo: can phai thay cai nay bang ngay chay DAG
--     FROM staging.{{ params.des_schema_name }}_{{ params.des_table_name }} src
--     ORDER BY src.etl_date DESC
--     LIMIT 1;

--     DELETE FROM {{ params.des_schema_name }}.{{ params.des_table_name }} des WHERE des.etl_date = latest_etl_date;

--     INSERT INTO {{ params.des_schema_name }}.{{ params.des_table_name }}
--     SELECT * FROM staging.{{ params.des_schema_name }}_{{ params.des_table_name }} as src WHERE src.etl_date = latest_etl_date;
-- END $$;

-- MERGE INTO pns.item_collection_detail des
-- USING staging.item_collection_detail src
-- MERGE INTO {{ params.des_schema_name }}.{{ params.des_table_name }} des
-- USING staging.{{ params.des_schema_name }}_{{ params.des_table_name }} src
-- ON des.etl_date = src.etl_date
-- WHEN MATCHED THEN
--     UPDATE
--     SET lading_code             = src.lading_code,
--         postman_code            = src.postman_code,
--         service_code            = src.service_code,
--         service_collection_code = src.service_collection_code,
--         collection_route_code   = src.collection_route_code,
--         weight                  = src.weight,
--         transport_type          = src.transport_type,
--         collection_main         = src.collection_main,
--         acceptance_date         = src.acceptance_date,
--         contract_type           = src.contract_type,
--         hrm_code                = src.hrm_code,
--         district_code           = src.district_code,
--         province_code           = src.province_code
-- WHEN NOT MATCHED THEN
--     INSERT (lading_code,
--             postman_code,
--             service_code,
--             service_collection_code,
--             collection_route_code,
--             weight,
--             transport_type,
--             collection_main,
--             acceptance_date,
--             contract_type,
--             hrm_code,
--             district_code,
--             province_code,
--             etl_date)
--     VALUES (src.lading_code,
--             src.postman_code,
--             src.service_code,
--             src.service_collection_code,
--             src.collection_route_code,
--             src.weight,
--             src.transport_type,
--             src.collection_main,
--             src.acceptance_date,
--             src.contract_type,
--             src.hrm_code,
--             src.district_code,
--             src.province_code,
--             src.etl_date)

