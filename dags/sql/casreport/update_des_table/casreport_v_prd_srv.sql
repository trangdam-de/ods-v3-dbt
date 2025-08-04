TRUNCATE TABLE {{ params.des_schema }}.{{ params.des_table}} ; 
MERGE INTO {{ params.des_schema }}.{{ params.des_table }} as des
USING staging.{{ params.des_schema }}_{{ params.des_table }} as src
ON (des.prd_srv_code = src.prd_srv_code)
    WHEN MATCHED THEN
    UPDATE SET 
        prd_srv_name = src.prd_srv_name,
        prd_srv_group_code = src.prd_srv_group_code,
        prd_srv_group_name = src.prd_srv_group_name,
        line = src.line,
        prd_srv_code_name = src.prd_srv_code_name
WHEN NOT MATCHED THEN
    INSERT (
        prd_srv_code,
        prd_srv_name,
        prd_srv_group_code,
        prd_srv_group_name,
        line,
        prd_srv_code_name
    )
    VALUES (
        src.prd_srv_code,
        src.prd_srv_name,
        src.prd_srv_group_code,
        src.prd_srv_group_name,
        src.line,
        src.prd_srv_code_name
    );
