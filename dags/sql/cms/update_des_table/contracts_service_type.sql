MERGE INTO {{ params.des_schema_name }}.{{ params.des_table_name }} des
USING staging.{{ params.des_schema_name }}_{{ params.des_table_name }} src
ON des.ID = src.ID
WHEN MATCHED THEN
    UPDATE SET
        CONTRACT_ID = src.CONTRACT_ID,
        CONTRACT_SERVICE = src.CONTRACT_SERVICE,
        CONTRACT_CLASSIFY = src.CONTRACT_CLASSIFY,
        CONTRACT_SERVICE_CODE = src.CONTRACT_SERVICE_CODE,
        C_PPA_NUMBER = src.C_PPA_NUMBER,
        RELEASE_NOTE = src.RELEASE_NOTE
WHEN NOT MATCHED THEN
    INSERT (
        ID,
        CONTRACT_ID,
        CONTRACT_SERVICE,
        CONTRACT_CLASSIFY,
        CONTRACT_SERVICE_CODE,
        C_PPA_NUMBER,
        RELEASE_NOTE
    )
    VALUES (
        src.ID,
        src.CONTRACT_ID,
        src.CONTRACT_SERVICE,
        src.CONTRACT_CLASSIFY,
        src.CONTRACT_SERVICE_CODE,
        src.C_PPA_NUMBER,
        src.RELEASE_NOTE
    );
