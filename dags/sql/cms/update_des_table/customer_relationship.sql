TRUNCATE TABLE {{ params.des_schema_name }}.{{ params.des_table_name }}; 
INSERT INTO {{ params.des_schema_name }}.{{ params.des_table_name }} 
(
        ID,
        ACCNT_ID,
        SYSTEM_NAME,
        SYSTEM_CUSTOMER_ID,
        CRM_CODE,
        UPDATED_DATE,
        CREATED_DATE
)
    SELECT 
        src.ID,
        src.ACCNT_ID,
        src.SYSTEM_NAME,
        src.SYSTEM_CUSTOMER_ID,
        src.CRM_CODE,
        src.UPDATED_DATE,
        src.CREATED_DATE
    FROM staging.{{ params.des_schema_name }}_{{ params.des_table_name }} src ; 
