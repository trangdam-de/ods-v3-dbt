SELECT 
UPDATED_DATE,
CREATED_DATE,
ID,
ACCNT_ID,
SYSTEM_NAME,
SYSTEM_CUSTOMER_ID,
CRM_CODE
FROM {{ params.src_schema_name }}.{{ params.src_table_name }}
