SELECT
    cst.ID,
    cst.CONTRACT_ID,
    cst.CONTRACT_SERVICE,
    cst.CONTRACT_CLASSIFY,
    cst.CONTRACT_SERVICE_CODE,
    cst.C_PPA_NUMBER,
    cst.RELEASE_NOTE
FROM {{ params.src_schema_name }}.{{ params.src_table_name }} cst 
LEFT JOIN {{ params.src_schema_name }}.contracts c on cst.CONTRACT_ID = c.CONTRACT_ID
-- FETCH FIRST 300000 ROWS ONLY
-- WHERE CREATED_DATE >= TO_DATE('2024-12-01', 'YYYY-MM-DD')
-- ORDER BY CREATED_DATE DESC
