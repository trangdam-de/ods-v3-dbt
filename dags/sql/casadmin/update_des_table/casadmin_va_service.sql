TRUNCATE TABLE {{ params.des_schema }}.{{ params.des_table }} ;
INSERT INTO {{ params.des_schema }}.{{ params.des_table }} 
 (
        va_service_id,
        va_service_name,
        fee_type_group_id,
        created,
        createdby,
        updated,
        updatedby,
        status,
        "case",
        symbol,
        incident,
        tax_declaration_code,
        tax_rate_code
    )
    SELECT 
        src.va_service_id,
        src.va_service_name,
        src.fee_type_group_id,
        src.created,
        src.createdby,
        src.updated,
        src.updatedby,
        src.status,
        src."case",
        src.symbol,
        src.incident,
        src.tax_declaration_code,
        src.tax_rate_code
    FROM staging.{{ params.des_schema }}_{{ params.des_table }} src ;
