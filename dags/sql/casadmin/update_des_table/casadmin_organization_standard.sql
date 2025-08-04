TRUNCATE TABLE {{ params.des_schema }}.{{ params.des_table }}; 
INSERT INTO {{ params.des_schema }}.{{ params.des_table }} 
(
        unit_code,
        unit_name,
        unit_type,
        type_code,
        parent_code,
        commune_code,
        postal_code,
        post_type,
        address,
	status,
        tel,
        district_code,
        province_code,
        validate_from,
        validate_to
    )
   SELECT 
        src.unit_code,
        src.unit_name,
        src.unit_type,
        src.type_code,
        src.parent_code,
        src.commune_code,
        src.postal_code,
        src.post_type,
        src.address,
	src.status,
        src.tel,
        src.district_code,
        src.province_code,
        src.validate_from,
        src.validate_to
    FROM staging.{{ params.des_schema }}_{{ params.des_table }} src ; 
