TRUNCATE TABLE {{ params.des_schema }}.{{ params.des_table }};
INSERT INTO {{ params.des_schema }}.{{ params.des_table }}
(
        CODE,
        NAME,
        CREATED_DATE,
        STATUS,
        CONT,
        ENAME,
        REGION
    )
	SELECT   
        src.CODE,
        src.NAME,
        src.CREATED_DATE,
        src.STATUS,
        src.CONT,
        src.ENAME,
        src.REGION
    FROM staging.{{ params.des_schema }}_{{ params.des_table }} src;
