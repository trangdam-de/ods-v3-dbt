TRUNCATE TABLE {{ params.des_schema }}.{{ params.des_table }};
INSERT INTO {{ params.des_schema }}.{{ params.des_table }}
    (
        CODE,
        NAME,
        PARENT_CODE,
        UNIT_TYPE,
        OLD_ADMINISTRATIVE_CODE,
        STATUS,
        MBC_CODE,
        COMMUNE,
        POD,
        HAN_TP_BCP,
        SORTINGCODE,
        IS_FAR,
        IS_ISLAND,
        IS_CENTER
    )
    SELECT
        src.CODE,
        src.NAME,
        src.PARENT_CODE,
        src.UNIT_TYPE,
        src.OLD_ADMINISTRATIVE_CODE,
        src.STATUS,
        src.MBC_CODE,
        src.COMMUNE,
        src.POD,
        src.HAN_TP_BCP,
        src.SORTINGCODE,
        src.IS_FAR,
        src.IS_ISLAND,
        src.IS_CENTER
    FROM staging.{{ params.des_schema }}_{{ params.des_table}} src 
