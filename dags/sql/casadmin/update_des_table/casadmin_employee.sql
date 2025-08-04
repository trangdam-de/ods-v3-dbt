TRUNCATE TABLE {{ params.des_schema }}.{{ params.des_table }} ;
INSERT INTO {{ params.des_schema }}.{{ params.des_table }}
(
        CODE,
        FULLNAME,
        DATE_OF_BIRTH,
        ID_CARD,
        EMAIL,
        ORG_CODE,
        PHONE_NUMBER,
        STATUS,
        ID_CARD_TYPE,
        POSITION_ID,
        POSITION_NAME,
        TITLE_ID,
        TITLE_NAME,
        GENDER,
        ADDRESS
)
    SELECT 
        src.CODE,
        src.FULLNAME,
        src.DATE_OF_BIRTH,
        src.ID_CARD,
        src.EMAIL,
        src.ORG_CODE,
        src.PHONE_NUMBER,
        src.STATUS,
        src.ID_CARD_TYPE,
        src.POSITION_ID,
        src.POSITION_NAME,
        src.TITLE_ID,
        src.TITLE_NAME,
        src.GENDER,
        src.ADDRESS
    FROM staging.{{ params.des_schema }}_{{ params.des_table }} src;
