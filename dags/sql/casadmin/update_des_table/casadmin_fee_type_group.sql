TRUNCATE TABLE {{ params.des_schema }}.{{ params.des_table }} ;
INSERT INTO {{ params.des_schema }}.{{ params.des_table }}
(
          FEE_TYPE_GROUP_ID,
          FEE_TYPE_GROUP_NAME,
          CREATED,
          CREATEDBY,
          UPDATED,
          UPDATEDBY,
          STATUS
          )
SELECT 
          src.FEE_TYPE_GROUP_ID,
          src.FEE_TYPE_GROUP_NAME,
          src.CREATED,
          src.CREATEDBY,
          src.UPDATED,
          src.UPDATEDBY,
          src.STATUS
          FROM staging.{{ params.des_schema }}_{{ params.des_table }} src;
