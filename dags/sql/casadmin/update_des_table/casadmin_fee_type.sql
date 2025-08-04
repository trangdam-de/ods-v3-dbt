TRUNCATE TABLE {{ params.des_schema }}.{{ params.des_table }} ;
INSERT INTO {{ params.des_schema }}.{{ params.des_table }} 
(
          fee_type_id,
          fee_type_name,
          fee_type_code,
          fee_type_group_id,
          created,
          createdby,
          updated,
          updatedby,
          status
)
    SELECT 
          src.fee_type_id,
          src.fee_type_name,
          src.fee_type_code,
          src.fee_type_group_id,
          src.created,
          src.createdby,
          src.updated,
          src.updatedby,
          src.status
          FROM staging.{{ params.des_schema }}_{{ params.des_table}} src ;
