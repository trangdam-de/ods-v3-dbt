TRUNCATE TABLE {{ params.des_schema }}.{{ params.des_table }};

INSERT INTO {{ params.des_schema }}.{{ params.des_table }} 
(
  row_item_code, row_item_name, nhom_chitieu, ma_spdv, ten_spdv, ma_nhom_spdv, ten_nhom_spdv,
  nuoc_phattra, pv_pv, dv_gtgt, is_gtg021, is_gtg038, is_gtg039, ngay_hl, ngay_kt
)
SELECT
  row_item_code, row_item_name, nhom_chitieu, ma_spdv, ten_spdv, ma_nhom_spdv, ten_nhom_spdv,
  nuoc_phattra, pv_pv, dv_gtgt, is_gtg021, is_gtg038, is_gtg039, 
  CASE
    WHEN EXTRACT(YEAR FROM ngay_hl) = 2261 THEN TO_DATE('9999-12-31', 'YYYY-MM-DD')
    ELSE ngay_hl
  END AS ngay_hl,
  CASE
    WHEN EXTRACT(YEAR FROM ngay_kt) = 2261 THEN TO_DATE('9999-12-31', 'YYYY-MM-DD')
    ELSE ngay_kt
  END AS ngay_kt
FROM staging.{{ params.des_schema }}_{{ params.des_table }}
