SELECT tran_date_id, created_by, post_code, contractid, customerid, tran_code, 
       s98.ma_spdv, ma_doanh_thu, nhom_nghiep_vu,
       sum(
           coalesce(s98_cit_15, 0) +
           coalesce(s98_cit_17, 0) +
           coalesce(s98_cit_18, 0) +
           coalesce(s98_cit_20, 0) +
           coalesce(s98_cit_21, 0)
       ) AS revenue,
       sum(
           coalesce(s98_cit_15, 0) +
           coalesce(s98_cit_17, 0) +
           coalesce(s98_cit_18, 0) +   
           coalesce(s98_cit_20, 0) +
           coalesce(s98_cit_21, 0)
       ) AS sales,
       tran_id
FROM casreport.f_item_s98 s98
LEFT JOIN casreport.D_ROW_ITEM d ON s98.ROW_ITEM_CODE = d.ROW_ITEM_CODE 
WHERE tran_date_id >= '{{ params.start_time }}'
  AND tran_date_id < '{{ params.end_time }}' 
  AND ma_doanhthu_ps = '1' 
  AND ma_loaigd = '1' 
  AND tinhchat_gd = '1'
GROUP BY tran_date_id, created_by, post_code, contractid, customerid, tran_code, 
         s98.ma_spdv, ma_doanh_thu, nhom_nghiep_vu, tran_id
