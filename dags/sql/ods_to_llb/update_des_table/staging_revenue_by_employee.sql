MERGE llb.revenue_by_employee AS target
USING llb.staging_revenue_by_employee AS source
ON target.tran_id = source.tran_id
   AND target.tran_date_id = source.tran_date_id
   AND ISNULL(target.created_by, '') = ISNULL(source.created_by, '')
   AND ISNULL(target.post_code, '') = ISNULL(source.post_code, '')
   AND ISNULL(target.contractid, '') = ISNULL(source.contractid, '')
   AND ISNULL(target.customerid, '') = ISNULL(source.customerid, '')
   AND ISNULL(target.tran_code, '') = ISNULL(source.tran_code, '')
   AND ISNULL(target.ma_spdv, '') = ISNULL(source.ma_spdv, '')
   AND ISNULL(target.ma_doanh_thu, '') = ISNULL(source.ma_doanh_thu, '')
   AND ISNULL(target.nhom_nghiep_vu, '') = ISNULL(source.nhom_nghiep_vu, '')
WHEN MATCHED THEN
    UPDATE SET
        revenue = source.revenue,
        sales = source.sales
WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        tran_date_id, created_by, post_code, contractid, customerid, tran_code, ma_spdv, ma_doanh_thu, nhom_nghiep_vu, revenue, sales, tran_id
    )
    VALUES (
        source.tran_date_id, source.created_by, source.post_code, source.contractid, source.customerid, source.tran_code, source.ma_spdv, source.ma_doanh_thu, source.nhom_nghiep_vu, source.revenue, source.sales, source.tran_id
    );
