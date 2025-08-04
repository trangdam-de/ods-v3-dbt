DELETE FROM {{ params.des_schema }}.{{ params.des_table }}
WHERE update_date >= TO_TIMESTAMP('{{ ti.xcom_pull(task_ids="one_to_one_tasks.casreport_mailsadditionaldata.source_to_staging_casreport_mailsadditionaldata", key="start_time") }}', 'YYYY-MM-DD HH24:MI:SS')
  AND update_date < TO_TIMESTAMP('{{ ti.xcom_pull(task_ids="one_to_one_tasks.casreport_mailsadditionaldata.source_to_staging_casreport_mailsadditionaldata", key="end_time") }}', 'YYYY-MM-DD HH24:MI:SS');
  
INSERT INTO {{ params.des_schema }}.{{ params.des_table }} (
    mailstxnid,
    detailitemname,
    detaiiitemnameeng,
    hscode,
    quantity,
    unit,
    weight,
    priceweight,
    unitpricevnd,
    unitpricefccy,
    pricevnd,
    pricefccy,
    originalcountrycode,
    note,
    packageno,
    packagecode,
    datetime,
    length,
    width,
    height,
    currencyunit,
    update_date
)
SELECT
    mailstxnid,
    detailitemname,
    detaiiitemnameeng,
    hscode,
    quantity,
    unit,
    weight,
    priceweight,
    unitpricevnd,
    unitpricefccy,
    pricevnd,
    pricefccy,
    originalcountrycode,
    note,
    packageno,
    packagecode,
    datetime,
    length,
    width,
    height,
    currencyunit,
    update_date
FROM staging.{{ params.des_schema }}_{{ params.des_table }};
