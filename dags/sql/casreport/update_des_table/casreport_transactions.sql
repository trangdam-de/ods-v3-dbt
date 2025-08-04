MERGE INTO {{ params.des_schema }}.{{ params.des_table }} as des
USING staging.{{ params.des_schema }}_{{ params.des_table }} as src
ON (des.groupid = src.groupid AND des.nodeid = src.nodeid AND des.num= src.num)
    WHEN MATCHED THEN
    UPDATE SET 
	txnid = src.txnid,
	sessionid = src.sessionid,
	txntype = src.txntype,
        createdby = src.createdby,
        createddate = src.createddate,
        createddate_id = src.createddate_id,
        update_date = src.update_date
WHEN NOT MATCHED THEN
    INSERT (
        txnid,
        sessionid,
        txntype,
        groupid,
        nodeid,
        num,
        createdby,
        createddate,
        createddate_id,
        update_date
    )
    VALUES (
        src.txnid,
        src.sessionid,
        src.txntype,
        src.groupid,
        src.nodeid,
        src.num,
        src.createdby,
        src.createddate,
        src.createddate_id,
        src.update_date
    );
