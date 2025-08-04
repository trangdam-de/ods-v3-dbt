MERGE INTO {{ params.des_schema }}.{{ params.des_table }} as des
USING staging.{{ params.des_schema }}_{{ params.des_table }} as src
ON (des.groupid = src.groupid AND des.nodeid = src.nodeid AND des.num = src.num)
    WHEN MATCHED THEN
    UPDATE SET 
	txnid = src.txnid,
	sessionid = src.sessionid,
        entrytype = src.entrytype,
        txntype = src.txntype,
        account = src.account,
        amount = src.amount,
        container = src.container,
        createdby = src.createdby,
        createddate = src.createddate,
        poscode = src.poscode,
        created_date_id = src.created_date_id,
        poscode_bdt = src.poscode_bdt,
        update_date = src.update_date
WHEN NOT MATCHED THEN
    INSERT (
        txnid,
        sessionid,
        entrytype,
        txntype,
        account,
        amount,
        container,
        nodeid,
        num,
        createdby,
        createddate,
        groupid,
        poscode,
        created_date_id,
        poscode_bdt,
        update_date
    )
    VALUES (
        src.txnid,
        src.sessionid,
        src.entrytype,
        src.txntype,
        src.account,
        src.amount,
        src.container,
        src.nodeid,
        src.num,
        src.createdby,
        src.createddate,
        src.groupid,
        src.poscode,
        src.created_date_id,
        src.poscode_bdt,
        src.update_date
    );
