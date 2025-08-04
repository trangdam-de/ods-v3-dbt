-- MERGE INTO pns.mail_route des
-- USING staging.pns_mail_route src
MERGE INTO {{ params.des_schema_name }}.{{ params.des_table_name }} des
USING staging.{{ params.des_schema_name }}_{{ params.des_table_name }} src
ON des.pos_send = src.pos_send AND des.pos_receive = src.pos_receive
WHEN MATCHED THEN
    UPDATE
    SET distance = src.distance
WHEN NOT MATCHED THEN
    INSERT (pos_send,
            pos_receive,
            distance)
    VALUES (src.pos_send,
            src.pos_receive,
            src.distance)
