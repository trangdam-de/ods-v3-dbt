MERGE INTO {{ params.des_schema }}.{{ params.des_table }} as des
USING staging.{{ params.des_schema }}_{{ params.des_table }} as src
ON (des.username = src.username)
    WHEN MATCHED THEN
    UPDATE SET 
        password = src.password,
        description = src.description,
        employee_code = src.employee_code,
        last_change_pass_dt = src.last_change_pass_dt,
        date_create = src.date_create,
        alternative_password = src.alternative_password,
        status = src.status,
        org_code = src.org_code
WHEN NOT MATCHED THEN
    INSERT (
        username,
        password,
        description,
        employee_code,
        last_change_pass_dt,
        date_create,
        alternative_password,
        status,
        org_code
    )
    VALUES (
        src.username,
        src.password,
        src.description,
        src.employee_code,
        src.last_change_pass_dt,
        src.date_create,
        src.alternative_password,
        src.status,
        src.org_code
    );
