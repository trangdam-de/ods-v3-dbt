MERGE INTO {{ params.des_schema }}.{{ params.des_table }} AS tgt
USING (
	SELECT unit_id,
		parent_id, 
		level_id, 
		unit_code, 
		unit_name, 
		unit_fullname, 
		parent_code, 
		province_id, 
		province_code, 
		created_date, 
		updated_date, 
		status
	FROM staging.{{ params.des_schema }}_{{ params.des_table }}
) AS src
ON tgt.unit_id=src.unit_id
WHEN MATCHED
THEN UPDATE SET
unit_id=src.unit_id, 
parent_id=src.parent_id, 
level_id=src.level_id, 
unit_code=src.unit_code, 
unit_name=src.unit_name, 
unit_fullname=src.unit_fullname, 
parent_code=src.parent_code, 
province_id=src.province_id, 
province_code=src.province_code, 
created_date=src.created_date, 
updated_date=src.updated_date, 
status=src.status
WHEN NOT MATCHED
THEN INSERT (unit_id, parent_id, level_id, unit_code, unit_name, unit_fullname, parent_code, province_id, province_code, created_date, updated_date, status)
VALUES (src.unit_id, src.parent_id, src.level_id, src.unit_code, src.unit_name, src.unit_fullname, src.parent_code, src.province_id, src.province_code, src.created_date, src.updated_date, src.status);
