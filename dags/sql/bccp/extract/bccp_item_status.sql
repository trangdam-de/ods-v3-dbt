Select Type,
      Status,
      StatusName,
      StatusNameEn,
      Stage
FROM {{ params.src_schema }}.{{ params.src_table }}