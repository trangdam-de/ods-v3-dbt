DECLARE @StartTime DATETIME = CONVERT(DATETIME,'{{ params.start_time }}', 120);
DECLARE @EndTime DATETIME = CONVERT(DATETIME,'{{ params.end_time }}', 120);

SELECT ItemCode,
        PropertyCode,
        Value,
        ValueAddedServiceCode,
        CreateTime,
        LastUpdatedTime
FROM {{ params.src_schema }}.{{ params.src_table }}
WHERE Item_code in (
          SELECT Item_code
          FROM {{ params.src_schema }}.Item
          WHERE LastUpdatedTime >= @StartTime
  AND LastUpdatedTime < @EndTime);