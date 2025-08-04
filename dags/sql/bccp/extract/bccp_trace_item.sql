DECLARE @StartTime DATETIME = CONVERT(DATETIME,'{{ params.start_time }}', 120);
DECLARE @EndTime DATETIME = CONVERT(DATETIME,'{{ params.end_time }}', 120);

SELECT TraceIndex ,
      ItemCode ,
      POSCode ,
      Status ,
      TraceDate ,
      StatusDesc ,
      Note ,
      TransferMachine ,
      TransferUser ,
      TransferPOSCode ,
      TransferDate ,
      TransferStatus ,
      TransferTimes ,
      CreateTime ,
      LastUpdatedTime
FROM {{ params.src_schema }}.{{ params.src_table }}
WHERE LastUpdatedTime >= @StartTime
  AND LastUpdatedTime < @EndTime;
