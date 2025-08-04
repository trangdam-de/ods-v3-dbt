DECLARE @StartTime DATETIME = CONVERT(DATETIME,'{{ params.start_time }}', 120);
DECLARE @EndTime DATETIME = CONVERT(DATETIME,'{{ params.end_time }}', 120);

SELECT ServiceCode,
        ValueAddedServiceCode,
        ItemCode,
        Freight,
        PhaseCode,
        AddedDate,
        FreightVAT,
        POSCode,
        OriginalFreight,
        OriginalFreightVAT,
        SubFreight,
        SubFreightVAT,
        OriginalSubFreight,
        OriginalSubFreightVAT,
        CreateTime,
        LastUpdatedTime
FROM {{ params.src_schema }}.{{ params.src_table }}
WHERE Item_code in (
          SELECT Item_code
          FROM {{ params.src_schema }}.Item
          WHERE LastUpdatedTime >= @StartTime
  AND LastUpdatedTime < @EndTime);
