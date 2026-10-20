-- Which dashboards and notebooks scan the most: Athena bytes by workgroup, from CloudTrail events in Athena.
SELECT json_extract_scalar(requestparameters, '$.workGroup')                       AS workgroup,
       count(*)                                                                    AS queries,
       round(sum(cast(json_extract_scalar(additionaleventdata, '$.dataScannedInBytes') AS double))
             / power(1024, 4) * 5, 2)                                              AS usd_at_5_per_tb
FROM   cloudtrail.events
WHERE  eventsource = 'athena.amazonaws.com' AND eventname = 'GetQueryExecution'
  AND  from_iso8601_timestamp(eventtime) >= current_timestamp - INTERVAL '30' DAY
GROUP BY 1 ORDER BY usd_at_5_per_tb DESC;
