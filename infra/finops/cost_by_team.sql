-- Spend by owning team and workload, last 90 days, from the AWS Cost and Usage Report (CUR 2.0 in Athena).
-- Published weekly next to the incident review, so a cost regression is handled like a defect.
SELECT resource_tags['user_owning_team']            AS owning_team,
       resource_tags['user_workload']               AS workload,
       product['product_name']                      AS service,
       round(sum(line_item_unblended_cost), 0)      AS spend_90d,
       round(100 * sum(line_item_unblended_cost)
             / sum(sum(line_item_unblended_cost)) OVER (), 1) AS pct
FROM   cur.data_exports
WHERE  line_item_usage_start_date >= current_date - INTERVAL '90' DAY
  AND  product['product_name'] IN ('AWS Glue', 'Amazon Athena', 'Amazon Simple Storage Service',
                                   'AWS Database Migration Service', 'Amazon Relational Database Service')
GROUP BY 1, 2, 3
ORDER BY spend_90d DESC;
