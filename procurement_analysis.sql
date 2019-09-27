SELECT from_city, 
       to_city, 
       from_cluster, 
       to_cluster, 
       qtr_name, 
       COUNT(order_id)                         AS orders, 
       COUNT(DISTINCT actual_sp_number)        AS sps, 
       COUNT(DISTINCT registration_number)     AS trucks, 
       ROUND(SUM(gmv), 0)                      AS gmv, 
       ROUND(SUM(revenue), 0)                  AS revenue, 
       ROUND(SUM(base_cost), 0)                AS base_cost, 
       ROUND(SUM(cost), 0)                     AS cost, 
       ROUND(SUM(tiger_tonnage), 0)            AS tiger_tonnage 
FROM   base_order_v9 
WHERE  year >= 2017 
       AND qtr <= 15 
GROUP  BY 1, 
          2, 
          3, 
          4, 
          5
