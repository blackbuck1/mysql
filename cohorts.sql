#Customer Cohort
SELECT   actual_customer_name, 
         customer_classification, 
         customer_onboard_qtr, 
         qtr_name, 
         ROUND(SUM(gmv),0)             AS gmv, 
         Count(order_id)               AS orders, 
         ROUND(SUM(demand_discount),0) AS demand_discount, 
         ROUND(SUM( 
         CASE 
                  WHEN customer_type = 'MS Order' THEN supply_incentive - other_cost 
                  ELSE supply_incentive 
         END),0) AS supply_incentive, 
         ROUND(SUM( 
         CASE 
                  WHEN customer_type = 'MS Order' THEN gmv - potential_cost 
                  ELSE gmv                                 - potential_cost + other_cost 
         END),0)                         AS take_rate, 
         ROUND(SUM(revenue-base_cost),0) AS cont_margin 
FROM     zinka.table_name 
WHERE actual_customer_name <> '' 
GROUP BY 1, 
         2, 
         3, 
         4;

#Carrier Cohort
SELECT actual_sp_number, 
       sp_onboard_qtr, 
       qtr_name, 
       ROUND(SUM(gmv), 0)                 AS gmv, 
       Count(order_id)                    AS orders, 
       ROUND(SUM(demand_discount), 0)     AS demand_discount, 
       ROUND(SUM(CASE 
                   WHEN customer_type = 'MS Order' THEN 
                   supply_incentive - other_cost 
                   ELSE supply_incentive 
                 END), 0)                 AS supply_incentive, 
       ROUND(SUM(CASE 
                   WHEN customer_type = 'MS Order' THEN gmv - potential_cost 
                   ELSE gmv - potential_cost + other_cost 
                 END), 0)                 AS take_rate, 
       ROUND(SUM(revenue - base_cost), 0) AS cont_margin 
FROM   zinka.table_name 
WHERE  actual_sp_number <> '' 
GROUP  BY 1, 
          2, 
          3; 

#Cluster Cohort
SELECT r.rnk, 
       b.* 
FROM   (SELECT from_cluster, 
               qtr_name, 
               ROUND(SUM(gmv), 0)                 AS gmv, 
               Count(order_id)                    AS orders, 
               ROUND(SUM(demand_discount), 0)     AS demand_discount, 
               ROUND(SUM(CASE 
                           WHEN customer_type = 'MS Order' THEN 
                           supply_incentive - other_cost 
                           ELSE supply_incentive 
                         END), 0)                 AS supply_incentive, 
               ROUND(SUM(CASE 
                           WHEN customer_type = 'MS Order' THEN gmv 
                           - potential_cost 
                           ELSE gmv - potential_cost + other_cost 
                         END), 0)                 AS take_rate, 
               ROUND(SUM(revenue - base_cost), 0) AS cont_margin 
        FROM   zinka.table_name 
        WHERE  month = 45 
        GROUP  BY 1, 
                  2) b 
       LEFT JOIN (SELECT rnk, 
                         cluster_name 
                  FROM   cluster_rank 
                  WHERE  qtr = 15) r 
              ON b.from_cluster = r.cluster_name 
ORDER  BY 3, 
          1;

#Kandla-Karnal Correction
SELECT CASE 
         WHEN customer_type = 'MS Order' THEN customer_type 
         ELSE 'Non - MS' 
       END             AS client_type, 
       a.from_cluster  AS from_cluster_name, 
       c.cluster_name  AS to_cluster, 
       qtr_name, 
       COUNT(order_id) AS orders, 
       SUM(cost)       AS cost, 
       SUM(base_cost)  AS base_cost, 
       SUM(revenue)    AS revenue, 
       SUM(gmv)        AS gmv, 
FROM   zinka.table_name a 
       LEFT JOIN (SELECT * 
                  FROM   zinka.cluster_mapping 
                  WHERE  cluster_type = 'From-Cluster') c 
              ON a.to_city = c.city 
WHERE  a.from_cluster IN ( 'Kandla', 'Karnal', 'Bareilly' ) 
       AND c.cluster_name IN ( 'Kandla', 'Karnal', 'Bareilly' ) 
       AND qtr > 10 
GROUP  BY 1, 
          2, 
          3, 
          4 
ORDER  BY 1, 
          4 ASC; 
          
#SME Customer Cohort
SELECT `customer_name`, 
       onboard_qtr, 
       m.qtr_name AS qtr, 
       gmv, 
       orders, 
       demand_discount, 
       supply_incentive, 
       take_rate, 
       cont_margin 
FROM   (SELECT actual_customer_name               AS `customer_name`, 
               customer_classification, 
               CASE 
                 WHEN customer_onboard_qtr < 8 THEN 8 
                 ELSE customer_onboard_qtr 
               end                                onboard_qtr, 
               CASE 
                 WHEN customer_onboard_qtr <= 8 
                      AND customer_onboard_qtr = qtr THEN 'True' 
                 WHEN customer_onboard_qtr <= 8 
                      AND qtr > 8 THEN 'True' 
                 WHEN customer_onboard_qtr > 8 THEN 'True' 
                 ELSE 'False' 
               end                                tag, 
               customer_onboard_qtr, 
               CASE 
                 WHEN qtr < 8 THEN 8 
                 ELSE qtr 
               end                                qtr, 
               ROUND(SUM(gmv), 0)                 AS gmv, 
               Count(order_id)                    AS orders, 
               ROUND(SUM(demand_discount), 0)     AS demand_discount, 
               ROUND(SUM(CASE 
                           WHEN customer_type = 'MS Order' THEN 
                           supply_incentive - other_cost 
                           ELSE supply_incentive 
                         end), 0)                 AS supply_incentive, 
               ROUND(SUM(CASE 
                           WHEN customer_type = 'MS Order' THEN gmv 
                           - potential_cost 
                           ELSE gmv - potential_cost + other_cost 
                         end), 0)                 AS take_rate, 
               ROUND(SUM(revenue - base_cost), 0) AS cont_margin 
        FROM   zinka.table_name 
        WHERE  customer_classification = 'SME' 
               AND qtr <= 15 
        GROUP  BY 1, 
                  2, 
                  3, 
                  4, 
                  5, 
                  6) a 
       LEFT JOIN (SELECT qtr, 
                         quarter_name AS qtr_name 
                  FROM   zinka.month_mapping 
                  GROUP  BY 1, 
                            2) m 
              ON a.qtr = m.qtr 
WHERE  a.tag = 'True';

#Cluster Lane Cohort
SELECT from_cluster, 
       to_cluster, 
       qtr_name, 
       ROUND(SUM(gmv), 0)                 AS gmv, 
       Count(order_id)                    AS orders, 
       ROUND(SUM(demand_discount), 0)     AS demand_discount, 
       ROUND(SUM(CASE 
                   WHEN customer_type = 'MS Order' THEN 
                   supply_incentive - other_cost 
                   ELSE supply_incentive 
                 END), 0)                 AS supply_incentive, 
       ROUND(SUM(CASE 
                   WHEN customer_type = 'MS Order' THEN gmv - potential_cost 
                   ELSE gmv - potential_cost + other_cost 
                 END), 0)                 AS take_rate, 
       ROUND(SUM(revenue - base_cost), 0) AS cont_margin 
FROM   zinka.table_name 
GROUP  BY 1, 
          2, 
          3 
ORDER  BY 2, 
          1 ASC;


#Top 60 Customer (Quarter 15)
SELECT rnk, 
       c.* 
FROM   (SELECT actual_customer_name, 
               customer_classification, 
               qtr_name, 
               ROUND(SUM(gmv), 0)                 AS gmv, 
               Count(order_id)                    AS orders, 
               ROUND(SUM(demand_discount), 0)     AS demand_discount, 
               ROUND(SUM(CASE 
                           WHEN customer_type = 'MS Order' THEN 
                           supply_incentive - other_cost 
                           ELSE supply_incentive 
                         END), 0)                 AS supply_incentive, 
               ROUND(SUM(CASE 
                           WHEN customer_type = 'MS Order' THEN gmv 
                           - potential_cost 
                           ELSE gmv - potential_cost + other_cost 
                         END), 0)                 AS take_rate, 
               ROUND(SUM(revenue - base_cost), 0) AS cont_margin 
        FROM   zinka.table_name 
        WHERE  actual_customer_name <> '' 
               AND qtr <= 15 
        GROUP  BY 1, 
                  2, 
                  3) c 
       LEFT JOIN (SELECT RANK() 
                           OVER( 
                             partition BY qtr 
                             ORDER BY gmv DESC) AS rnk, 
                         actual_customer_name, 
                         qtr, 
                         gmv 
                  FROM   (SELECT actual_customer_name, 
                                 qtr, 
                                 ROUND(SUM(gmv), 0) AS gmv 
                          FROM   zinka.table_name 
                          WHERE  actual_customer_name <> '' 
                                 AND actual_customer_name NOT IN ( 
                                     'Maple Logistics Pvt Limited', 
                                     'SJA Shipping Pvt Ltd', 'GOSS' 
                                                                 ) 
                                 AND qtr = 15 
                          GROUP  BY 1, 
                                    2) a) d 
              ON c.actual_customer_name = d.actual_customer_name 
WHERE  rnk <= 60 
ORDER  BY 1, 
          4 ASC;

#Top 60 Lanes
SELECT d.rnk, 
       c.* 
FROM   (SELECT CONCAT(from_cluster, '-', to_cluster) AS cluster_lane, 
               qtr_name, 
               ROUND(SUM(gmv), 0)                    AS gmv, 
               Count(order_id)                       AS orders, 
               ROUND(SUM(demand_discount), 0)        AS demand_discount, 
               ROUND(SUM(CASE 
                           WHEN customer_type = 'MS Order' THEN 
                           supply_incentive - other_cost 
                           ELSE supply_incentive 
                         END), 0)                    AS supply_incentive, 
               ROUND(SUM(CASE 
                           WHEN customer_type = 'MS Order' THEN gmv 
                           - potential_cost 
                           ELSE gmv - potential_cost + other_cost 
                         END), 0)                    AS take_rate, 
               ROUND(SUM(revenue - base_cost), 0)    AS cont_margin 
        FROM   zinka.table_name 
        WHERE  from_cluster <> '' 
               AND to_cluster <> '' 
               AND qtr <= 15 
        GROUP  BY 1, 
                  2) c 
       LEFT JOIN (SELECT RANK() 
                           OVER( 
                             partition BY qtr 
                             ORDER BY gmv DESC) AS rnk, 
                         cluster_lane, 
                         qtr, 
                         gmv 
                  FROM   (SELECT CONCAT(from_cluster, '-', to_cluster) AS 
                                 cluster_lane, 
                                 qtr, 
                                 ROUND(SUM(gmv), 0)                    AS gmv 
                          FROM   zinka.table_name 
                          WHERE  from_cluster <> '' 
                                 AND to_cluster <> '' 
                                 AND qtr = 15 
                          GROUP  BY 1, 
                                    2) a) d 
              ON c.cluster_lane = d.cluster_lane 
WHERE  rnk <= 60 
ORDER  BY 1, 
          2, 
          3 ASC;
