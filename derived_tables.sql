#Customers Onboard MoM & QoQ
DROP TABLE IF EXISTS zinka.customers;
CREATE TABLE zinka.customers                           AS 
SELECT   actual_customer_name, 
         customer_classification, 
         MIN(month) AS customer_onboard_month, 
         MIN(qtr)   AS customer_onboard_qtr, 
         MIN(year)  AS customer_onboard_year 
FROM     zinka.table_name
GROUP BY actual_customer_name, 
         customer_classification 
ORDER BY 3, 
         1;
         
#Carrier Onboard MoM & QoQ
DROP TABLE IF EXISTS zinka.carriers;
CREATE TABLE zinka.carriers
SELECT   actual_sp_number,
         MIN(month) AS sp_onboard_month, 
         MIN(qtr)   AS sp_onboard_qtr, 
         MIN(year)  AS sp_onboard_year 
FROM     zinka.table_name
WHERE    actual_sp_number <> '' 
GROUP BY actual_sp_number 
ORDER BY 2, 
         1;
         
#Trucks Onboard MoM & QoQ
DROP TABLE IF EXISTS zinka.trucks;
CREATE TABLE zinka.trucks                           AS 
SELECT   registration_number,
         MIN(month) AS truck_onboard_month, 
         MIN(qtr)   AS truck_onboard_qtr, 
         MIN(year)  AS truck_onboard_year 
FROM     zinka.table_name
WHERE    registration_number <> '' 
GROUP BY registration_number 
ORDER BY 2, 
         1;
         
#Core Carriers (Carrier Contr.%)
DROP TABLE IF EXISTS zinka.core_carriers;
CREATE TABLE zinka.core_carriers                           AS 
SELECT     actual_sp_number, 
           revenue, 
           total_revenue, 
           ROUND(revenue/total_revenue,5) AS `% OF total` 
FROM       ( 
                    SELECT   actual_sp_number, 
                             ROUND(SUM(revenue),0) AS revenue 
                    FROM     zinka.table_name
                    WHERE    actual_sp_number <> '' 
                    AND      qtr <=15 
                    GROUP BY actual_sp_number 
                    ORDER BY 2 DESC ) a 
CROSS JOIN 
           ( 
                  SELECT round(sum(revenue),0) AS total_revenue 
                  FROM   zinka.table_name
                  WHERE  actual_sp_number <> '' 
                  AND    qtr <=15 ) b 
ORDER BY   2 DESC;

#Cluster Ranking (Quarter)
DROP TABLEIF EXISTS zinka.cluster_ranking;
CREATE TABLE zinka.cluster_ranking                       AS 
SELECT   RANK() OVER(partition BY qtr ORDER BY gmv DESC) AS rnk, 
         r.* 
FROM     ( 
                  SELECT   from_cluster AS cluster_name, 
                           qtr, 
                           SUM(gmv) AS gmv 
                  FROM     zinka.table_name 
                  WHERE    qtr = 15 
                  GROUP BY 1, 
                           2 
                  ORDER BY 3 DESC ) r;
 
