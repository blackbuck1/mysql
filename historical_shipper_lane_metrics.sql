#       1. # of existing customers, customer-lane, orders, gmv & tonnage

SELECT
   customer_classification,
   `month`,
   COUNT(DISTINCT actual_customer_name) AS total_customer,
   COUNT(DISTINCT CONCAT(actual_customer_name,from_city,to_city)) AS total_customer_lane,
   COUNT(order_id) AS total_order,
   SUM(gmv) AS GMV,
   SUM(tiger_tonnage) AS tonnage
FROM
   orders 
WHERE
   customer_classification <> 'Delta' 
   AND actual_customer_name <> '' 
   AND `year` = 2019
GROUP BY
   1,
   2 
ORDER BY
   2,
   1 ASC;
   
#      2. # of new customers, customer-lane, orders, gmv & tonnage    
