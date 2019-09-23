# Revenue Bucket & Customer wise Customer Count  
SELECT month, 
       customer_classification, 
       bucket, 
       COUNT(DISTINCT actual_customer_name) AS Cust_Count 
FROM   (SELECT *, 
               CASE 
                 WHEN revenue > 5000000 THEN '50+' 
                 WHEN revenue > 3000000 THEN '30+' 
                 WHEN revenue > 0 THEN '30-' 
                 ELSE '' 
               end AS Bucket 
        FROM   (SELECT month, 
                       actual_customer_name, 
                       customer_classification, 
                       ROUND(SUM(revenue), 0) AS revenue 
                FROM   zinka.base_order_v8 
                WHERE  month = 44 
                       AND customer_type <> 'Delta' 
                GROUP  BY month, 
                          actual_customer_name, 
                          customer_classification 
                ORDER  BY revenue DESC) A) B 
GROUP  BY month, 
          customer_classification, 
          bucket 
ORDER  BY month, 
          customer_classification, 
          bucket; 
          
#  
