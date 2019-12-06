SELECT 
  CASE WHEN ngmp < -0.1 THEN '-10% and below' 
  WHEN ngmp >= -0.1  AND ngmp < 0 THEN '-10% to 0%' 
  WHEN ngmp >= 0   AND ngmp < 0.1 THEN '0% to 10%' 
  ELSE '10% above' END AS gm_bucket, 
  qtr_name, 
  SUM(gmv) AS gmv, 
  SUM(revenue) AS revenue
FROM 
  (
    SELECT 
      actual_customer_name, 
      customer_classification, 
      qtr_name, 
      gmv, 
      revenue, 
      #CASE WHEN customer_classification = 'Corporate' THEN revenue - base_cost + bonus ELSE revenue - base_cost END AS ngm2, 
      (CASE WHEN customer_classification = 'Corporate' THEN revenue - base_cost + bonus ELSE revenue - base_cost END)/ gmv AS ngmp 
    FROM 
      zlog.order_details 
    WHERE 
      qtr <= 16 
      #AND customer_classification = 'Corporate' 
      #AND actual_customer_name <> '' 
  ) a 
GROUP BY 
  1, 
  2
