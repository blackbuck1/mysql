SELECT 
  CASE WHEN ngmp <= -0.05 THEN '-5% and below' WHEN ngmp > -0.05 
  AND ngmp <= 0 THEN '-5% to 0%' WHEN ngmp > 0 
  AND ngmp <= 0.05 THEN '0% to 5%' ELSE '5% above' END AS gm_bucket, 
  customer_classification, 
  qtr_name, 
  SUM(gmv) AS gmv, 
  SUM(revenue) AS revenue, 
  SUM(NGM2) AS margin, 
  COUNT(DISTINCT actual_customer_name) AS shipper_count 
FROM 
  (
    SELECT 
      actual_customer_name, 
      customer_classification, 
      qtr_name, 
      SUM(gmv) AS gmv, 
      SUM(revenue) AS revenue, 
      SUM(revenue - base_cost + bonus) AS NGM2, 
      SUM(revenue - base_cost + bonus)/ SUM(gmv) AS ngmp 
    FROM 
      zlog.order_details 
    WHERE 
      qtr = 16 
      AND customer_classification = 'Corporate' 
      AND actual_customer_name <> '' 
    GROUP BY 
      1, 
      2, 
      3
  ) a 
GROUP BY 
  1, 
  2, 
  3
