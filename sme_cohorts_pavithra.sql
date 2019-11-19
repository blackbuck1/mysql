SELECT 
  order_id, 
  actual_customer_name, 
  actual_sector, 
  customer_onboard_month, 
  customer_onboard_qtr, 
  from_city, 
  to_city, 
  '' AS lane, 
  from_cluster, 
  '' AS state_name, 
  corridor, 
  order_truck_type, 
  '' AS truck_type, 
  payment_done_date, 
  month_name, 
  qtr_name, 
  revenue, 
  cost 
FROM 
  zlog.order_details_201910 a 
WHERE 
  customer_classification = 'SME' 
  AND customer_type <> 'SME App' 
  AND actual_customer_name <> '' 
  AND YEAR = 2019
