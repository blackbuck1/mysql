# Shipper Bucket

SELECT
   month,
   business_type,
   bucket,
   COUNT(DISTINCT actual_customer_name) AS Cust_Count 
FROM
   (
      SELECT
         *,
         CASE
            WHEN
               revenue > 5000000 
            THEN
               '50+' 
            WHEN
               revenue > 3000000 
            THEN
               '30+' 
            WHEN
               revenue > 0 
            THEN
               '30-' 
            ELSE
               '' 
         end
         AS Bucket 
      FROM
         (
            SELECT
               month,
               actual_customer_name,
               business_type,
               ROUND(SUM(revenue), 0) AS revenue 
            FROM
               blackbuck_freight.orders 
            WHERE
               MONTH = 54 
               AND customer_type <> 'Delta' 
            GROUP BY
               1,
               2,
               3 
            ORDER BY
               4 DESC
         )
         A
   )
   B 
GROUP BY
   1,
   2,
   3
ORDER BY
   1,
   2,
   3;
   
# Shipper Revenue Growth Trends - Overall

SELECT
   month,
   customer_onboard_year,
   ROUND(SUM(revenue), 0) AS revenue 
FROM
   blackbuck_freight.orders 
WHERE
   MONTH = 54 
   AND customer_type <> 'Delta' 
GROUP BY
   1,
   2 
ORDER BY
   2
