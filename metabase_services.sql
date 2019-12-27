#Transacting SP wise Business (DB:Presto)
SELECT 
  month, 
  fo_onboarded_month, 
  phone, 
  fo_id, 
--truck_onboarding_month, 
--registered_truck_no, 
--truck_id, 
  SUM(toll) AS toll, 
  SUM(fuel) AS fuel, 
  SUM(gps_amt) AS gps_amt,
  SUM(toll+fuel) AS toll_fuel,
  SUM(toll+fuel+gps_amt) AS total_revenue
FROM 
  (
    SELECT * FROM 
      (
        SELECT 
          CASE WHEN d.dt IS NULL THEN gps_txn_dt ELSE d.dt end AS month, 
          d.dt AS recharge_month, 
          a.dt_onboarded AS FO_Onboarded_month, 
          a.phone_no AS Phone, 
          a.id AS FO_ID, 
          b.truck_added AS truck_onboarding_month, 
          b.truck_no AS Registered_Truck_no, 
          b.truck_id, 
          Coalesce(d.fastag_recharge,0) AS Toll, 
          Coalesce(d.fuel_recharge,0) AS Fuel, 
          c.gps_txn_dt, 
          Coalesce(c.gps_amt,0) AS GPS_amt 
        FROM 
          (
            SELECT 
              id, 
              name, 
              phone_no, 
              Date_format(
                app_installed_on + INTERVAL '5' hour + INTERVAL '30' minute, 
                '%Y%m'
              ) AS dt_onboarded 
            FROM 
              divum.blackbuck.fleetapp_fleetowner
          ) a 
          LEFT JOIN (
            SELECT 
              fleet_owner_id, 
              id AS truck_id, 
              truck_no, 
              Date_format(
                Date(
                  created_at + INTERVAL '5' hour + INTERVAL '30' minute
                ), 
                '%Y%m'
              ) AS truck_added 
            FROM 
              divum.blackbuck.fleetapp_truck 
            WHERE 
              is_verified = 1 
              AND (
                truck_no IS NOT NULL 
                OR Lower(truck_no) != 'na'
              )
          ) b ON a.id = b.fleet_owner_id 
          LEFT JOIN (
            SELECT 
              truck_number, 
              Date_format(
                Date(
                  created_on + INTERVAL '5' hour + INTERVAL '30' minute
                ), 
                '%Y%m'
              ) AS gps_txn_dt, 
              Sum(paid_amount) AS gps_amt 
            FROM 
              divum.blackbuck.gps_subscription_txns 
            WHERE 
              Date_format(
                Date(
                  created_on + INTERVAL '5' hour + INTERVAL '30' minute
                ), 
                '%Y%m'
              ) = '{{dt}}' 
            GROUP BY 
              1, 
              2
          ) c ON c.truck_number = b.truck_no 
          LEFT JOIN (
            SELECT 
              truck_no, 
              user_id, 
              Date_format(
                updated_at + INTERVAL '5' hour + INTERVAL '30' minute, 
                '%Y%m'
              ) AS dt, 
              Coalesce(
                Sum(
                  CASE WHEN status IN (
                    'HPCL-Card-Recharge', 'HPCLCardless', 
                    'Reliance-Card-Recharge', 'RelianceCardless', 
                    'Card-Recharge', 'Cash'
                  ) THEN amount end
                ), 
                0
              ) - Coalesce(
                Sum(
                  CASE WHEN status IN ('Card-Pull', 'Card-less Pull') THEN amount end
                ), 
                0
              ) AS fuel_recharge, 
              Coalesce(
                Sum(
                  CASE WHEN status IN ('FasTag Recharge') THEN amount end
                ), 
                0
              ) - Coalesce(
                Sum(
                  CASE WHEN status IN ('FastageRecharge Failure') THEN amount end
                ), 
                0
              ) AS fastag_recharge 
            FROM 
              (
                SELECT 
                  truck_no, 
                  user_id, 
                  amount, 
                  updated_at, 
                  status 
                FROM 
                  services_payment.blackbuck.wallet_wallettransactionhistory 
                WHERE 
                  status IN (
                    'HPCL-Card-Recharge', 'HPCLCardless', 
                    'Reliance-Card-Recharge', 'RelianceCardless', 
                    'Card-Recharge', 'Card-Pull', 'Card-less Pull', 
                    'FasTag Recharge', 'FastageRecharge Failure'
                  ) 
                UNION ALL 
                SELECT 
                  d.truck_no AS truck_no, 
                  c.user_id AS user_id, 
                  a.amount AS amount, 
                  a.created_on AS updated_at, 
                  'Cash' AS status 
                FROM 
                  (
                    SELECT 
                      * 
                    FROM 
                      divum.blackbuck.voucher_transaction
                  ) a 
                  LEFT JOIN (
                    SELECT 
                      * 
                    FROM 
                      divum.blackbuck.voucher
                  ) b ON a.voucher_id = b.id 
                  LEFT JOIN (
                    SELECT 
                      * 
                    FROM 
                      divum.blackbuck.fleetapp_fleetowner
                  ) c ON b.fleet_owner_id = c.id 
                  LEFT JOIN (
                    SELECT 
                      * 
                    FROM 
                      divum.blackbuck.fleetapp_truck
                  ) d ON b.truck_id = d.id 
                WHERE 
                  a.type = 'DEBIT' 
                  AND b.mode IN ('CASH', 'ACCOUNT')
              ) a 
            WHERE 
              Date_format(
                updated_at + INTERVAL '5' hour + INTERVAL '30' minute, 
                '%Y%m'
              ) = '{{dt}}' 
            GROUP BY 
              1, 
              2, 
              3
          ) d ON d.truck_no = b.truck_no -- and d.dt = b.truck_added 
          -- where a.id = 34 
          ) a 
    WHERE 
      (
        a.recharge_month = '{{dt}}' 
        OR gps_txn_dt = '{{dt}}'
      ) 
      AND Length(registered_truck_no) > 4
  ) a
GROUP BY 1,2,3,4
--HAVING SUM(toll+fuel+gps_amt) > 0


#Registered FO (DB:prod-divum-mysql-slave-redshift)
select 
phone_no,
date_format(date(convert_tz(app_installed_on,'+00:00','+05:30')),'%Y%m%d') as install_dt,
date_format(date(convert_tz(app_installed_on,'+00:00','+05:30')),'%Y%m') as month
#count(distinct id) as verified_user 
from fleetApp_fleetowner 
where is_verified = 1
and date_format(date(convert_tz(app_installed_on,'+00:00','+05:30')),'%Y%m') = 201911
and date_format(date(convert_tz(app_installed_on,'+00:00','+05:30')),'%Y%m%d') <= 20191124
group by 1,2,3
