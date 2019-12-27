#Transacting SP wise Business (DB:Presto)

SELECT phone_no AS sp_number,
dt AS month,
'' AS col3,
SUM(fuel_recharge) AS fuel_recharge,
SUM(fastag_recharge) AS fastag_recharge
FROM
(
	SELECT * FROM
	(
		SELECT truck_no,
		user_id,
		date_format(updated_at + interval '5' HOUR + interval '30' MINUTE, '%Y%m') AS dt,
		coalesce(sum(CASE WHEN status IN ('HPCL-Card-Recharge', 'HPCLCardless', 'Reliance-Card-Recharge', 'RelianceCardless', 'Card-Recharge', 'Cash') THEN amount END), 0) - coalesce(sum(CASE WHEN status IN ('Card-Pull', 'Card-less Pull') THEN amount END), 0) AS fuel_recharge,
		coalesce(sum(CASE WHEN status IN ('FasTag Recharge') THEN amount END), 0) - coalesce(sum(CASE WHEN status IN ('FastageRecharge Failure') THEN amount END), 0) AS fastag_recharge
		FROM
		(
			SELECT truck_no,
			user_id,
			amount,
			updated_at,
			status
			FROM services_payment.blackbuck.wallet_wallettransactionhistory
			WHERE status IN 
			(
				'HPCL-Card-Recharge',
				'HPCLCardless',
				'Reliance-Card-Recharge',
				'RelianceCardless',
				'Card-Recharge',
				'Card-Pull',
				'Card-less Pull',
				'FasTag Recharge',
				'FastageRecharge Failure'
			)
			
			UNION ALL 
			
			SELECT d.truck_no AS truck_no,
			c.user_id AS user_id,
			a.amount AS amount,
			a.created_on AS updated_at,
			'Cash' AS status
			FROM
			(
				SELECT * FROM divum.blackbuck.voucher_transaction
			) a
			LEFT JOIN
			(
				SELECT * FROM divum.blackbuck.voucher) b ON a.voucher_id = b.id
			LEFT JOIN
			(
				SELECT * FROM divum.blackbuck.fleetApp_fleetowner) c ON b.fleet_owner_id = c.id
			LEFT JOIN
			(
			SELECT * FROM divum.blackbuck.fleetApp_truck
			) d 
			ON b.truck_id = d.id
			WHERE a.type = 'DEBIT' AND b.mode IN ('CASH',	'ACCOUNT') 
		) a
		WHERE date_format(updated_at + interval '5' HOUR + interval '30' MINUTE, '%Y%m') = '{{month}}'
		GROUP BY 1, 2, 3
	) a
	INNER JOIN
	(
		SELECT user_id,
		phone_no
		FROM divum.blackbuck.fleetApp_fleetowner
	) b 
	ON a.user_id = b.user_id
	WHERE a.dt = '{{month}}' 
) f
GROUP BY 1,2,3


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
