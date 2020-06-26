--Owner: Nihal Singh Verma
--Query: Services FO Orders v2

SELECT ngf1.parent_name AS from_zone, 
ngf.parent_name AS from_unit,
zbo.id AS order_id, 
zbo.status AS order_status,
bt.registration_number,
aus.username AS sp_mobile_number,
bcup.name AS customer_name,
bsf.city AS from_city, 
bst.city AS to_city,
bsl.name AS from_warehouse_name, 
zbo.start_date + interval '330'minute AS order_creation_timestamp,
tas.tas_time AS truck_arrival_source_timestamp,
tit.tit_time AS truck_in_transit_timestamp,
-- AS ETA_given_by_sp,
(CASE WHEN cast(rd.freight AS int)=cast(rd.raw_rate AS int) OR ( rd.raw_rate>=10000 ) THEN freight ELSE rd.raw_rate END) AS procurement_rate,
(CASE WHEN cast(rd.freight AS int)=cast(rd.raw_rate AS int) OR ( rd.raw_rate>=10000 ) THEN 'per_truck' ELSE 'per_ton' END) AS procurement_type,
bi.client_indent_id AS indent_id,
ind.status AS indent_status,
fn.order_id AS last_fulfilled_order_id,
fn.order_creation_timestamp AS created_timestamp_for_lst_order_id,
fn.procurement_rate AS procurement_rate_last_fulfilled_order_id
FROM bb.zinka.base_order zbo 
INNER JOIN bb.zinka.base_customeruserprofile AS bcup ON bcup.user_id = zbo.user_id 
LEFT JOIN bb.zinka.base_location AS bsf ON bsf.id = zbo.from_city_id
LEFT JOIN bb.zinka.base_location AS bst ON bst.id = zbo.to_city_id 
LEFT JOIN bb.zinka.base_sublocation AS bsl ON bsl.id = zbo.from_sublocation_id
LEFT JOIN bb.zinka.base_truck AS bt ON zbo.assigned_truck_id = bt.id
LEFT JOIN bb.zinka.auth_user AS aus ON aus.id = zbo.supply_partner_id
LEFT JOIN (SELECT phone_no, physical_verification, transacting_carrier from divum.blackbuck.fleetapp_fleetowner) x ON aus.username = x.phone_no
LEFT JOIN location_service.public.node_group AS ngf ON ngf.child_id = zbo.from_city_id
AND ngf.child_type = 'LOCATION'
AND ngf.parent_type = 'UNIT'
AND ngf.deleted = FALSE
AND ngf.parent_name != 'Pan-India'
LEFT JOIN location_service.public.node_group AS ngf1 ON ngf1.child_id = ngf.parent_id
AND ngf1.child_type = 'UNIT'
AND ngf1.parent_type = 'ZONE'
AND ngf1.deleted = FALSE
LEFT JOIN 
(
	SELECT order_id, 
	status, 
	dt_added + interval '330' minute AS tas_time, 
	last_modified_by_id 
	FROM bb.zinka.base_status 
	WHERE status = 'Truck Arrival Source'
) AS tas 
ON zbo.id = tas.order_id
LEFT JOIN bb.zinka.auth_user AS au1 ON tas.last_modified_by_id = au1.id 
INNER JOIN
(
	SELECT order_id, 
	status, 
	dt_added + interval '330' minute AS tit_time 
	FROM bb.zinka.base_status 
	WHERE status = 'Truck In-Transit'
) AS tit
ON zbo.id = tit.order_id
LEFT JOIN bb.zinka.base_indent bi ON zbo.indent_id = bi.id
LEFT JOIN indent.indent_microservice.indent ind ON CAST(bi.client_indent_id AS INTEGER) = ind.id
LEFT JOIN invoice.invoicing.revenue_data AS rd on rd.order_id = zbo.id
LEFT JOIN
(
    SELECT indent_id,
    order_id,
    order_creation_timestamp,
    procurement_rate
    FROM
    (
        SELECT bi.client_indent_id AS indent_id,
        zbo.id AS order_id,
        zbo.start_date + interval '330'minute AS order_creation_timestamp,
        (CASE WHEN cast(rd.freight AS int)=cast(rd.raw_rate AS int) OR ( rd.raw_rate>=10000 ) THEN freight ELSE rd.raw_rate END) AS procurement_rate,
        RANK() OVER(PARTITION BY bi.client_indent_id ORDER BY zbo.start_date DESC) AS rnk
        FROM bb.zinka.base_order zbo
        LEFT JOIN invoice.invoicing.revenue_data AS rd on rd.order_id = zbo.id
        LEFT JOIN bb.zinka.base_indent bi on zbo.indent_id = bi.id
        LEFT JOIN indent.indent_microservice.indent ind on cast(bi.client_indent_id as integer) = ind.id
        WHERE zbo.start_date >= DATE '2020-06-01'
        --AND DATE(ind.created_on + interval '330' minute) >= DATE '2020-06-01'
        AND zbo.status NOT IN (
            'Cancelled',
            'Cancelled by Customer'
        )
    ) a
    WHERE rnk = 1
) fn
ON fn.indent_id = bi.client_indent_id

--- Filters ---
WHERE (
	1 = 1
	AND	zbo.start_date >= DATE '2020-06-01'
    --AND zbo.status IN ('Cancelled', 'Cancelled by Customer', 'Order Processing', 'KAM Review')
)
AND (
    x.physical_verification = false AND
    x.transacting_carrier = true
)