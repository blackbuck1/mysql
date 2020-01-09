# Supplier Phone Number to Name Mapping

SELECT username                           AS sp_number, 
       Concat(first_name, ' ', last_name) AS sp_name, 
       last_login                         AS last_updated_on 
FROM   zinka.auth_user 
UNION ALL 
SELECT contracted_sp_mobile AS sp_number, 
       contracted_sp_name   AS sp_name, 
       last_updated_on 
FROM   managed_services.base_contracts 
GROUP  BY 1, 
          2, 
          3

# All Sublocations
SELECT bs.id, 
       bs.name, 
       bs.google_place_id, 
       bl.city, 
       bl.code, 
       bl.district, 
       bl.state, 
       bg.latitude, 
       bg.longitude 
FROM   base_sublocation bs 
       INNER JOIN base_googleplaces bg 
               ON bg.id = bs.google_place_id 
       LEFT JOIN base_location bl 
              ON bs.location_id = bl.id 


# Lane wise Disrance (in KMs)

SELECT from_city_id, 
       b1.city              AS from_city, 
       b1.code              AS from_city_code, 
       to_city_id, 
       b2.city              AS to_city, 
       b2.code              AS to_city_code, 
       Avg(distance) / 1000 AS avg_distance 
FROM   base_order b 
       LEFT JOIN base_location b1 
              ON from_city_id = b1.id 
       LEFT JOIN base_location b2 
              ON to_city_id = b2.id 
GROUP  BY from_city_id, 
          b1.city, 
          b1.code, 
          to_city_id, 
          b2.city, 
          b2.code 


# Truck Capacity

SELECT truck_type, 
       rtkm_tonnage AS capacity 
FROM   base_trucktype 

#Order Id to Truck & SP Mapping

select a.order_id,
b.username,
a.supply_partner_id,
a.mobile_number,
a.assigned_truck_id,
a.registration_number,
c.name
#a.* 
from base_ordersupplypartnertruckhistory a
left join zinka.auth_user  b
on a.supply_partner_id = b.id
left join zinka.base_truck c
on a.assigned_truck_id = c.id
where order_id IN (
'691550',
'1220286'
)
