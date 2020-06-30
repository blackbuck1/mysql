SELECT DISTINCT DATE(cw.created_on + INTERVAL '330' minute) AS warehouse_creation_date, 
cw.bb_place_id AS warehouse_place_id, 
wp.name AS warehouse_name, 
cw.customer_id, 
bcup.name AS customer_name, 
zbl.city AS from_location, 
wp.address, 
cw.loading_tat, 
wp.contact_name, 
wp.contact_number, 
wp.timings, 
wp.last_updated_by AS places_table_last_updated_by, 
  COUNT(DISTINCT i.id) AS no_of_indents_last_9_months 
FROM exchange.demand.customer_warehouse cw 
INNER JOIN bb.zinka.base_customeruserprofile bcup ON CAST(cw.customer_id AS INT)= bcup.user_id 
INNER JOIN location_service.public.warehouse_places wp ON cw.bb_place_id = wp.place_id 
LEFT JOIN indent.indent_microservice.indent_warehouse iw ON iw.start_warehouse_id = wp.old_id 
AND wp.deleted = FALSE 
AND wp.source = 'OMS' 
LEFT JOIN bb.zinka.base_location zbl ON iw.from_location_id = zbl.id 
LEFT JOIN indent.indent_microservice.indent i ON iw.indent_id = i.id 
AND cw.customer_id = i.customer_id 
AND i.is_deleted = 0 
AND i.business_type <> 'Spot' 
AND DATE(i.created_on + INTERVAL '330' minute)>= date_add('month', -9,DATE(now())) 
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
HAVING COUNT(DISTINCT i.id)> 0 
ORDER BY 13 DESC
