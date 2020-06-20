Select zbo.id as Order_id,
ngf.parent_name as from_unit,
ngf1.parent_name as from_zone,
zbo.status as Order_status,
bcup.name as customer_name,
bsf.city as from_city,
bsl.name as From_sublocation_name,
bsl.name as From_warehouse_name,
replace(au.username,'dr')as driver_mobile,
bt.registration_number,
tas_time,
concat(au1.first_name,' ',au1.last_name) as OE_Tas_name,
tit.tit_time as Truck_in_transit_timestamp,
max(ddoc.dl_uploaded_timestamp_through_dr_app) as dl_image_uploaded_timestamp_via_dr_app,
ddoc.dl_status,
max(driver_app) as driver_app_points,
max(di.policy_id) as insurance_activated,
max(acp.accepted) as dr_app_accepted,
max(lat.P_timestamp +interval'330'minute) as Latest_loc_timestamp


from (select * from bb.zinka.base_order where start_date >= date '2020-06-01') as zbo
left join bb.zinka.base_customeruserprofile as bcup on bcup.user_id=zbo.user_id
left join bb.zinka.base_location as bsf on bsf.id=zbo.from_city_id
left join bb.zinka.base_sublocation as bsl on bsl.id=zbo.from_sublocation_id
left join bb.zinka.auth_user as au on au.id=zbo.assigned_driver_id
left join bb.zinka.base_truck as bt on zbo.assigned_truck_id=bt.id
left join (select order_id,status,dt_added+interval'330'minute as tas_time, last_modified_by_id  from bb.zinka.base_status where status='Truck Arrival Source'  ) as tas on tas.order_id=zbo.id
left join (select order_id,status,dt_added+interval'330'minute as tit_time  from bb.zinka.base_status where status='Truck In-Transit'  ) as tit on tit.order_id=zbo.id
left join bb.zinka.auth_user as au1 on au1.id =tas.last_modified_by_id
left join (select id,ms_order_id,user_id from supply.driver.base_order )b on b.ms_order_id=zbo.id 
left join (select Mobile , bu.id as user_id, dc.created_on as dl_uploaded_timestamp_through_dr_app, dc.image_verification_status as dl_status , zbo.order_id as latest_order_id
            from supply.driver.base_user bu
            left join (select * from supply.driver.driver_documents where image_type = 'DRIVER_DL') dc on bu.id = dc.user_id
            left join (select max(id) as order_id, assigned_driver_id  from bb.zinka.base_order group by 2) as zbo on zbo.assigned_driver_id = bu.id
            where dc.created_on  is not null
            group by 1,2,3,4,5
            order by 5 desc)as ddoc on  ddoc.user_id=zbo.assigned_driver_id
left join (select * from bb.zinka.base_driverprofile )as dp on dp.user_id=zbo.assigned_driver_id
left join supply.driver.driver_insurance as di on di.order_id=b.id
left join (select distinct x.*
from 
(select order_id,order_status,status_updated_time+interval'330'minute as status_updated_time,cast(json_extract(all_gps_coverage, '$.data_points')as int )gps,
cast(json_extract(all_driver_app_coverage, '$.data_points')as int)driver_app,cast(json_extract(all_sim_coverage, '$.data_points')as int)sim, 
(cast(json_extract(all_gps_coverage, '$.data_points')as int)+cast(json_extract(all_driver_app_coverage, '$.data_points')as int)+cast(json_extract(all_sim_coverage, '$.data_points')as int))as tracking_points,rank()over(partition by order_id order by status_updated_time desc )as rnk 
from cache.cache.order_device_coverage_new where  status_updated_time+interval'330'minute>= date '2020-04-01') as x where x.rnk=1) as tr on tr.order_id=zbo.id
left join 
( select * from location_service.public.node_group 
where child_type = 'LOCATION' and parent_type = 'UNIT' and deleted=false 
and parent_name!='Pan-India'
) as ngf  on ngf.child_id=zbo.from_city_id 
left join 
(select * from location_service.public.node_group 
where child_type = 'UNIT' and parent_type = 'ZONE' and deleted=false ) as ngf1 on ngf1.child_id=ngf.parent_id
left join (select order_id,type,action_time,cast(json_extract(metadata,'$.card_action') as varchar) as accepted,
case when action_time is not null then 1 else 0 end  as action_taken from   
supply.driver.driver_card where type='ACCEPTANCE' )as acp on acp.order_id=b.id

left join tracking_source_prod.public.tracking_entity as te on te.unique_id=replace(au.username,'dr')
left join tracking_source_prod.public.tracking_entity_device_mapping as tedm on tedm.tracking_entity_id=te.id
-- left join (select * from tracking_source_prod.public.device where type='MS_DRIVER_APP')as d on d.id=tedm.device_id 
left join (select * from tracking_data_prod.public.latestlocation where device_type in ('MS_DRIVER_APP') ) Lat on tedm.device_id=Lat.device_id


where 1=1
and zbo.status not in ('Cancelled','Cancelled by Customer','Order Processing','KAM Review')
-- and ngf1.parent_name in ('East') --- Zone
-- and ngf.parent_name in ('Kolkata') --- unit 
-- and bsf.city in ('Haldia') -- From_location 
and bcup.name not in ('Kerry Indev Logistics Pvt. Ltd.','SJA Shipping Pvt Ltd','Maple Logistics Private Limited','Continental Multimodal Terminals Limited','Ashte Logistics Pvt Ltd','APM TERMINALS INDIA PVT LTD','Damco India Private Limited','Maersk Line A/S','Seabird Marine Services Pvt. Ltd.','Ahlers India Pvt Ltd','EFC Logistics India Pvt Ltd','JWC Logistics Park Private Limited') 
-- and tas_time is not NUll
and tit.tit_time is not NULL

group by 1,2,3,4,5,6,7,8,9,10,11,12,13,15
order by 11 desc
