select zbo.id as Order_id,
replace(au.username,'dr')as driver_mobile,
ngf1.parent_name as from_zone,
ngf.parent_name as from_unit,
bsf.city as from_city,
bsf2.city as to_city,
bcup.name as customer_name,
zbo.status as Order_status,
Zbo.start_date + interval  '330' minute as Order_creation_time,
tit.tit_time as truck_in_transit_time,
tdd.tdd_time as Truck_departure_destination_time,
-- case when imei is not null then 1 else 0 end as App_Installed,
-- oa_time as Order_accepted_timestamp,
TAS_timestamp_Driver_App,
adv_time.upload_timestamp as Advance_doc_upload_timestamp,
truck_at_unloading_time as truck_arrival_destination_timestamp,
podtime.pod_upload as Pod_upload_timestamp,
max(lat.P_timestamp +interval'330'minute) as Latest_loc_timestamp,
bt.registration_number,
max(di.policy_id) as insurance_activated,
ver.pod_verification_time
from 
(select * from bb.zinka.base_order where start_date >= date ('2020-04-09')) as zbo
-- left join bb.zinka.base_driverprofile dp on dp.user_id=zbo.assigned_driver_id
left join bb.zinka.auth_user as au on au.id=zbo.assigned_driver_id
left join 
( select * from location_service.public.node_group 
where child_type = 'LOCATION' and parent_type = 'UNIT' and deleted=false 
and parent_name!='Pan-India'
) as ngf  on ngf.child_id=zbo.from_city_id 
left join 
(select * from location_service.public.node_group 
where child_type = 'UNIT' and parent_type = 'ZONE' and deleted=false ) as ngf1 on ngf1.child_id=ngf.parent_id
left join 
bb.zinka.base_customeruserprofile as bcup on bcup.user_id=zbo.user_id
left join bb.zinka.base_location as bsf on bsf.id=zbo.from_city_id
-- join
-- (select imei,"user name" from dp.mixpanel.gullak where imei!='0' and imei is not null and from_unixtime(time - 19800)>current_timestamp-interval '10' day )se
-- on se."user name"=replace(au.username,'dr') 
left join 
(select order_id,status,dt_added+interval'330'minute as oa_time  from bb.zinka.base_status where status='Order Accepted'  ) as Oa on oa.order_id=zbo.id
left join supply.driver.base_order sbo on sbo.ms_order_id=zbo.id
left join 
(select (action_time+interval '330' minute) as TAS_timestamp_Driver_App ,order_id from supply.driver.driver_card where type in('PUNCHED','PUNCHEDWITHOUTETA') and action_time is not null) as Tas on tas.order_id=sbo.id
left join           /* advance doc upload status*/
(select order_id,min(last_updated_on+interval '330' minute) as upload_timestamp from supply.driver.driver_documents where image_type in ('INVOICE','LR','WEIGH_SLIP') and is_uploaded=true group by 1)adv_time 
on adv_time.order_id=sbo.id
-- left join 
-- (select order_id,status,dt_added+interval'330'minute as tad_time  from bb.zinka.base_status where status= 'Truck Arrival Destination'  ) as Tad on tad.order_id=zbo.id
left join                                                                               /* Actuao POD data*/
(select order_id,min(last_updated_on+interval '330' minute) as pod_upload from supply.driver.driver_documents_aud where image_type='POD' and is_uploaded=True group by 1)podtime on podtime.order_id=sbo.id
left join tracking_source_prod.public.tracking_entity as te on te.unique_id=replace(au.username,'dr')
left join tracking_source_prod.public.tracking_entity_device_mapping as tedm on tedm.tracking_entity_id=te.id
-- left join (select * from tracking_source_prod.public.device where type='MS_DRIVER_APP')as d on d.id=tedm.device_id 
left join (select * from tracking_data_prod.public.latestlocation where device_type in ('MS_DRIVER_APP') ) Lat on tedm.device_id=Lat.device_id
left join                                                               /* TRUCK_AT_UNLOADING_POINT*/
(select (action_time+interval '330' minute) as truck_at_unloading_time ,order_id from supply.driver.driver_card where type in('TRUCKINTRANSIT','TRUCKINTRANSITWITHOUTETA') and action_time is not null )unld 
on unld.order_id=sbo.id
left join 
bb.zinka.base_truck as bt on zbo.assigned_truck_id=bt.id
left join bb.zinka.base_location as bsf2 on bsf2.id=zbo.to_city_id
left join 
(select order_id,status,dt_added+interval'330'minute as tit_time  from bb.zinka.base_status where status='Truck In-Transit'  ) as tit on tit.order_id=zbo.id
left join 
(select order_id,status,dt_added+interval'330'minute as tdd_time from bb.zinka.base_status where status='Truck Departure Destination'  ) as tdd on tdd.order_id=zbo.id
left join 
(select order_id,status,dt_added+interval'330'minute as tas_time from bb.zinka.base_status where status='Truck Arrival Source'  ) as tass on tas.order_id=zbo.id
left join (select id,ms_order_id,user_id from supply.driver.base_order )b on b.ms_order_id=zbo.id 
left join supply.driver.driver_insurance as di on di.order_id=b.id
left join (select min(last_modified_date+interval'330'minute) as pod_verification_time,order_id from bb.zinka.base_historicalorderdocument where document_type=2 
and verification_status='VERIFIED' group by 2 ) ver on zbo.id=ver.order_id

where lat.last_updated_on is not null
-- and zbo.status = 'Truck Departure Destination'
-- and ngf1.parent_name in ('East') --- Zone
-- and ngf.parent_name in ('Kolkata') --- unit 
-- and bsf.city in ('Haldia') -- From_location
and au.username is not null
-- and tdd.tdd_time is not null
-- and tass.tas_time is not null
and tit.tit_time is not null


group by 
1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,17,19
order by tdd.tdd_time desc
