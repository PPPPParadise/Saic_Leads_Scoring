set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
---- 取两年前客户车辆信息表
DROP TABLE IF EXISTS marketing_modeling.dlm_tmp_vehicle_cleansing;
CREATE TABLE marketing_modeling.dlm_tmp_vehicle_cleansing as
SELECT 
	a.cust_id,
	b.mobile,
	a.bought_time,
	a.deleted_flag,
	a.create_time
FROM 
	(
		select 
			cust_id,
			bought_time,
			deleted_flag,
			create_time
		from dtwarehouse.ods_dlm_t_cust_vehicle 
		where pt = '${pt}'
		  and create_time < '2019-08-01 00:00:00'
	) a,
	(
		select
			mobile,
			id as cust_id 
		from 
			dtwarehouse.ods_dlm_t_cust_base 
		where pt = '${pt}'
		  and create_time < '2019-08-01 00:00:00'
		  and length(replace(mobile,'+86','')) = 11
		group by mobile,id 
	) b
where 
	a.cust_id = b.cust_id 
;
  
