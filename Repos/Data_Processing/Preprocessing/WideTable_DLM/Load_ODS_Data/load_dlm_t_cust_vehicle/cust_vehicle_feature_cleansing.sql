set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
set mapreduce.job.queuename=${queuename};

---- 取两年前客户车辆信息表
DROP TABLE IF EXISTS marketing_modeling.tmp_dlm_vehicle_cleansing;
CREATE TABLE marketing_modeling.tmp_dlm_vehicle_cleansing as
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
		  and create_time < '${beginTime}'
	) a,
	(
		select
			mobile,
			id as cust_id 
		from 
			dtwarehouse.ods_dlm_t_cust_base 
		where pt = '${pt}'
		  and create_time < '${beginTime}'
		  and length(replace(mobile,'+86','')) = 11
		group by mobile,id 
	) b
where 
	a.cust_id = b.cust_id 
;
  
