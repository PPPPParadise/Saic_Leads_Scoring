set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
set hive.mapjoin.smalltable.filesize=55000000;
set hive.auto.convert.join=false;  -- #取消小表加载至内存中
set tez.queue.name=${queuename};

-- 留资数据过滤到临时表
DROP TABLE IF EXISTS marketing_modeling.tmp_dlm_leads_cleansing;
CREATE TABLE marketing_modeling.tmp_dlm_leads_cleansing as
SELECT 
	a.mobile as mobile,
	a.data_source_from_website as channel,
	a.dealer_id as dealer_id,
	b.dealer_level as dealer_level,
	b.brand_id as brand_id,
	a.tel_duration as tel_duration,
	a.allocate_status as allocate_status,
	a.create_time as create_time,
	a.first_resource_name as first_resource_name,
	a.second_resource_name as second_resource_name,
	MAX(a.create_time) OVER(PARTITION BY a.mobile) as last_time,
	ROW_NUMBER() OVER(PARTITION BY a.mobile ORDER BY a.create_time) AS rn,
	ROW_NUMBER() OVER(PARTITION BY a.mobile ORDER BY a.create_time desc) AS rn2
FROM 
    (
		select * from dtwarehouse.ods_dlm_t_potential_customer_leads_m 
		WHERE create_time >= '${beginTime}' 
        -- AND length(replace(mobile,'+86','')) = 11
		AND mobile regexp "^[1][3-9][0-9]{9}$"
		AND pt = '${pt}'
	) a,
	(select * from dtwarehouse.ods_rdp_v_sales_region_dealer where pt = '${pt}') b 
where a.dealer_id = b.dlm_org_id and b.brand_id = 121 -- 经销商主表  
 ;