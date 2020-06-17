set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
set hive.mapjoin.smalltable.filesize=55000000;
set hive.auto.convert.join=false;  -- #取消小表加载至内存中

-- 留资数据过滤到临时表
DROP TABLE IF EXISTS marketing_modeling.dlm_tmp_leads_cleansing;
CREATE TABLE marketing_modeling.dlm_tmp_leads_cleansing as
SELECT 
	a.mobile as mobile,
	a.data_source_from_website as channel,
	a.dealer_id as dealer_id,
	b.dealer_level as dealer_level,
	b.brand_id as brand_id,
	a.tel_duration as tel_duration,
	a.allocate_status as allocate_status,
	a.create_time as create_time,
	MAX(a.create_time) OVER(PARTITION BY a.mobile) as last_time,
	ROW_NUMBER() OVER(PARTITION BY a.mobile ORDER BY a.create_time) AS rn
FROM 
    (
		select * from dtwarehouse.ods_dlm_t_potential_customer_leads_m 
		WHERE create_time >= '2019-08-01 00:00:00' 
        AND length(replace(mobile,'+86','')) = 11
		and pt = '${pt}'
	) a,
	(select * from dtwarehouse.ods_rdp_v_sales_region_dealer where pt = '${pt}') b 
where a.dealer_id = b.dlm_org_id and b.brand_id = 121 -- 经销商主表  
 ;
