---- 建卡信息清洗
set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
set hive.mapjoin.smalltable.filesize=55000000;
set hive.auto.convert.join=false;  -- #取消小表加载至内存中
set tez.queue.name=${queuename};

DROP TABLE IF EXISTS marketing_modeling.tmp_dlm_cust_cleansing;
CREATE TABLE marketing_modeling.tmp_dlm_cust_cleansing as
SELECT 
	a.id as cust_id,
	a.mobile as mobile,
	a.dealer_id,
	a.cust_type,
	a.cust_level,
	a.status,
	a.age,
	a.deal_flag,
	a.create_time,
	a.followup_amount,
	a.last_followup_time,
	MAX(a.create_time) OVER(PARTITION BY a.mobile) as last_time,
	ROW_NUMBER() OVER(PARTITION BY a.mobile ORDER BY a.create_time) AS rn
FROM 
	(
		SELECT * FROM dtwarehouse.ods_dlm_t_cust_base           -- 建卡表
		WHERE create_time >= '${beginTime}' 
		  -- AND length(replace(mobile,'+86','')) = 11
		  AND mobile regexp "^[1][35678][0-9]{9}$"		  
		  AND pt = '${pt}'
	) a,
	(SELECT * FROM dtwarehouse.ods_rdp_v_sales_region_dealer WHERE pt = '${pt}') b 
WHERE a.dealer_id = b.dlm_org_id 
  AND b.brand_id = 121
 ;

