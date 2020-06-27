set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
set mapreduce.job.queuename=${queuename};

---- 战败信息清洗

DROP TABLE IF EXISTS marketing_modeling.tmp_dlm_fail_cleansing;
CREATE TABLE marketing_modeling.tmp_dlm_fail_cleansing as
select 
	a.cust_id,
	b.mobile,
	a.dealer_id,
	a.oppor_id,
	a.followup_id,
	a.create_time,
	ROW_NUMBER() OVER(PARTITION BY a.cust_id ORDER BY a.create_time desc) AS rn 
from 
	(
		select * from dtwarehouse.ods_dlm_t_oppor_fail 
		where pt = '${pt}'
		and create_time >= '${beginTime}' 
		and cust_id is not null 
	) a ,
	marketing_modeling.tmp_dlm_cust_cleansing b 
where a.cust_id = b.cust_id and a.dealer_id = b.dealer_id 
;
-- 按mobile再分组计算出首次战败时间 
DROP TABLE IF EXISTS marketing_modeling.tmp_dlm_fail_cleansing2;
CREATE TABLE marketing_modeling.tmp_dlm_fail_cleansing2 as
select 
	a.*,
	MIN(a.create_time) OVER(PARTITION BY a.mobile) as first_time,
	ROW_NUMBER() OVER(PARTITION BY a.mobile ORDER BY a.create_time desc) AS rn2	
from 
	marketing_modeling.tmp_dlm_fail_cleansing a
;