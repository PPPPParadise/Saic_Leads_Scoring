set mapreduce.map.memory.mb=8192;
set mapreduce.reduce.memory.mb=8192;

---- 跟进信息清洗
DROP TABLE IF EXISTS marketing_modeling.dlm_tmp_followup_cleansing;
CREATE TABLE marketing_modeling.dlm_tmp_followup_cleansing as
SELECT 
	a.cust_id as cust_id,
	b.mobile,
	a.dealer_id,
	if(a.actual_follow_time is null,a.create_time,a.actual_follow_time) as follow_time -- 跟进时间
FROM 
	(
		select 
			cust_id,dealer_id,actual_follow_time,create_time
		from
			dtwarehouse.ods_dlm_t_followup          
		WHERE create_time >= '2019-08-01 00:00:00' 
		  AND status = 2001002 
		  AND cust_id is not null 
		  and pt = '${pt}'
	) a ,
	marketing_modeling.dlm_tmp_cust_cleansing b 
where a.cust_id = b.cust_id and a.dealer_id = b.dealer_id 
;

-- 按mobile再分组计算
DROP TABLE IF EXISTS marketing_modeling.dlm_tmp_followup_cleansing2;
CREATE TABLE marketing_modeling.dlm_tmp_followup_cleansing2 as
select 
	a.*,
	ROW_NUMBER() OVER(PARTITION BY a.mobile ORDER BY a.follow_time) AS rn2	
from 
	marketing_modeling.dlm_tmp_followup_cleansing a
where a.mobile is not null
;
  
