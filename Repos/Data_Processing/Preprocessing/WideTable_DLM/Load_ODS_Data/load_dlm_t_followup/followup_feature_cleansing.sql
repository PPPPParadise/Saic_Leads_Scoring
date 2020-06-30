--set mapreduce.map.memory.mb=8192;
--set mapreduce.reduce.memory.mb=8192;
set tez.queue.name=${queuename};

---- 跟进信息清洗
DROP TABLE IF EXISTS marketing_modeling.tmp_dlm_followup_cleansing;
CREATE TABLE marketing_modeling.tmp_dlm_followup_cleansing as
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
		WHERE create_time >= '${beginTime}' 
		  AND status = 2001002 
		  AND cust_id is not null 
		  and pt = '${pt}'
	) a
	,marketing_modeling.tmp_dlm_cust_cleansing b
--	,marketing_modeling.tmp_dlm_deal_flag_feature c
where a.cust_id = b.cust_id 
  and a.dealer_id = b.dealer_id 
 -- and c.mobile = b.mobile
 -- and c.deal_flag < 1
;

-- 按mobile再分组计算
DROP TABLE IF EXISTS marketing_modeling.tmp_dlm_followup_cleansing2;
CREATE TABLE marketing_modeling.tmp_dlm_followup_cleansing2 as
select 
	a.*,
	ROW_NUMBER() OVER(PARTITION BY a.mobile ORDER BY a.follow_time) AS rn2	
from 
	marketing_modeling.tmp_dlm_followup_cleansing a
where a.mobile is not null
;
  
